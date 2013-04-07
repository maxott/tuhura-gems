
require 'tuhura/omf_rc'
require 'tempfile'
require 'securerandom'

module Tuhura::OmfRc
  module Task
    include ::OmfRc::ResourceProxyDSL
    require 'omf_common/exec_app'
  
    register_proxy :tuhura_task
    
    property :description, :default => ''
    property :package, :default => nil # package containing all the files necessary to execute task
    property :state, :default => :initialised
    property :target_state, :default => :undefined   # state to aspire to 
    property :script_path, :default => nil
    property :script_args, :default => nil
    property :oml_url, :default => 'tcp:oml.incmg.net:3004'

    VALID_TARGET_STATES = [:running, :stopped]
    
    hook :after_initial_configured do |res|
      #puts ">>>> STATE: #{res.property.target_state}::#{res.property.state}"
      if res.property.target_state == :undefined
        res.configure_target_state(:running)
      end
      unless res.property.script_path
        res.inform_error "Missing 'script_path'"
      end
    end
    
    # configure :package do |res, value|
      # puts "PACKAGE: #{value}"
      # res.property.package = value
    # end
# 
    # configure :script_path do |res, value|
      # res.property.script_path= value
    # end
    
    configure :target_state do |res, value|
            
      value = value.to_s.downcase.to_sym
      return value if res.property.target_state == value

      unless VALID_TARGET_STATES.include? value
        res.inform :warn, reason: "Illegal target_state '#{value}'. Only supporting '#{VALID_TARGET_STATES.join(', ')}'"
        return
      end
      res.property.target_state = value
      
      OmfCommon.eventloop.after(0) do
        res.aim
      end
      res.property.state
    end
    
    # Move state towards target_state
    work('aim') do |res|
      case res.property.target_state
      when :running then res.aim_for_running
      when :stopped then res.aim_for_stopped
      end
    end
      
    
    work('aim_for_running') do |res|
      case res.property.state
      when :running
        # yahoo
      when :initialised
        if res.property.package
          res.install_package
        else
          res.inform :error, reason: "Missing 'package' property. Can't proceed."
        end
      when :installed
        res.run
      else
        res.inform :error, reason: "Don't know how to proceed from '#{res.property.state}' to 'running'."
      end
    end
    
    work('install_package') do |res|
      pkg = res.property.package
      res.change_state :downloading
      if m = pkg.match(/([a-z]*):(.*)/)
        type = m[1]
        path = m[2]
        case type
        when 'bdcst'
          OmfCommon.comm.receive_file(path) do |state|
            if state[:action] == :done
              case state[:mime_type]
              when 'application/x-gzip'
                res.change_state :installing
                @tmpdir = File.join(Dir.tmpdir, SecureRandom.uuid)
                Dir.mkdir(@tmpdir)
                cmd = "cd #{@tmpdir}; tar zxf #{state[:path]}; /usr/local/rvm/bin/rvm jruby exec bundle package --all 2>&1"
                debug "Executing '#{cmd}'"
                res = `#{cmd}`
                unless $?.success?
                  res.inform_error "Can't unpack and install package (#{res})"
                else
                  res.change_state :installed
                  res.aim
                end
              else
                res.inform_error "Do not support package mime-type '#{state[:mime_type]}'"
              end
            end
          end
        else  
          res.inform_error "Do not support download mechanism '#{type}'"
        end
      else
        res.inform_error "Can't parse package URL '#{pkg}'. Expected 'type:id'."
      end
    end
    
    work 'change_state' do |res, value|
      #puts ">>>> Changing state to #{value}"
      debug "Changing state to #{value}"
      res.property.state = value
      res.inform :status, {state: value, target_state: res.property.target_state}, :ALL
    end
    
    # Swich this Application RP into the 'stopped' state
    # (see the description of configure :state)
    #
    work('switch_to_stopped') do |res|
      if res.property.state == :running || res.property.state == :paused
        id = res.property.app_id
        unless ExecApp[id].nil?
          # stop this app
          begin
            # first, try sending 'exit' on the stdin of the app, and wait
            # for 4s to see if the app acted on it...
            ExecApp[id].stdin('exit')
            sleep 4
            unless ExecApp[id].nil?
              # second, try sending TERM signal, wait another 4s to see
              # if the app acted on it...
              ExecApp[id].signal('TERM')
              sleep 4
              # finally, try sending KILL signal
              ExecApp[id].kill('KILL') unless ExecApp[id].nil?
            end
            res.property.state = :completed
          rescue => err
          end
        end
      end
    end
  
    # Swich this Application RP into the 'running' state
    # (see the description of configure :state)
    #
    work('run') do |res|
      script = res.property.script_path
      if script.nil?
        res.inform_warn "Property 'script_path' not set! No idea what to run!"
      else
        #puts "#{script} #{res.property.script_args} "
        #cmd = "cd #{@tmpdir}; rvm jruby exec bundle exec ruby #{script}"
        args = res.property.script_args
        if oml_url = res.property.oml_url
          args += " --oml-collect #{oml_url}"
        end
        cmd = "/usr/local/rvm/bin/rvm jruby exec bundle exec ruby #{script} #{args}"
        info "Executing '#{cmd}' in #{@tmpdir}"
        ExecApp.new('task', cmd, true, @tmpdir) do |event_type, app_id, msg|
          res.process_event(event_type, msg)
        end
        res.change_state :running
      end
    end
    
    work 'process_event' do |res, event_type, msg|
      puts "#{event_type}>>>> #{msg}"
    end
  end
end
