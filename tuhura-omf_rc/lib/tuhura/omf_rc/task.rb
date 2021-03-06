#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/omf_rc'
require 'tempfile'
require 'securerandom'
require 'time'
require 'omf_common/exec_app'

module Tuhura::OmfRc
  module Task
    include ::OmfRc::ResourceProxyDSL

    register_proxy :tuhura_task

    property :description, :default => ''
    property :package, :default => nil # package containing all the files necessary to execute task
    property :state, :default => {current: :initialised}
    property :automatic_restart, :default => {
      active: false, # set to true if task should be restarted on 'done.error'
      retries: 3, # Number of retry events before giving up (< 0 ...  try forever)
      retry_threshhold: 120,  # Seconds after which to reset the retry counter
      retry_delay: 30 # Seconds to wait before restarting
    }
    #property :target_state, :default => :undefined   # state to aspire to
    property :script_path, :default => nil
    property :script_args, :default => nil
    property :ruby_version, :default => 'jruby-1.7.3'
    property :oml_url, :default => 'tcp:oml.incmg.net:3004'
    property :pid, :readonly => true
    property :node, :read_only => true

    VALID_TARGET_STATES = [:running, :stopped]


    hook :after_initial_configured do |res|
      #puts ">>>> STATE: #{res.property.target_state}::#{res.property.state}"
      unless res.property.state.target
        res.configure_state :running
      end
      unless res.property.script_path
        res.inform_error "Missing 'script_path'"
      end
      if parent = res.opts.parent
        res.property.node = parent.resource_address
      end
      res.property.state.since = Time.now.iso8601
    end

    hook :before_release do |res|
      puts "RELEASING #{res}"
    end

    configure :state do |res, value|

      value = value.to_s.downcase.to_sym
      return value if res.property.state[:target] == value
      return value if res.property.state[:current] == value

      unless VALID_TARGET_STATES.include? value
        res.inform :warn, reason: "Illegal target state '#{value}'. Only supporting '#{VALID_TARGET_STATES.join(', ')}'"
        return
      end
      res.property.state[:target] = value

      OmfCommon.eventloop.after(0) do
        res.aim
      end
      res.property.state
    end

    configure :automatic_restart do |res, value|
      p = res.property.automatic_restart
      [:active, :retries, :retry_threshhold, :retry_delay].each do |k|
        p[k] = value[k] if value[k]
      end
      p
    end

    # Move state towards target_state
    work('aim') do |res|
      next unless target_state = res.property.state.target
      next if target_state == res.property.state.current

      case target_state
      when :running then res.aim_for_running
      when :stopped then res.aim_for_stopped
      end
    end

    work('aim_for_running') do |res|
      case current_state = res.property.state[:current]
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
      when /done\.error/
        if res.restartable?
          delay = (res.property.automatic_restart.retry_delay || 0)
          res.property.state.target = :running
          res.property.state.restarting_at = (Time.now + delay).iso8601
          res.change_state :restarting
          debug "Attempt to restart task after #{delay} seconds (#{res.property.state})"
          OmfCommon.eventloop.after(delay) do
            debug "Attempt to restart task now (#{res.property.state})"
            if res.property.state.current == :restarting && res.property.state.target == :running
              res.property.state.restarted ||= []
              res.property.state.restarted << Time.now.iso8601 # can't do this in one shot. No idea why
              res.property.state.delete(:restarting_at)
              res.run
            end
          end
        end
      when :done
        res.inform :done
      else
        res.inform :error, reason: "Don't know how to proceed from '#{current_state}' to 'running'."
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
                prepare_cmd = File.join(File.dirname(__FILE__), '../../../sbin/prepare_task.sh')
                cmd = "env -i bash -x #{prepare_cmd} #{state[:path]} #{@tmpdir} #{res.property.ruby_version}"
                debug "Executing '#{cmd}'"
                ExecApp.new('preparing', cmd, true) do |event_type, app_id, msg|
                  debug "<#{event_type}>:: #{msg}"
                  case event_type.to_s
                  when 'DONE.OK'
                    res.change_state :installed
                    res.aim
                  when 'DONE.ERROR'
                    res.change_state 'install.failed'
                    res.aim
                  when 'STDOUT'
                    if m = msg.match(/^STATUS: (.*)/)
                      res.change_state m[1]
                    end
                  when 'STARTED'
                    # ignore
                  else
                    res.inform_warn "Unknown event '#{event_type}' while installing task package"
                  end
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

    work('aim_for_stopped') do |res|
      case current_state = res.property.state.current#.to_s
      when :stopped
        # yahoo
      when :initialised, :installed
        res.change_state 'stopped'
      when :running
        res.switch_to_stopped
      when /^installing/
        # wait until done
      when :installed
        res.change_state 'stopped'
      else
        res.inform :error, reason: "Don't know how to proceed from '#{current_state}' to 'stooped'."
      end
    end

    work 'change_state' do |res, value|
      debug "Changing state to #{value}"
      #res.property.state[:current] = value
      res.property.state.current = (value = value.to_sym)
      res.property.state.since = Time.now.iso8601
      res.property.state.delete(:target) if res.property.state[:target] == value

      res.inform :status, {state: res.property.state.to_hash}, :ALL
    end

    # Swich this Application RP into the 'stopped' state
    # (see the description of configure :state)
    #
    work('switch_to_stopped') do |res|
      current_state = res.property.state.current.to_sym
      debug "Stopping app - #{@app}"
      if current_state == :running || current_state == :paused
        if @app
          # stop this app
          begin
            # second, try sending TERM signal, wait another 4s to see
            # if the app acted on it...
            debug "Signal 'TERM'"
            @app.signal('TERM')
            sleep 4
            # finally, try sending KILL signal
            if @app
              debug "Signal 'KILL'"
              @app.signal('KILL')
            end
            res.change_state 'stopped'
          rescue => err
            warn "While stopping app - #{err}"
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
          args += " --oml-collect #{oml_url} --oml-id #{res.uid}"
        end
        ruby_opts = []
        if Dir.exist?(File.join(@tmpdir, 'lib'))
          ruby_opts << '-I lib'
        end
        cmd = "env -i /usr/local/rvm/bin/rvm #{res.property.ruby_version} exec bundle exec ruby #{ruby_opts.join(' ')} #{script} #{args}"
        info "Executing '#{cmd}' in #{@tmpdir}"
        res.change_state :running
        @app = ExecApp.new(nil, cmd, true, @tmpdir) do |event_type, app_id, msg|
          debug "<#{event_type}> #{msg}", res.uid
          case event_type.to_s
          when 'DONE.OK'
            @app = nil
            res.change_state :done
            res.aim
          when 'DONE.ERROR'
            @app = nil
            res.property.state.target = :running
            res.change_state 'done.error'
            res.aim
          when 'STDOUT'
            if m = msg.match(/STATUS: (.*)/)
              res.change_state m[1]
            end
          when 'STARTED'
            # nothing
          else
            res.inform_warn "Unknown event '#{event_type}' while running task"
            # res.change_state 'running.warn'
            # res.aim
          end
        end
        res.property.pid = @app.pid
      end
    end

    work 'process_event' do |res, event_type, msg|
      debug "#{event_type}:: #{msg}"
    end

    # Return true if the parent resource can delete this resource
    #
    work 'reclaimable?' do |res|
      case res.property.state.current
      when 'done.error'
        !res.restartable?
      when /done/
        true
      when :stopped
        true
      when /failed$/
        true
      else
        false
      end
    end

    work 'restartable?' do |res|
      debug "Restartable?: #{res.property.automatic_restart.active}"
      res.property.automatic_restart.active
    end

  end
end
