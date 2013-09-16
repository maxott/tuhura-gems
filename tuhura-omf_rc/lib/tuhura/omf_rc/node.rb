require 'tuhura/omf_rc'

module Tuhura::OmfRc
  module Node
    include OmfRc::ResourceProxyDSL

    register_proxy :tuhura_node

    hook :before_create do |node, type, opts|
      case type.to_sym
      when :net
        net_dev = node.request_devices.find do |v|
          v[:name] == opts[:if_name]
        end
        raise "Device '#{opts[:if_name]}' not found" if net_dev.nil?
      when :tuhura_task
        unless opts[:uid]
          opts[:uid] = "#{opts[:parent] ? opts[:parent].uid : 'xxx'}-task-#{SecureRandom.uuid}"
        end
      end
    end

    # Set up a timer to check for reclaimable tasks
    #
    hook :before_ready do |res|
      OmfCommon.eventloop.every(5) do
        res.reclaim_tasks
      end

    end


    request :tasks do |node|
      node.children.find_all { |v| v.property.type =~ /tuhura_task/ }.map do |v|
        t = { uid: v.uid, address: v.resource_address, state: v.property.state }
        if name = v.hrn
          t[:name] = name
        end
        t
      end.sort { |x, y| (x[:name] || x[:uid]) <=> (y[:name] || y[:uid]) }
    end

    request :stats do |node|
      stats = {}
      unless @num_processors
        @num_processors = 1
        if File.exist? '/proc/cpuinfo'
          if `cat /proc/cpuinfo | grep 'model name' | wc -l` =~ /(\d+)/
            @num_processors = $1.to_i
          else
            warn "Couldn't use /proc/cpuinfo to determine number of processors."
          end
        end
      end
      if `uptime` =~ /load average(s*): ([\d.]+)(,*) ([\d.]+)(,*) ([\d.]+)\Z/
        stats[:load_avg] = {
          one_min: $2.to_f / @num_processors,
          five_min: $4.to_f/ @num_processors,
          fifteen_min: $6.to_f/ @num_processors,
          cores: @num_processors
        }
      else
        warn "Couldn't use `uptime` as expected."
      end

      if File.exist? '/proc/meminfo'
        mi = {}
        `cat /proc/meminfo`.each_line do |line|
          _, key, value = *line.match(/^(\w+):\s+(\d+)\s/)
          mi[key] = value.to_i
        end
        m_stats = stats[:memory] = {}
        m_stats[:unit] = 'MB'
        m_stats[:total] = mi['MemTotal'] / 1024
        #m_stats[:free] = (mi['MemFree'] + mi['Buffers'] + mi['Cached']) / 1024
        m_stats[:free] = mi['MemFree'] / 1024
        m_stats[:cached] = mi['Cached'] / 1024
        m_stats[:used] = m_stats[:total] - m_stats[:free] - m_stats[:cached]
        m_stats[:percent_used] = (m_stats[:used] / m_stats[:total].to_f * 100).to_i
        s_stats = stats[:swap] = {}
        s_stats[:unit] = 'MB'
        s_stats[:total] = mi['SwapTotal'] / 1024
        unless s_stats[:total] == 0
          s_stats[:free] = mi['SwapFree'] / 1024
          s_stats[:used] = s_stats[:total] - s_stats[:free]
          s_stats[:percent_used] = (s_stats[:used] / s_stats[:total].to_f * 100).to_i
        end
      end

      di = stats[:disk] = []
      `df -l -T`.split("\n").each_with_index { |l, i|
        next if i < 1 # skip headers
        m = l.match('([^ ]*)[ ]*' * 7)
        dummy, fs, type, blocks, avail, used, used_pct, mount = m.to_a
        di << {
          mount: mount, disk: fs, ftype: type,
          available: avail.to_i, used: used.to_i, used_pct: used_pct[0 ... -1].to_i
        }
      }

      stats
    end


    request :interfaces do |resource|
      devices = []
      # Support net devices for now
      category = "net"
      Dir.glob("/sys/class/net/eth*").each do |v|
        File.exist?("#{v}/uevent") && File.open("#{v}/uevent") do |f|
          subcategory = f.read.match(/DEVTYPE=(.+)/) && $1
          proxy = "net"
          File.exist?("#{v}/device/uevent") && File.open("#{v}/device/uevent") do |f|
            driver = f.read.match(/DRIVER=(.+)/) && $1
            device = { name: File.basename(v), driver: driver, category: category }
            device[:subcategory] = subcategory if subcategory
            File.exist?("#{v}/operstate") && File.open("#{v}/operstate") do |fo|
              device[:op_state] = (fo.read || '').chomp
            end
            # Let's see if the interface is already up
            # NOTE: THIS MAY NOT BE ROBUST
            s = `ifconfig #{File.basename(v)}`
            unless s.empty?
              if m = s.match(/inet addr:\s*([0-9.]+)/)
                device[:ip4] = m[1]
              end
              if m = s.match(/inet6 addr:\s*([0-9a-f.:\/]+)/)
                device[:ip6] = m[1]
              end
            end
            devices << device
          end
        end
      end
      devices
    end

    # Check if any of the created tasks can be deleted
    #
    work 'reclaim_tasks' do |res|
      res.children.find_all { |v| v.property.type =~ /tuhura_task/ }.each do |v|
        debug "Check child #{v}: reclaimable? #{v.reclaimable?}"
        if v.reclaimable?
          info "Release #{v}"
          res.release(v.uid)
        end
      end
    end

  end
end
