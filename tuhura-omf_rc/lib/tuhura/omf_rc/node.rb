require 'tuhura/omf_rc'

module Tuhura::OmfRc
  module Node
    include OmfRc::ResourceProxyDSL
  
    register_proxy :tuhura_node
    
    hook :before_create do |node, type, opts|
      if type.to_sym == :net
        net_dev = node.request_devices.find do |v|
          v[:name] == opts[:if_name]
        end
        raise "Device '#{opts[:if_name]}' not found" if net_dev.nil?
      end
    end

    request :interfaces do |node|
      node.children.find_all { |v| v.type == :net || v.type == :wlan }.map do |v|
        { name: v.property.if_name, type: v.type, uid: v.uid }
      end.sort { |x, y| x[:name] <=> y[:name] }
    end
  
    request :tasks do |node|
      node.children.find_all { |v| v.type =~ /tuhura_task/ }.map do |v|
        { name: v.hrn, type: v.type, uid: v.uid }
      end.sort { |x, y| x[:name] <=> y[:name] }
    end

    request :stats do |node|
      stats = {}
      unless @num_processors
        if `cat /proc/cpuinfo | grep 'model name' | wc -l` =~ /(\d+)/
          @num_processors = $1.to_i
        else
          @num_processors = 1
          warn "Couldn't use /proc/cpuinfo to determine number of processors."
        end
      end
      if `uptime` =~ /load average(s*): ([\d.]+)(,*) ([\d.]+)(,*) ([\d.]+)\Z/
        stats[:load_avg] = {
          one_min: $2.to_f / @num_processors,
          five_min: $4.to_f/ @num_processors,
          fifteen_min: $6.to_f/ @num_processors
        }
      else
        warn "Couldn't use `uptime` as expected."
      end
      
      mi = {}
      `cat /proc/meminfo`.each_line do |line|
        _, key, value = *line.match(/^(\w+):\s+(\d+)\s/)
        mi[key] = value.to_i
      end
      m_stats = stats[:memory]
      m_stats[:total] = mi['MemTotal'] / 1024
      #m_stats[:free] = (mi['MemFree'] + mi['Buffers'] + mi['Cached']) / 1024
      m_stats[:free] = mi['MemFree'] / 1024
      m_stats[:used] = m_stats[:total] - m_stats[:free]
      m_stats[:percent_used] = (m_stats[:used] / m_stats[:total].to_f * 100).to_i
      s_stats = stats[:swap]
      s_stats[:total] = mi['SwapTotal'] / 1024
      s_stats[:free] = mi['SwapFree'] / 1024
      s_stats[:used] = s_stats[:total] - s_stats[:free]
      unless s_stats[:total] == 0    
        s_stats[:percent_used] = (s_stats[:used] / s_stats[:total].to_f * 100).to_i
      end
      
      stats
    end
    
    
    request :devices do |resource|
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
    
  end
end
