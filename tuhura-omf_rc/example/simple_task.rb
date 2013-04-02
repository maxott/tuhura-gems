# OMF_VERSIONS = 6.0
require 'omf_common'
require 'omf_rc'
require 'omf_rc/resource_proxy/node'

DESCR = %{
Execute a remote task
}

OP_MODE = :development


opts = {
  communication: {
    url: 'amqp://0.0.0.0',
  },
  eventloop: { type: :em},
  logging: {
#    level: 'info'
  },
}

aopts = {
  package_dir: nil,
  resource_url: 'node'
}


op = $op = OptionParser.new
op.banner = "Usage: #{op.program_name} [options] script_name [script_args]\n#{DESCR}\n"
op.on '-r', '--resource-url URL', "URL of resource where task should run" do |url|
  aopts[:resource_url] = url
end
op.on '-D', '--directory DIR', "Directory to package up and send to resource" do |path|
  aopts[:package_dir] = path
end
op.on '-d', '--debug', "Set logging to DEBUG level" do
  opts[:logging][:level] = 'debug'
  $debug = true
end
op.on_tail('-h', "--help", "Show this message") { $stderr.puts op; exit }
rest = op.parse(ARGV) || []

aopts[:script_path] = rest.shift
aopts[:args] = rest.join(' ')

def error_msg(msg)
  $stderr.puts "\nERROR: #{msg}\n\n"
  $stderr.puts $op
  exit(-1)
end

unless aopts[:resource_url] && aopts[:script_path]
  error_msg 'Missing --resource-url or script_name'
end

# Environment setup
#OmfCommon.init(:developement, opts) 

def start_task(group_t, node_t, opts)
  script_path = opts[:script_path] 
  unless package_dir = opts[:package_dir]
    package_dir = File.dirname(script_path)
    script_path = File.basename(script_path)
  end
  unless File.exist?(rake_file = File.join(package_dir, 'Rakefile'))
    error_msg "Can't find 'Rakefile' in '#{package_dir}'"
  end
  output = `cd #{package_dir}; rake package 2>&1`
  unless $?.success?
    o = output.split("\n").join("\n==>\t")
    error_msg "While executing 'rake' \n==>\t#{o}"
  end
  unless File.exist?(tar_file = File.join(package_dir, 'build/package.tgz'))
    error_msg "Can't find '#{tar_file}'"
  end
  pkg_id = "#{node_t.id}/package"
  pkg_addr = OmfCommon.comm.broadcast_file(tar_file)
  
  node_t.create(:tuhura_task, {
    package: pkg_addr,
    script_path: script_path,
    script_args: opts[:args],
    membership: group_t.address
  })
end


#OmfCommon.init(:local, {}) do |el|
OmfCommon.init(OP_MODE, opts) do |el|
  OmfCommon.comm.on_connected do |comm|
    
    if OP_MODE == :local
      load File.join(File.dirname(__FILE__), '..', 'lib', 'tuhura', 'omf_rc', 'task.rb')
      node_rc = OmfRc::ResourceFactory.create(:node, uid: aopts[:resource_url])
    end
    
    # Get handle on existing entity
    comm.subscribe(aopts[:resource_url]) do |node_t|
      node_t.on_subscribed do
        comm.subscribe('task') do |group_t|
          group_t.on_inform_status do |msg|
            puts "INFORM: #{msg}"
          end
          start_task(group_t, node_t, aopts)
        end
      end
    end
    
    el.after(600) { el.stop }
  end
end

puts "DONE"

