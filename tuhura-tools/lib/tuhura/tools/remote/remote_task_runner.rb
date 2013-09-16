#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
# OMF_VERSIONS = 6.0
require 'omf_common'

DESCR = %{
Execute a remote task
}

$op_mode = :custom


opts = {
  communication: {
    url: 'amqp://0.0.0.0',
  },
  eventloop: { type: :em},
  logging: {
    level: {
      'OmfCommon::Comm::AMQP' => 'info',
      #default: 'debug'
    },
#    level: 'info'
    appenders: {
      stdout: {
        date_pattern: '%H:%M:%S',
        pattern: '%d %5l %c{2}: %m\n',
        color_scheme: 'default'
      }
    }
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
op.on '-R', '--ruby-version VERSION', "Ruby version to use" do |ver|
  aopts[:ruby_version] = ver
end
op.on_tail '-m', '--mode MODE', "Set operation mode [#{$op_mode}]" do | mode |
  $op_mode = mode
end
op.on '-a', '--automatic_restart', "Restart task when exiting with done.error" do
  aopts[:automatic_restart] = true
end
op.on_tail '-d', '--debug', "Set logging to DEBUG level" do
  opts[:logging][:level][:default] = 'debug'
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
r = aopts[:resource_url].split('/')
resource_name = r.pop
if r.length > 1
  opts[:communication][:url] = r.join('/')
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
  output = `cd #{package_dir}; env -i rake package 2>&1`
  unless $?.success?
    o = output.split("\n").join("\n==>\t")
    error_msg "While executing 'rake' \n==>\t#{o}"
    exit(-1)
  end
  unless File.exist?(tar_file = File.join(package_dir, 'build/package.tgz'))
    error_msg "Can't find '#{tar_file}'"
    exit(-1)
  end
  pkg_id = "#{node_t.id}/package"
  pkg_addr = OmfCommon.comm.broadcast_file(tar_file)

  copts = {
    package: pkg_addr,
    script_path: script_path,
    script_args: opts[:args],
    #membership: group_t.address
    membership: group_t.id
  }
  copts[:ruby_version] = opts[:ruby_version] if opts[:ruby_version]
  copts[:automatic_restart] = {active: true} if opts[:automatic_restart]
  node_t.create(:tuhura_task, copts)
end

def print_inform(msg, topic)
  if (src_topic = msg.src.id) == topic.id
    puts "  #{topic.id}"
  else
    puts "  #{src_topic} via #{topic.id}"
  end
  msg.each_property do |name, value|
    puts "    #{name}: #{value}"
  end
  puts "------"
end

#OmfCommon.init(:local, {}) do |el|
OmfCommon.init($op_mode, opts) do |el|
  OmfCommon.comm.on_connected do |comm|

    if $op_mode == :local
      require 'omf_rc'
      require 'omf_rc/resource_proxy/node'
      load File.join(File.dirname(__FILE__), '..', 'lib', 'tuhura', 'omf_rc', 'task.rb')
      node_rc = OmfRc::ResourceFactory.create(:node, uid: aopts[:resource_url])
    end

    # Get handle on existing entity
    comm.subscribe(resource_name) do |node_t|
      node_t.on_subscribed do
        comm.subscribe('tasks') do |group_t|
          group_t.on_inform_status do |msg|
            print_inform(msg, group_t)
          end
          start_task(group_t, node_t, aopts)
        end
      end
    end

    el.after(600) { el.stop }
  end
end

puts "DONE"

