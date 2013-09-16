#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
# OMF_VERSIONS = 6.0
require 'omf_common'
require 'omf_rc'
require 'omf_rc/resource_proxy/node'

DESCR = %{
Provides a simple node RC which can execute Tuhura tasks
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
op.banner = "Usage: #{op.program_name} [options]\n#{DESCR}\n"
op.on_tail('-h', "--help", "Show this message") { $stderr.puts op; exit }
rest = op.parse(ARGV) || []

def error_msg(msg)
  $stderr.puts "\nERROR: #{msg}\n\n"
  $stderr.puts $op
  exit(-1)
end

#OmfCommon.init(:local, {}) do |el|
OmfCommon.init(OP_MODE, opts) do |el|
  OmfCommon.comm.on_connected do |comm|
    
    load File.join(File.dirname(__FILE__), '..', 'lib', 'tuhura', 'omf_rc', 'task.rb')
    node_rc = OmfRc::ResourceFactory.create(:node, uid: aopts[:resource_url])
  end
end

puts "DONE"

