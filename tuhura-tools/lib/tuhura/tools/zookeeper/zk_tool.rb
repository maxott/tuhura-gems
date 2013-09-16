#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/common/logger'
require 'tuhura/common/zookeeper'

DESCR = %{
Interact with information in Zookeeper related to Tuhura
}

opts = {
  zk: {}
}

log_opts = Tuhura::Common::Logger::LOGGER_OPTS
log_opts[:log4j_config] = File.join(File.dirname(__FILE__), 'log4j.properties')
log_opts[:level] = :info

op = $op = OptionParser.new
op.banner = "Usage: #{op.program_name} [options] \n#{DESCR}\n"
op.on '-c', '--children PATH', "List the children of PATH" do |path|
  opts[:action] = :children  
  opts[:path] = path
end
op.on '-d', '--delete PATH', "Delete PATH" do |path|
  opts[:action] = :delete  
  opts[:path] = path
end
op.on '-g', '--get PATH', "Return the value of PATH" do |path|
  opts[:action] = :get  
  opts[:path] = path
end
op.on '-p', '--put PATH VALUE', "Set the VALUE of PATH" do |path|
  opts[:action] = :put 
  opts[:path] = path
end
op.on '-w', '--walk PATH', "Walk tree rooted at PATH and print all children and their values" do |path|
  opts[:action] = :walk  
  opts[:path] = path
end
op.on '-z', '--zk-url URL', "URL of zookeeper server [#{Tuhura::Common::Zookeeper::ZOOKEEPER_OPTS[:url]}]" do |url|
  opts[:zk][:url] = url
end
op.on_tail '--debug', "Set logging to 'debug' [#{log_opts[:level]}]" do 
  log_opts[:level] = :debug
end
op.on_tail('-h', "--help", "Show this message") { $stderr.puts op; exit }
rest = op.parse(ARGV) || []
if opts[:action] == :put
  opts[:value] = rest[0]  
end

class ZkTool
  include Tuhura::Common::Logger
  include Tuhura::Common::Zookeeper

  def run(opts)
    path = opts[:path]
    begin
      case opts[:action]
      when :get
        puts "#{path}: #{zk_get(path)}"
      when :put
        if value = opts[:value]
          zk_put(path, value)
          puts "#{path}: #{value}"
        else
          puts "ERROR: Missing value" 
        end

      when :delete
        zk_delete(path)
        puts "Deleted #{path}"

      when :children
        c = zk_children(path)
        puts "Found #{c.length} children:\n\t#{c.join("\n\t")}"
      when :walk
        _walk(path)
      else
        puts "ERROR: Unknown action '#{opts[:action]}'"
      end
    rescue Tuhura::Common::Zookeeper::NonExistingPathException => ex
      puts "WARNING: '#{path}' does NOT exist"
    end
  end
  
  def initialize(opts)
    logger_init()
    zk_init(opts[:zk])
  end
  
  def _walk(path, level = 0)
    value = nil
    c = zk_children(path)
    if c.length == 0
      # leaf
      value = zk_get(path)
    end
    puts "#{'  ' * level}#{level == 0 ? path : path.split('/')[-1]}: #{value}"
    c.sort.each { |p| _walk("#{path == '/' ? '' : path}/#{p}", level + 1) } 
  end
end

ZkTool.new(opts).run(opts)
