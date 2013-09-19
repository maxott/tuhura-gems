#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
module Tuhura
  module Tools; end
end

require 'optparse'

module Tuhura::Tools
  module StartupHelper

    @@tasks = {}
    @@verbose = false
    @@install_gems = false
    @@update_gems = false

    def self.def_task(name, opts = {}, &block)
      unless opts[:descr] && opts[:top_dir] && opts[:ruby] && opts[:path]
        raise "Missing any of :desc, :top_dir, or :path"
      end
      opts[:_block_] = block
      @@tasks[name] = opts
    end

    def self.run()
      op = OptionParser.new
      descr = "\n  Execute various Tuhura tasks, such as:\n\n"
      @@tasks.each do |k, v|
        descr += "    #{k}\t... #{v[:descr]}\n"
      end
      op.banner = "Usage: #{op.program_name} task_name [options]\n#{descr}\n"
      op.on('-i', "--install", "Install GEMS before running task") { @@install_gems = true }
      op.on('-u', "--update", "Update GEMS before running task") { @@update_gems = true }
      op.on('-v', "--verbose", "Update GEMS before running task") { @@verbose = true }
      op.on_tail('-h', "--help", "Show this message") { $stderr.puts op; exit }

      # we are splitting the command line into flags before a command and the rest
      flags = []
      ARGV.each {|e| break unless e.start_with? '-'; flags << e}
      rest = ARGV[flags.size .. -1]
      cmd = rest.shift || ''
      begin
        op.parse(flags)
      rescue OptionParser::InvalidOption => ex
        $stderr.puts "\nERROR: #{ex}\n\n"
        $stderr.puts op; exit
      end

      if cmd.empty? || cmd.start_with?('-')
        $stderr.puts "\nERROR: Missing command\n\n"
        $stderr.puts op; exit
      end
      unless task = @@tasks[cmd.to_sym]
        $stderr.puts "\nERROR: Unknown command '#{cmd}'\n\n"
        $stderr.puts op; exit
      end
      _execute_task(task, rest)
    end

    def self._execute_task(task, args)
      # opts[:descr] && opts[:top_dir] && opts[:ruby] &&opts[:path]
      gemfile = task[:gemfile] || 'Gemfile'
      if @@install_gems
        cmd = "cd #{task[:top_dir]}; env BUNDLE_GEMFILE=#{gemfile} bundle install"
        puts ".. executing #{cmd}" if @@verbose
        @@verbose ? puts(`#{cmd}`) : `#{cmd}`
      end
      if @@update_gems
        cmd = "cd #{task[:top_dir]}; env BUNDLE_GEMFILE=#{gemfile} bundle update"
        puts ".. executing #{cmd}" if @@verbose
        @@verbose ? puts(`#{cmd}`) : `#{cmd}`
      end
      if task[:use_bundler] == false
        cmd = "cd #{task[:top_dir]}; env BUNDLE_GEMFILE=#{gemfile} #{task[:ruby]} -I lib #{task[:path]} #{args.join(' ')}"
      else
        cmd = "cd #{task[:top_dir]}; env BUNDLE_GEMFILE=#{gemfile} bundle exec #{task[:ruby]} -I lib #{task[:path]} #{args.join(' ')}"
      end
      puts ".. executing #{cmd}" if @@verbose
      exec cmd
    end

  end


end

#######

