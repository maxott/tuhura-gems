#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'optparse'
require 'oml4r/benchmark'
require 'tuhura/common'

module Tuhura::Common
  module OML

    OML_OPTS = {
      appName: File.basename($0, ".*"),
      domain: 'benchmark',
      nodeID: "#{Socket.gethostname}-#{Process.pid}",
      collect: 'file:-'
    }

    def oml_bm(name = nil, opts = {periodic: 1}, &block)
      name ||= @oml_options[:appName]
      OML4R::Benchmark.bm(name, opts, &block)
    end

    def oml_parse(options = {}, &block)
      begin
        optparse = nil
        OML4R::init(ARGV, @oml_options) do |op|
          optparse = op
          op.on( '-h', '--help', 'Display this message') do
            puts op
            exit
          end

          block.call(op) if block
        end
      rescue OptionParser::InvalidOption, OptionParser::MissingArgument
        puts $!.to_s
        puts optparse
        abort
      end

      missing = options.select{ |key, value| value == :MANDATORY }
      unless missing.empty?
        puts "Missing options: #{missing.keys.join(', ')}"
        puts optparse
        abort
      end

    end

    def oml_close
      OML4R::close
    end

    def oml_init(opts = OML_OPTS)
      @oml_options = opts
    end
  end
end
