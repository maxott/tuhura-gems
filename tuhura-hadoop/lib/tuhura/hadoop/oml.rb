
require 'optparse'
require 'oml4r/benchmark'
require 'tuhura/common'

module Tuhura::Common
  module OML
    
    OML_OPTS = {
      appName: File.basename($0, ".*"),
      domain: 'benchmark', 
      nodeID: "#{Socket.gethostbyname(Socket.gethostname)[0]}-#{Process.pid}",
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