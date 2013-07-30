require 'tuhura/common/logger'
require 'csv'

module Tuhura::AWS::S3
  #
  # Class to serialize records into CSV files
  #
  class CSVWriter
    include Tuhura::Common::Logger

    def self.file_ext
      'csv'
    end

    CLASS2TYPE = {
      String => 'string',
      Fixnum => 'long',
      Float => 'double',
      TrueClass => 'boolean',
      FalseClass => 'boolean',
      NilClass => 'null'
    }

    STRING2TYPE = {
      'string' => 'string',
      'int' => 'int',
      'integer' => 'int',
      'long' => 'long',
      'float' => 'float',
      'double' => 'double',
      'bool' => 'boolean',
      'boolean' => 'boolean',
      'nil' => 'null',
      'null' => 'null'
    }

    TYPE2DEFAULT = {
      'string' => '???',
      'int' => 0,
      'long' => 0,
      'float' => 0,
      'double' => 0,
      'boolean' => false,
      'null' => nil
    }

    attr_reader :keys

    # Constructor
    #
    # @param schema [Hash] - Containing [name, cols]
    # @param out_stream [IO] - Where to write the AVRO blocks to
    # @param aliases [Array] - Additional aliases
    #
    def initialize(name, schema, out_stream, aliases = [])
      logger_init(nil, top: false)

      @name = name
      @defaults = {}
      @boolean_fields = []
      @keys = []
      header = []
      schema[:cols].each do |n, t, default|
        #t = t.to_s
        n = n.to_s
        unless type = t.is_a?(Class) ? CLASS2TYPE[t.to_s] : STRING2TYPE[t.to_s]
          if t.to_s.match(/^[A-Z]/)
            # enum
            type = 'int'
          elsif t.is_a? Hash
            # should check if this is really an array type
            type = t
            default ||= []
          else
            raise "Unknown type declaration '#{t}:#{t.class}' for '#{n}' in '#{name}'"
          end
        end
        @boolean_fields << n if type == 'boolean'
        @defaults[n] = default ? default : TYPE2DEFAULT[type]
        keys << n
        header << "#{n}:#{type}"
      end
      #puts "#{header}"
      out_stream << header.to_csv
      @out_stream = out_stream
    end

    def validate_fields(record)
    end

    def <<(record)
      @boolean_fields.each do |n|
        # fix boolean
        v = record[n]
        record[n] = true if v == 1
        record[n] = false if v == 0
      end
      r = @defaults.merge(record)
      @out_stream << @keys.map {|k| r[k] }.to_csv
    end

    def flush
      @out_stream.flush
    end

    def close
      @out_stream.close
    end

  end
end

