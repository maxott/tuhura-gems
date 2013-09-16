#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/common/logger'
require 'avro'

module Tuhura::AWS::S3
  #
  # Class to serialise records into an Avro chunk
  #
  class AvroWriter
    include Tuhura::Common::Logger

    def self.file_ext
      'avr'
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
      logger_init()

      @name = name
      @defaults = {}
      @boolean_fields = []
      fields = schema[:cols].map do |n, t, default|
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
        {'name' => n, 'type' => type}
      end
      @keys = fields.map {|f| f['name']}
      schema_desc = {
        "type" => "record",
        "name" => schema[:name] || name,
        'version' => (schema[:version] || 0),
        "aliases" => aliases,
        "fields" => fields
      }
      #puts ">>> SCHEMA(#{name}): #{schema_desc}"
      @avro_schema = Avro::Schema.parse(schema_desc.to_json)
      @writer = Avro::IO::DatumWriter.new(@avro_schema)
      @dw = Avro::DataFile::Writer.new(out_stream, @writer, @avro_schema)
    end

    def validate_fields(record)
      datum = @defaults.merge(record)
      res = Avro::Schema.validate(@avro_schema, datum)
      #puts "----- #{res} -- #{record['data'].inspect}--- #{datum.diff($datum)}"
      @avro_schema.fields.each do |f|
        v = datum[f.name]
        unless Avro::Schema.validate(f.type, v)
          warn "Field '#{f.name}' should be of type #{f.type} but is '#{v}'::#{v.class} - #{datum}"
        end
      end
      r = record.dup
      record.each {|k, v| r.delete(k)}
      unless r.empty?
        warn "Unknown fields '#{r.keys}' in record, but not in schema"
      end
    end

    def <<(record)
      @boolean_fields.each do |n|
        # fix boolean
        v = record[n]
        record[n] = true if v == 1
        record[n] = false if v == 0
      end
      @dw << @defaults.merge(record)
    end

    def flush
      @dw.flush
    end

    def close
      @dw.close
    end

  end
end

# class Hash
  # def diff(other)
    # self.keys.inject({}) do |memo, key|
      # unless self[key] == other[key]
        # memo[key] = [self[key], other[key]]
      # end
      # memo
    # end
  # end
# end
