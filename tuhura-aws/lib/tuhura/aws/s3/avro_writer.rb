require 'tuhura/common/logger'
require 'avro'

module Tuhura::AWS::S3
  #
  # Class to serialise records into an Avro chunk
  #
  class AvroWriter
    include Tuhura::Common::Logger

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
        "aliases" => aliases,
        "fields" => fields
      }
      puts "SCHEMA(#{name}): #{schema_desc}"
      @avro_schema = Avro::Schema.parse(schema_desc.to_json)
      @writer = Avro::IO::DatumWriter.new(@avro_schema)
      @dw = Avro::DataFile::Writer.new(out_stream, @writer, @avro_schema)
    end

    def validate_fields(record)
      datum = @defaults.merge(record)
      @avro_schema.fields.each do |f|
        v = datum[f.name]
        unless Avro::Schema.validate(f.type, v)
          warn "Field '#{f.name}' should be of type #{f.type} but is '#{v}'::#{v.class} - #{datum}"
        end
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
  end
end
