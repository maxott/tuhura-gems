require 'tuhura/common/logger'
require 'tuhura/aws/s3/avro_writer'

module Tuhura::AWS::S3
  class Table
    include Tuhura::Common::Logger

    def self.get(table_name, create_if_missing, s3_connector, &get_schema_proc)
      schema = get_schema_proc ? get_schema_proc.call(table_name) : [[:key, :string]]
      self.new(table_name, schema, create_if_missing, s3_connector)
    end

    # Constructor
    #
    def initialize(table_name, schema, create_if_missing, s3_connector)
      logger_init(nil, top: false)
      @table_name = table_name
      @head_schema = schema
      @s3_connector = s3_connector
      @initialized = false
      @no_insert_mode = @s3_connector.no_insert_mode?

      file_name = File.join((s3_connector.opts[:data_dir] || ''), "#{@table_name}.avr")
      puts "FILE_NAME: #{file_name}"
      @file = File.open(file_name, 'wb')
      @unknown_schema = false
      unless schema[:cols]
        # unknown schema
        @unknown_schema = true
        schema[:cols] = [['msg', :string]]
      end
      @schema = schema
      @avro_writer = AvroWriter.new(table_name, schema, @file)
    end

    def before_dropping(&block)
      @before_dropping = block
    end

    def put(events)
      # if @table_name == 'sen_24_w2262'
        # puts events.map {|e| e[inspect
        # exit
      # end

      if @no_insert_mode # test
        i = events.length
        info "Would have written #{i} records"
        return i
      end

      events.each do |r|
        row = r[0].merge(r[1])
        if @unknown_schema
          # puts row.map {|k,v| k}.inspect
          # exit
          row = {'msg' => row.to_json }
        end
        _put_row(row)
      end
      return events.length
    end

    def _put_row(row, tries = 1)
      begin

        @avro_writer << row
      rescue Avro::IO::AvroTypeError => ex
        if tries < 10 && @before_dropping
          if fixed_row = @before_dropping.call(row, @schema, tries)
            return _put_row(fixed_row, tries + 1)
          end
        end
        @avro_writer.validate_fields(row)
        keys = row.map {|k, v| k}
        #puts "#{@table_name}-#{ex}-\n#{ex.backtrace.join("\n")}"
        puts "   DROPPING event for '#{@table_name} - '#{keys - @avro_writer.keys}' | '#{@avro_writer.keys - keys}' - #{keys}"
      end
    end

    def get(key)
      raise "Not supported"
    end
  end
end