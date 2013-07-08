require 'tuhura/common/logger'
require 'tuhura/aws/s3/avro_writer'
require 'monitor'

module Tuhura::AWS::S3
  class Table
    include Tuhura::Common::Logger

    # Interval in seconds to call sweep
    #
    SWEEP_INTERVAL = 60

    # Close file if nothing was written during that many sweep intervals.
    #
    EPOCHS_BEFORE_CLOSE = 2

    @@tables = []
    @@lock = Mutex.new

    Thread.new do
      loop do
        @@lock.synchronize do
          @@tables.each do |t|
            begin
              t.sweep
            rescue Exception => ex
              puts "Sweep failed: #{ex}"
            end
          end
        end
        sleep SWEEP_INTERVAL
      end
    end

    def self.get(table_name, create_if_missing, schema, s3_connector, &get_schema_proc)
      schema ||= get_schema_proc ? get_schema_proc.call(table_name) : {name: table_name}
      self.new(table_name, schema, create_if_missing, s3_connector)
    end

    def self.close_all()
      @@lock.synchronize do
        @@tables.each {|t| t.close}
      end
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

      @unknown_schema = false
      unless schema[:cols]
        # unknown schema
        @unknown_schema = true
        schema[:cols] = [['msg', :string]]
      end
      @schema = schema

      @file_prefix = File.join((s3_connector.opts[:data_dir] || ''), @table_name)
      @file_opened = 0
      @avro_writer_unused = 0
      @avro_writer = nil
      @monitor = Monitor.new
      @@lock.synchronize do
        @@tables << self
      end
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

      start = Time.now
      @monitor.synchronize do
        writer = _get_writer
        events.each do |row|
          if @unknown_schema
            row = {'msg' => row.to_json }
          end
          _put_row(row, writer)
        end
      end
      i = events.length
      debug "Wrote #{i} records at #{(1.0 * i / (Time.now - start)).round(1)} rec/sec to #{@file_name}"
      i
    end

    def _put_row(row, writer, tries = 1)
      begin
        writer << row
      rescue Avro::IO::AvroTypeError => ex
        if tries < 10 && @before_dropping
          if fixed_row = @before_dropping.call(row, @schema, tries)
            #puts "----- RETRY AGAIN ----"
            return _put_row(fixed_row, writer, tries + 1)
          end
        end
        writer.validate_fields(row)
        keys = row.map {|k, v| k}
        #puts "#{@table_name}-#{ex}-\n#{ex.backtrace.join("\n")}"
        warn "DROPPING event for '#{@table_name} - '#{keys - writer.keys}' (#{ex})"
        #puts ex.backtrace.join("\n")
      end
    end

    def _get_writer
      @monitor.synchronize do
        @avro_writer_unused = 0 # reset inactivity timeout
        unless @avro_writer
          #puts "FILE_NAME: #{file_name}"
          @file_name = "#{@file_prefix}.#{@file_opened}.avr"
          info "Opening #{@file_name}"
          @file = File.open(@file_name, 'wb')
          @file_opened += 1
          @avro_writer = AvroWriter.new(@table_name, @schema, @file)
        end
        @avro_writer
      end
    end

    def get(key)
      raise "Not supported"
    end

    def sweep
      @monitor.synchronize do
        return unless @file
        @avro_writer.flush
        if (@avro_writer_unused += 1) > EPOCHS_BEFORE_CLOSE
          info "Closing #{@file_name} due to inactivity"
          close
        end
      end
    end

    def close
      @monitor.synchronize do
        @avro_writer.close # also closes @file
        @avro_writer = nil
        @file = nil
      end
    end
  end
end