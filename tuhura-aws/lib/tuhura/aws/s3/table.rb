require 'tuhura/common/logger'
require 'tuhura/aws/s3/avro_writer'
require 'tuhura/aws/s3/csv_writer'
require 'monitor'

module Tuhura::AWS::S3
  class Table
    include Tuhura::Common::Logger
    extend Tuhura::Common::Logger

    # Interval in seconds to call sweep
    #
    SWEEP_INTERVAL = 60

    # Close file if nothing was written during that many sweep intervals.
    #
    EPOCHS_BEFORE_CLOSE = 2

    @@tables = []
    @@lock = Mutex.new


    def self.get(table_name, create_if_missing, schema, s3_connector, opts, &get_schema_proc)
      schema ||= get_schema_proc ? get_schema_proc.call(table_name) : {name: table_name}
      self.new(table_name, schema, create_if_missing, s3_connector, opts)
    end

    def self.close_all()
      @@lock.synchronize do
        @@tables.each {|t| t.close}
      end
    end

    def self.sweep(force = false)
      @@lock.synchronize do
        unused_epochs = EPOCHS_BEFORE_CLOSE
        if force
          # make sure at leat one file is being closed
          unused_epochs = 0
          @@tables.each do |t|
            if (ue = t.unused_epochs) > unused_epochs
              unused_epochs = ue
            end
          end
        end
        closing_count = 0
        @@tables.each do |t|
          begin
            closing_count += t.sweep(unused_epochs)
          rescue Exception => ex
            puts "Sweep failed: #{ex}"
          end
        end
       if (closing_count > 0)
         info "Closed #{closing_count} files due to inactivity within the last #{unused_epochs} epochs"
       end
      end
    end

    # Call sweep periodically
    Thread.new do
      loop do
        sleep SWEEP_INTERVAL
        sweep
      end
    end


    # Constructor
    #
    def initialize(table_name, schema, create_if_missing, s3_connector, opts)
      logger_init()
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
      version = schema[:version] || 0

      case (opts[:format] || 'avro').to_s
      when 'avro'
        @writer_class = AvroWriter
      when 'csv'
        @writer_class = CSVWriter
      else
        raise "Unknown Table Writer '#{opts[:writer]}' - supporting 'avro' and 'csv'"
      end
      @file_prefix = File.join((s3_connector.opts[:data_dir] || ''), "#{@table_name}_v#{version}")
      @writer_unused = 0
      @writer = nil
      @monitor = Monitor.new
      @@lock.synchronize do
        @@tables << self
      end
    end

    def before_dropping(&block)
      @before_dropping = block
    end

    def put(events)
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
        @writer_unused = 0 # reset inactivity timeout
        unless @writer
          # DEADLOCK ALERT: As opening a file may first require a sweep of unused
          # file descriptors to allow for the opening of a new one, we need to
          # leave the monitor as a sweep may be triggered on hte periodic cleanup thread
          #
          @monitor.exit
          file_name = "#{@file_prefix}.#{rand(1000000000)}.#{@writer_class.file_ext}"
          debug "Opening #{file_name}"
          file = _open_file(file_name)
          @monitor.enter
          # if we are using this table in a multi-threaded environment, some other thread
          # may have created a writer already, so lets check again before commiting
          if @writer
            file.close # ok, don't need it
            return @writer
          end
          @file_name = file_name
          @file = file
          @writer = @writer_class.new(@table_name, @schema, @file)
        end
        @writer
      end
    end

    def _open_file(name, attempts = 0)
      if attempts > 5
        raise "Giving up opening file '#{name}'"
      end
      begin
        debug "Opening #{name}"
        file = File.open(name, 'wb')
      rescue Exception => ex
        if ex.is_a? Errno::EMFILE
          self.class.sweep(true) # force some file closings and try again
          return _open_file(name, attempts + 1)
        end
        warn "Error while opening file '#{name}' - #{ex}::#{ex.class}"
      end
      file
    end

    def get(key)
      raise "Not supported"
    end

    def unused_epochs
      @writer_unused
    end

    # Close the underlying file if this table hasn't been access in
    # some time to limit the number of open file descriptors. Returns the
    # number of closed files.
    #
    def sweep(unused_epochs)
      @monitor.synchronize do
        return 0 unless @file
        @writer.flush
        if (@writer_unused += 1) > unused_epochs
          debug "Closing #{@file_name} due to inactivity"
          close
          return 1
        end
      end
      return 0
    end

    def close
      @monitor.synchronize do
        @writer.close if @writer # also closes @file
        @writer = nil
        @file = nil
      end
    end
  end
end