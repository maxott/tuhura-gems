#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/common/logger'
require 'monitor'
require 'tuhura/level_db'
require 'leveldb'

module Tuhura::LevelDB
  class Table
    include Tuhura::Common::Logger
    include MonitorMixin
    extend MonitorMixin

    @@tables = {}

    def self.get(table_name, create_if_missing, schema, connector = nil, &get_schema_proc)
      synchronize do
        #puts "TABLE: #{table_name}"
        if m = table_name.match(/(.*)_[a-z][0-9]*$/)
          # merge all weekly, monthly tables into a single one
          table_name = m[1]
        end
        unless table = @@tables[table_name]
          if create_if_missing
            schema ||= get_schema_proc ? get_schema_proc.call(table_name) : {name: table_name}
            table = @@tables[table_name] = self.new(table_name, schema, connector)
          else
            # TODO: Should I throw an exception or simply return nil
          end
        end
        table
      end
    end

    def self.close_all
      synchronize do
        @@tables.values.each do |table|
          table.close
        end
        @@tables = {}
      end
    end


    # Constructor
    #
    def initialize(table_name, schema, connector)
      super() # initialises MonitorMixin
      logger_init(nil, top: false)
      @table_name = table_name
      @schema = schema
      @connector = connector
      unless @no_insert_mode = connector ? connector.no_insert_mode? : false
        fname = File.join(CONFIG_OPTS[:db_dir], table_name.to_s)
        debug "opening leveldb database '#{fname}'"
        @leveldb = ::LevelDB::DB.new(fname)
        unless @primary_key = schema[:primary]
          raise "Missing primary key in '#{schema}'"
        end
        @range_key = schema[:range]
      end
    end

    def put(events)
      if events.is_a? Hash
        events = [events] # put of a single record
      end
      size = events.size
      if @no_insert_mode # test
        info "Would have written #{size} records"
        return set.size
      end
      synchronize do
        events.each do |e|
          unless key = e[@primary_key]
            raise "Missing primary key '#{@primary_key}' in event '#{e}'"
          end
          if @range_key
            if range = e[@range_key]
              key = "#{key}:#{range}"
            else
              raise "Missing range key '#{@range_key}' in event '#{e}'"
            end
          else
            key = key.to_s
          end
          #puts "KEY>>> #{key}"
          @leveldb.put(key, Marshal.dump(e))
        end
      end
      size
    end

    # Return the value stored with 'key' and if defined, a 'range'
    # parameter.
    #
    def get(key, range = nil)
      raise "Can't do that in test mode" if @no_insert_mode # test
      raise "Missing range key '#{@range_key}'" if @range_key && range.nil?

      key = range ? "#{key}:#{range}" : key.to_s
      value = Marshal.load(@leveldb.get(key))
      puts "GET value: #{value}"
      value
    end

    # Retrieve records starting at 'from_key' up to (and not including) 'to_key'
    # and call 'block' with every record. If 'to_key' is nil, set it to 'from_key.succ'
    #
    def each(from_key = nil, to_key = nil, &block)
      raise "Can't do that in test mode" if @no_insert_mode # test

      opts = {}
      if from_key
        from_key = opts[:from] = from_key.to_s
        opts[:to] = to_key ? to_key.to_s : from_key.succ
      end
      @leveldb.each(opts) do |k, vr|
        v = Marshal.load(vr)
        block.call(v)
      end
    end

    def close()
      @leveldb.close
      @leveldb = nil
    end
  end
end

if __FILE__ == $0
  puts "Primary only"
  Tuhura::Common::Logger.global_init
  schema = {primary: :key}
  tbl = Tuhura::LevelDB::Table.get('test_foo', true, schema)
  tbl.put([key: :foo, attr1: 'foo', attr2: [1, 2]])
  v = tbl.get(:foo)
  puts "GET: <#{v}>::#{v.class}"
  tbl.close

  puts "With range"
  tbl = Tuhura::LevelDB::Table.get('test_goo', true, {primary: :p, range: :r})
  tbl.put([p: :foo, r: 12, attr1: 'foo', attr2: [1, 2]])
  v = tbl.get(:foo, 12)
  puts "GET: <#{v}>::#{v.class}"

  tbl.put([p: :foo, r: 6, attr1: 'goo', attr2: [12, 23]])
  tbl.put([p: :goo, r: 10, attr2: [33, 13]])

  puts tbl.each(:foo) {|e| puts e}
  tbl.close

end
