#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/common/logger'

module Tuhura::AWS::DynamoDB
  class Table
    include Tuhura::Common::Logger

    def self.get(table_name, create_if_missing, schema, database, &get_schema_proc)
      schema ||= get_schema_proc ? get_schema_proc.call(table_name) : {name: table_name}
      self.new(table_name, schema, create_if_missing, database)
    end

    def self.close_all
      # TODO: What do we need to close tables
    end

    TYPE2TYPE = {
      :string => :string,
      :integer => :number,
      :long => :number,
      :real => :number,
      :double => :number,
      :blob => :binary,
    }
    TYPE2TYPE.default = :string

    # Constructor
    #
    def initialize(table_name, schema, create_if_missing, database)
      logger_init(nil, top: false)
      @table_name = table_name
      @schema = schema
      @database = database
      @db = database.connector
      @no_insert_mode = database.no_insert_mode?
      @table = @db.tables[table_name]
      unless @table.exists?
        opts = {}
        # schema = {name: schema_name, primary: 'day', range: 'range', cols: schema}
        cols = schema[:cols]
        find_type = lambda do |name|
          unless col = cols.find {|t| t[0] == name}
            raise "Missing column declaration of key '#{name}'"
          end
          TYPE2TYPE[col[1]]
        end
        unless hk = schema[:primary]
          raise "Missing primary key in '#{schema}'"
        end
        opts[:hash_key] = { hk => find_type.call(hk) }
        if rk = schema[:range]
          opts[:range_key] = { rk => find_type.call(rk) }
        end
        # puts ">>>> WOULD CREATE TABLE WITH '#{opts}' - #{schema}"
        # raise

        @table = @db.tables.create(table_name, 100, 1000, opts)
        info "CREATING TABLE #{table_name} schema: #{schema} - status: #{@table.status}"
        sleep 1 while @table.status == :creating
      end
      #@table.provision_throughput :read_capacity_units => 10, :write_capacity_units => 20
      debug "Using table #{table_name} - #{@table.status}"
    end

    def put(events)
      #puts "EVENTS>> #{events}"
      start = Time.now
      #items = @table.items
      i = 0
      items = []
      set = Set.new
      keep_track = {}
      events.each do |e|
        e.each do |k, v|
          e[k] = '__T__' if v.is_a? TrueClass
          e[k] = '__F__' if v.is_a? FalseClass
          if v.respond_to?(:each)
            e[k] = v.to_json
          end
        end
        set.add(e)
      end
      if @no_insert_mode # test
        info "Would have written #{i} records"
        return set.size
      end

      set.each_slice(25) do |it|
        begin
          @table.batch_put(it)
        rescue Exception => ex
          puts ex
          puts "--------------"
          # puts it.inspect
          # puts "3---------------"
          # puts @schema
          # puts "5---------------"
          it.each {|r| puts r}
          raise ex
        end
      end
      i = set.size
      debug "Wrote #{i} records at #{(1.0 * i / (Time.now - start)).round(1)} rec/sec to #{@table_name}"
      i
    end

    def get(key)
      puts "GET key: #{key}::#{key.class}"
      value = @table.items[key].attributes.to_hash
      puts "GET value: #{value}"
      value
    end
  end
end
