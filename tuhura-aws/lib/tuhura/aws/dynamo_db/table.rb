require 'tuhura/common/logger'

module Tuhura::AWS::DynamoDB
  class Table
    include Tuhura::Common::Logger

    def self.get(table_name, create_if_missing, connector, &get_schema_proc)
      schema = get_schema_proc ? get_schema_proc.call(table_name) : [[:key, :string]]
      puts "SCHEMA FOR: #{table_name} => #{schema}"

      self.new(table_name, schema, create_if_missing, connector)
    end

    TYPE2TYPE = {
      :string => :string,
      :integer => :number,
      :real => :number,
      :double => :number,
      :blob => :binary,
    }
    TYPE2TYPE.default = :string

    # Constructor
    #
    def initialize(table_name, schema, create_if_missing, connector)
      @table_name = table_name
      @schema = schema
      @db = connector
      @table = @db.tables[table_name]
      unless @table.exists?
        opts = {}
        hk = schema[0]
        opts[:hash_key] = { hk[0] => TYPE2TYPE[hk[1]] }
        if rk = schema[1]
          opts[:range_key] = { rk[0] => TYPE2TYPE[rk[1]] }
        end
        @table = @db.tables.create(table_name, 100, 1000, opts)
        puts "CREATING TABLE #{table_name} - #{@table.status}"
        sleep 1 while @table.status == :creating
      end
      #@table.provision_throughput :read_capacity_units => 10, :write_capacity_units => 20
      puts "CREATED TABLE #{table_name} - #{@table.status}"
    end

    def put(events)
      puts "--- #{@table_name} -------"
      start = Time.now
      #items = @table.items
      i = 0
      items = []
      set = Set.new
      keep_track = {}
      events.each do |k, v|
        e = v.merge(k)
        e.each do |k, v|
          e[k] = '__T__' if v.is_a? TrueClass
          e[k] = '__F__' if v.is_a? FalseClass
        end
        if set.add?(k)
          keep_track[k] = v
          items << e
          i += 1
        else
          #puts "DUPLICTAE: \n#{k} - #{v}\n#{k} - #{keep_track[k]}"
        end
      end
      items.each_slice(25) do |it|
        begin
          @table.batch_put(it)
        rescue Exception => ex
          puts ex
          puts @schema
          (it.map {|r| r['recommendation_id']}.sort).each {|r| puts r}

          it.each {|r| puts r}
          raise ex
        end
      end
      puts "----------- #{i}:#{1.0 * i / (Time.now - start)}"
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