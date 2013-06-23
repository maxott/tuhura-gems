
require 'tuhura/common'


module Tuhura::Common
  module Database

    DB_OPTS = {
      provider: 'dynamo_db',
      test_mode: false,
      no_insert: false
    }

    attr_reader :db_opts

    def db_get_table(table_name, create_if_missing = true, &get_schema)
      if db_test_mode?
        table_name = "#{table_name}_test"
      end
      #puts ">>> TABLE_NAME: #{table_name}"

      unless inst = @db_tables[table_name]
        inst = @db_tables[table_name] = @db.get_table(table_name, create_if_missing, &get_schema)
        # unless inst.exists?
          # raise "Table '#{table_name}' does NOT exist" unless create_if_missing
          # @logger.info ">>>> CREATING #{table_name}"
          # inst.create! :f => {}
          # #inst.create! :f => { :compression => :snappy, :bloomfilter => :row }
        # end
      end
      inst
    end

    # Drop all tables associated with sensation.
    # DANGER!!!!
    def db_drop_tables!()
      raise "No regexp to identify tables is defined" unless @db_table_regexp

      my_tables = @db.tables.select do |t|
        t.name.match(@db_table_regexp)
      end

      puts "*********************************************************"
      puts "* Do you REALLY want to drop the following tables?"
      my_tables.each do |t|
        puts "*           * #{t.name}"
      end
      puts "*"
      puts "*   You have 10 seconds to reconsider - just press Ctl-C "
      puts "*********************************************************"
      sleep 10
      my_tables.each do |t|
        puts ">>>>>> DROPPING #{t.name}"
        t.drop!
      end
    end

    def db_json_field(row, col_name)
      json_str = row.string(col_name)
      m = JSON.parse(json_str)
    end


    def db_test_mode?
      @db_test_mode
    end

    # Initialize the HBASE extension
    #
    # @param opts - Options given to the db-jruby HBase class
    #     opts[:test_mode] - If 'true' add '_test' to all table name requests
    # @param table_regexp - Regexp to identify tables to delete in 'db_drop_tables'
    #
    def db_init(opts = {}, table_regexp = nil)
      @db_opts = opts = DB_OPTS.merge(opts || {})

      case provider = (opts[:provider] || :unknown).to_s.downcase
      when 'dynamo_db'
        require 'tuhura/aws/dynamo_db'
        @db = Tuhura::AWS::DynamoDB.create(opts)
      when 's3'
        require 'tuhura/aws/s3'
        @db = Tuhura::AWS::S3.create(opts)
      else
        raise "Unknown database provider '#{provider}'"
      end

      @db_tables = {} # hold mapping from table name to its instance
      @db_test_mode = opts.delete(:test_mode) == true
      @db_table_regexp = table_regexp
    end

  end
end