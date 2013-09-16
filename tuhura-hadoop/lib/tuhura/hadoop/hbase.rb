#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'hbase-jruby'
require 'tuhura/common'


module Tuhura::Common
  module HBase
  
    HBASE_OPTS = {
      # 'hbase.zookeeper.quorum' => zk_opts[:url] # resolved in hbase_init()
      :test_mode => false
    }
    
    attr_reader :hbase_opts
    
    def hbase_get_table(table_name, create_if_missing = true)
      if hbase_test_mode?
        table_name = "#{table_name}_test"
      end
      #puts ">>> TABLE_NAME: #{table_name}"

      unless inst = @hbase_tables[table_name]
        inst = @hbase_tables[table_name] = @hbase.table(table_name)
        unless inst.exists?
          raise "Table '#{table_name}' does NOT exist" unless create_if_missing
          @logger.info ">>>> CREATING #{table_name}"
          inst.create! :f => {}
          #inst.create! :f => { :compression => :snappy, :bloomfilter => :row }
        end
      end
      inst
    end
    
    # Drop all tables associated with sensation.
    # DANGER!!!!
    def hbase_drop_tables!()
      raise "No regexp to identify tables is defined" unless @hbase_table_regexp
      
      my_tables = @hbase.tables.select do |t| 
        t.name.match(@hbase_table_regexp)
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
    
    def hbase_json_field(row, col_name)
      json_str = row.string(col_name)
      m = JSON.parse(json_str)
    end

    
    def hbase_test_mode?
      @hbase_test_mode
    end
    
    # Initialize the HBASE extension
    #
    # @param opts - Options given to the hbase-jruby HBase class
    #     opts[:test_mode] - If 'true' add '_test' to all table name requests
    # @param table_regexp - Regexp to identify tables to delete in 'hbase_drop_tables'
    #
    def hbase_init(opts = {}, table_regexp = nil)
      @hbase_opts = opts = HBASE_OPTS.merge(opts)
      @hbase_opts['hbase.zookeeper.quorum'] ||= zk_opts[:url]
      
      @logger.info "Resolving HBase dependencies"
      ::HBase.resolve_dependency! 'cdh4.1' #'0.94'
      #java_import org.apache.hadoop.hbase.TableNotFoundException
      
      @hbase_tables = {} # hold mapping from table name to its instance
      @hbase_test_mode = opts.delete(:test_mode) == true
      @hbase = ::HBase.new opts 
      @hbase_table_regexp = table_regexp
    end
    
  end
end
