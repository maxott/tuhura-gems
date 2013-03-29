require "bundler/setup"
require 'kafka'
# require 'json'
# require 'zlib'
# require 'hbase-jruby'
# require 'benchmark'
# require 'optparse'
# require 'open-uri'
# require 'oml4r/benchmark'
# require 'socket'

# require 'java'
# HBase.resolve_dependency! 'cdh4.1' #'0.94'
# java_import org.apache.hadoop.hbase.TableNotFoundException

#require 'log4jruby'
#logger = Log4jruby::Logger.get($0, :tracing => true, :level => :debug)

require 'tuhura/common/logger'
require 'tuhura/common/oml'
require 'tuhura/common/zookeeper'
require 'tuhura/common/hbase'

require 'tuhura/ingestion'

module Tuhura::Ingestion
  
  class AbstractKafkaBridge
    include Tuhura::Common::Logger
    include Tuhura::Common::OML
    include Tuhura::Common::Zookeeper
    include Tuhura::Common::HBase
    #enable_logger
  
    def self.create(options = {}, &block)
      OML4R::init(ARGV, OML_OPTS) do |op|
        op.on( '-D', '--drop-tables', "Drop all table" ) do
          options[:task] = :drop_tables
        end
        op.on( '-m', '--max-messages NUM', "Number of messages to process [ALL]" ) do |n|
          options[:max_msgs] = n.to_i
        end
        op.on('-o', '--offset NUM', "Offset into Kafka queue [#{KAFKA_OPTS[:offset]}]" ) do |n|
          KAFKA_OPTS[:offset] = n.to_i
        end
        op.on('-t', '--topic TOPIC', "Name of Kafka queue to read from [#{KAFKA_OPTS[:topic]}]" ) do |s|
          KAFKA_OPTS[:topic] = s
        end
        op.on(nil, '--test', "Test mode. Append '-test' to all created HBASE tables [#{HBASE_OPTS[:test_mode]}]" ) do 
          HBASE_OPTS[:test_mode] = true
          puts "SET TEST: #{HBASE_OPTS.inspect}"
        end
        op.on('-v', '--verbose', "Verbose mode [#{options[:verbose] == true}]" ) do 
          options[:verbose] = true
        end
        op.on_tail('-h', '--help', 'Display this screen') do
          puts op
          exit
        end
  
        block.call(op) if block
      end
      self.new(options)
    end
  
    def work(opts = {}, &block)
      if block
        block.call(self)
      else 
        case opts[:task]
        when :drop_tables
          drop_tables!
        when :inject
          inject(opts[:max_msgs])
        else
          @logger.error "Unknown task '#{opts[:task]}'"
          exit(-1)
        end
      end
  
      OML4R::close
      @logger.info "Done"
    end
  
    # Deal with a single message. 'msg' is a Ruby Hash.
    # Needs to return an array of two elements, the first one is the key, the second 
    # one is the value for this 1 cell row
    #
    def process(msg)
      raise "IMPLEMENT ME!"
    end
  
    # def get_table(table_name)
      # if HBASE_OPTS[:test_mode]
        # table_name = "#{table_name}_test"
      # end
      # #puts ">>> TABLE_NAME: #{table_name}"
#   
      # unless inst = @hbase_tables[table_name]
        # inst = @hbase_tables[table_name] = @hbase.table(table_name)
        # unless inst.exists?
          # logger.info ">>>> CREATING #{table_name}"
          # inst.create! :f => {}
          # #inst.create! :f => { :compression => :snappy, :bloomfilter => :row }
        # end
      # end
      # inst
    # end
  
    # def create_key(day_ts, user_id = nil, payload = nil)
      # key = day_ts << 64
      # if user_id
        # key += (user_id << 32)
      # end
      # if payload
        # key += Zlib::crc32(payload)
      # end
      # key.to_java.toByteArray
    # end
  
    # Drop all tables associated with sensation.
    # DANGER!!!!
    def drop_tables!()
      puts "*********************************************************"
      puts "* Do you REALLY want to drop all Sensation tables?"
      puts "*"
      puts "*   You have 10 seconds to reconsider - just press Ctl-C "
      puts "*********************************************************"
      sleep 10
      @hbase.tables.each do |t| 
        if t.name.match(@table_regexp)
          puts ">>>>>> DROPPING #{t.name}"
          t.drop!
        end
      end
    end
  
    def inject(max_count = -1)
      reporting_interval = 10
      total_count = 0
      indv_counts = {}
      bm_r = OML4R::Benchmark.bm('kafka_read')
      bm_w = OML4R::Benchmark.bm('hbase_write')
      OML4R::Benchmark.bm('overall', periodic: reporting_interval) do |bm|
        loop do 
          groups = {}
          msg_cnt = 0
          bm_r.task do 
            records = @kafka_consumer.consume
            break if records.nil? || records.empty?
            records.each do |m|
              #puts "MMM>>> #{m.inspect}"
              _parse_and_process(m).each do |group_id, k, v|
                next if group_id.nil?
                (groups[group_id] ||= {})[k] = v
                indv_counts[group_id] = (indv_counts[group_id] || 0) + 1
              end
              msg_cnt += 1
            end
            bm_r.step msg_cnt
            bm.step msg_cnt
          end
          break if msg_cnt == 0
          bm_r.report
          @logger.info ">>>> Read #{msg_cnt} record(s) - offset: #{@kafka_consumer.offset}"
  
          cnt = 0
          bm_w.task do
            groups.each do |group_id, events|
              table = hbase_get_table(group_id)
              cnt += table.put(events)
            end
            bm_w.step cnt
          end
          total_count += cnt
          # Persist progress
          _persist_offset(@kafka_consumer.offset)
          bm_w.report
          @logger.info ">>>> Wrote #{cnt}/#{total_count} record(s)" if @verbose
          break if (max_count > 0 && total_count >= max_count) 
        end
      end
      bm_r.stop
      bm_w.stop
      @logger.info ">>>> SUMMARY: Wrote #{total_count} record(s) - offset: #{@kafka_consumer.offset}"
      indv_counts.each do |group_id, cnt|
        @logger.info ">>>>      #{group_id}:\t#{cnt}"
      end
    end
  
    def initialize(opts)
      logger_init()
      oml_init(opts)
      @kafka_opts = KAFKA_OPTS.merge(opts[:kafka] || {})
      @topic = @kafka_opts[:topic]
      _init_zk()
      hbase_init()
      if (offset = @kafka_opts[:offset]) < 0
        if offset_s = zk_get(@offset_path)
          offset = offset_s.to_i
        else
          offset = 0
        end
        @kafka_opts[:offset] = offset
      end
      @kafka_consumer = Kafka::Consumer.new(@kafka_opts)
      @logger.info "Using Kafak offset: #{@kafka_opts[:offset]}"
      
      # @hbase_tables = {} # hold mapping from table name to its instance
      # @hbase = HBase.new hbase_opts 
      # @verbose = opts[:verbose] == true
    end
  
    def _parse_and_process(kafka_msg)
      begin
        payload = kafka_msg.payload
        msg = JSON.parse(payload)
      rescue Exception => ex
        @logger.error "While parsing JSON - #{ex} - #{URI::encode(payload)}"
        nil
      end
      begin
        process(msg, payload)
      rescue Exception => ex
        @logger.error "While processing message - #{ex}::#{ex.class}"
        @logger.warn ex.backtrace.join("\n\t")
        []
      end
  
    end
  
    def _init_zk()
      zk_init()
      unless hbase_test_mode?
        path_prefix = "/incmg/kafka_bridge/#{@topic}"
      else
        path_prefix = "/incmg/kafka_bridge/test/#{@topic}"
      end
      @offset_path = "#{path_prefix}/offset"
    end

    def _persist_offset(offset)
      zk_put(@offset_path, offset.to_s)
    end
  end # class
end # module






