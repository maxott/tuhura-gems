require "bundler/setup"
require 'kafka'
require 'json'
require 'zlib'
require 'hbase-jruby'
require 'benchmark'
require 'optparse'
require 'open-uri'
require 'oml4r/benchmark'
require 'zookeeper'
require 'socket'

require 'java'
HBase.resolve_dependency! 'cdh4.1' #'0.94'
java_import org.apache.hadoop.hbase.TableNotFoundException

require 'log4jruby'
logger = Log4jruby::Logger.get($0, :tracing => true, :level => :debug)

ZOOKEEPER_OPTS = {
  url: 'zk.incmg.net'
}

KAFKA_OPTS = {
  :host => "10.129.80.2", 
  :topic => "sensation0", 
  :offset => -1
}

HBASE_OPTS = {
  'hbase.zookeeper.quorum' => ZOOKEEPER_OPTS[:url]
}

OML_OPTS = {
  appName: 'kafka_bridge',
  domain: 'benchmark', 
  nodeID: "#{Socket.gethostbyname(Socket.gethostname)[0]}-#{Process.pid}",
  collect: 'file:-'
}

class Work
  enable_logger

  TZ_OFFSET = 2**32

  def process(msg)
    begin
      payload = msg.payload
      r = JSON.parse(payload)
      evt_type = r["event_type"]
      user_id = r["user_id"]
      timestamp = r["timestamp"]
      crc32 = Zlib::crc32(payload)
      #key = TZ_OFFSET * timestamp + user_id
      key = TZ_OFFSET * timestamp + crc32
      #logger.info "#{key} -- #{payload}"
      [evt_type, key.to_java.toByteArray, {'f:uid' => user_id, 'f:msg' => payload}]
    rescue Exception => ex
      logger.error "ERROR: While parsing JSON - #{ex} - #{URI::encode(payload)}"
      nil
    end
  end

  def get_table(table_name)
    unless inst = @hbase_tables[table_name]
      inst = @hbase_tables[table_name] = @hbase.table(table_name)
      unless inst.exists?
        logger.info ">>>> CREATING #{table_name}"
        inst.create! :f => {}
        #inst.create! :f => { :compression => :snappy, :bloomfilter => :row }
      end
    end
    inst
  end

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
      if t.name.match(/^s[0-9]+$/)
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
        evt_blocks = {}
        msg_cnt = 0
        bm_r.task do 
          records = @kafka_consumer.consume
          break if records.empty?
          records.each do |m|
            evt_type, k, v = process(m)
            next if evt_type.nil?
            (evt_blocks[evt_type] ||= {})[k] = v
            msg_cnt += 1
            indv_counts[evt_type] = (indv_counts[evt_type] || 0) + 1
          end
          bm_r.step msg_cnt
          bm.step msg_cnt
        end
	break if msg_cnt == 0
        bm_r.report
        logger.info ">>>> Read #{msg_cnt} record(s) - offset: #{@kafka_consumer.offset}"

        cnt = 0
        bm_w.task do
          evt_blocks.each do |evt_type, events|
            table_name = "s#{evt_type}"
            table = get_table(table_name)
            cnt += table.put(events)
          end
          bm_w.step cnt
        end
        total_count += cnt
        # Persist progress
        @zk.set path: @zk_path, data: @kafka_consumer.offset.to_s
        bm_w.report
        logger.info ">>>> Wrote #{cnt}/#{total_count} record(s)"
        break if (max_count > 0 && total_count >= max_count) 
      end
    end
    bm_r.stop
    bm_w.stop
    logger.info ">>>> SUMMARY: Wrote #{total_count} record(s) - offset: #{@kafka_consumer.offset}"
    indv_counts.each do |evt_type, cnt|
      logger.info ">>>>      s#{evt_type}:\t#{cnt}"
    end
  end

  def initialize(zk_opts, kafka_opts, hbase_opts)
    @topic = kafka_opts[:topic]
    _init_zk(zk_opts, kafka_opts)
    @hbase_tables = {} # hold mapping from table name to its instance
    @hbase = HBase.new hbase_opts 
  end

  def _init_zk(zk_opts, kafka_opts)
    @zk = Zookeeper.new(zk_opts[:url])
    @zk_path = "/incmg/kafka_bridge/#{@topic}/offset"
    unless @zk.get(path: @zk_path)[:stat].exists?
      @zk.create path: "/incmg/kafka_bridge/#{@topic}"
      @zk.create path: @zk_path
    end
    if (offset = kafka_opts[:offset]) < 0
      if (offset_s = @zk.get(path: @zk_path)[:data])
        offset = offset_s.to_i
      else
        offset = 0
      end
      kafka_opts[:offset] = offset
    end
    @kafka_consumer = Kafka::Consumer.new(kafka_opts)
    logger.info "Using Kafak offset: #{kafka_opts[:offset]}"
  end
end

options = {task: :inject, max_msgs: -1}
OML4R::init(ARGV, OML_OPTS) do |op|
#optparse = OptionParser.new do |op|
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
  op.on_tail('-h', '--help', 'Display this screen') do
    puts op
    exit
  end
end
#optparse.parse!


w = Work.new(ZOOKEEPER_OPTS, KAFKA_OPTS, HBASE_OPTS)
case options[:task]
when :drop_tables
  w.drop_tables!
when :inject
  w.inject(options[:max_msgs])
else
  logger.error "Unknown task '#{options[:task]}'"
  exit(-1)
end
OML4R::close
logger.info "Done"



