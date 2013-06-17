#require "bundler/setup"
require 'kafka'

require 'tuhura/common/logger'
require 'tuhura/common/oml'
require 'tuhura/common/state'
require 'tuhura/common/database'

require 'tuhura/ingestion'

module Tuhura::Ingestion

  class AbstractKafkaBridge2

    KAFKA_OPTS = {
      offset: -1,
      host: 'cloud1.tempo.nicta.org.au'
    }

    include Tuhura::Common::Logger
    include Tuhura::Common::OML
    include Tuhura::Common::Database
    include Tuhura::Common::State
    # include Tuhura::Common::Zookeeper
    # include Tuhura::Common::HBase
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
        op.on('-h', '--host HOST', "Name of Kafka host [#{KAFKA_OPTS[:host]}]" ) do |h|
          KAFKA_OPTS[:host] = h
        end
        op.on('', '--aws-creds IDSECRET', "AWS Credentials" ) do |token|
          (options[:database] ||= {})[:aws_creds] = token
        end
        op.on(nil, '--test', "Test mode. Append '-test' to all created HBASE tables [#{Tuhura::Common::Database::DB_OPTS[:test_mode]}]" ) do
          (options[:database] ||= {})[:test_mode] = true
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
          error "Unknown task '#{opts[:task]}'"
          exit(-1)
        end
      end

      OML4R::close
      info "Done"
    end

    # Deal with a single message. 'msg' is a Ruby Hash.
    # Needs to return an array of two elements, the first one is the key, the second
    # one is the value for this 1 cell row
    #
    def process(msg)
      raise "IMPLEMENT ME!"
    end

    # Drop all tables associated with sensation.
    # DANGER!!!!
    def drop_tables!()
      puts "*********************************************************"
      puts "* Do you REALLY want to drop all tables?"
      puts "*"
      puts "*   You have 10 seconds to reconsider - just press Ctl-C "
      puts "*********************************************************"
      sleep 10
      # @hbase.tables.each do |t|
        # if t.name.match(@table_regexp)
          # puts ">>>>>> DROPPING #{t.name}"
          # t.drop!
        # end
      # end
    end

    def inject(max_count = -1)
      reporting_interval = 10
      total_count = 0
      indv_counts = {}
      bm_r = OML4R::Benchmark.bm('kafka_read')
      bm_w = OML4R::Benchmark.bm('db_write')
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
                (groups[group_id] ||= []) << [k, v]
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
              table = get_table_for_group(group_id)
              cnt += table.put(events)
            end
            bm_w.step cnt
          end
          total_count += cnt
          # Persist progress
          _persist_offset(@kafka_consumer.offset)
          bm_w.report
          info ">>>> Wrote #{cnt}/#{total_count} record(s)" if @verbose
          break if (max_count > 0 && total_count >= max_count)
        end
      end
      bm_r.stop
      bm_w.stop
      info ">>>> SUMMARY: Wrote #{total_count} record(s) - offset: #{@kafka_consumer.offset}"
      indv_counts.each do |group_id, cnt|
        info ">>>>      #{group_id}:\t#{cnt}"
      end
    end

    def get_table_for_group(group_name)
      db_get_table(group_name)
    end

    def initialize(opts)
      puts opts.inspect
      logger_init()
      oml_init(opts[:oml])
      db_init(opts[:database])
      _init_state(opts[:state])

      @kafka_opts = KAFKA_OPTS.merge(opts[:kafka] || {})
      @topic = @kafka_opts[:topic]

      if (offset = @kafka_opts[:offset]) < 0
        if offset_s = state_get(@offset_path)
          offset = offset_s.to_i
        else
          offset = 0
        end
        @kafka_opts[:offset] = offset
      end
      @kafka_consumer = Kafka::Consumer.new(@kafka_opts)
      @logger.info "Reading Kafka queue '#{@kafka_opts[:topic]}' with offset '#{@kafka_opts[:offset]}'"
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

    def _init_state(opts)
      state_init(opts)
      unless db_test_mode?
        path_prefix = "/incmg/kafka_bridge/#{@topic}"
      else
        path_prefix = "/incmg/kafka_bridge/test/#{@topic}"
      end
      @offset_path = "#{path_prefix}/offset"
    end

    def _persist_offset(offset)
      state_put(@offset_path, offset)
    end


  end # class
end # module






