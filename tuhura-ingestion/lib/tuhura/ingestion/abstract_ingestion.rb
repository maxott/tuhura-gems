#require "bundler/setup"

require 'tuhura/common/logger'
require 'tuhura/common/oml'
require 'tuhura/common/state'
require 'tuhura/common/database'

require 'tuhura/ingestion'
require 'tuhura/ingestion/kafka_ingestion'

$default_provider = 'aws'

module Tuhura::Ingestion

  class AbstractIngestion

    # KAFKA_OPTS = {
      # #offset: -1,
      # host: 'cloud1.tempo.nicta.org.au'
    # }

    include Tuhura::Common::Logger
    include Tuhura::Common::OML
    include Tuhura::Common::Database
    include Tuhura::Common::State
    include Tuhura::Ingestion::Kafka
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
          Tuhura::Ingestion::Kafka::KAFKA_OPTS[:offset] = n.to_i
        end
        op.on('-t', '--topic TOPIC', "Name of Kafka queue to read from [#{KAFKA_OPTS[:topic]}]" ) do |s|
          Tuhura::Ingestion::Kafka::KAFKA_OPTS[:topic] = s
        end
        op.on('-h', '--host HOST', "Name of Kafka host [#{KAFKA_OPTS[:host]}]" ) do |h|
          Tuhura::Ingestion::Kafka::KAFKA_OPTS[:host] = h
        end

        op.on('-a', '--avro-decl FILE', "File containing AVRO definitions for records" ) do |af|
          options[:avro_file] = af
        end

        if $default_provider == 'aws'
          require 'tuhura/aws'
          Tuhura::AWS.configure_opts(op)
        end

        op.on('', '--db-provider PROVIDER', "Provider for database capability [#{Tuhura::Common::Database::DB_OPTS[:provider]}]" ) do |provider|
          (options[:database] ||= {})[:provider] = provider
        end
        op.on(nil, '--db-noinsert', "Test mode. Do NOT insert data into database [#{Tuhura::Common::Database::DB_OPTS[:no_insert]}]" ) do
          (options[:database] ||= {})[:no_insert] = true
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
          kafka_init(@opts[:kafka])
          kafka_inject(opts[:max_msgs])
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
    def ingest_json_message(msg, payload)
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

    def get_table_for_group(group_name)
      #puts "GET TABLE: #{group_name}"
      db_get_table(group_name, &method(:get_schema_for_table))
    end

    def initialize(opts)
      @opts = opts
      puts opts.inspect
      logger_init()
      oml_init(opts[:oml])
      db_init(opts[:database])
      state_init(opts[:state])

      # @kafka_opts = KAFKA_OPTS.merge(opts[:kafka] || {})
      # @topic = @kafka_opts[:topic]
      # _init_state(@topic, opts[:state])
#
      # unless offset = @kafka_opts[:offset]
        # if offset_s = state_get(@offset_path)
          # offset = offset_s.to_i
        # else
          # offset = 0
        # end
        # @kafka_opts[:offset] = offset
      # end
      # info "Kafka options: #{@kafka_opts}"
      # @kafka_consumer = Kafka::Consumer.new(@kafka_opts)
      # @logger.info "Reading Kafka queue '#{@topic}' with offset '#{@kafka_opts[:offset]}'"
    end

    # def _init_state(topic, opts)
      # state_init(opts)
      # unless db_test_mode?
        # path_prefix = "/incmg/kafka_bridge/#{topic}"
      # else
        # path_prefix = "/incmg/kafka_bridge/test/#{topic}"
      # end
      # @offset_path = "#{path_prefix}/offset"
    # end

    # def _persist_offset(offset)
      # state_put(@offset_path, offset)
    # end


  end # class
end # module






