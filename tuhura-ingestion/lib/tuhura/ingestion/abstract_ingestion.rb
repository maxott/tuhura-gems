#require "bundler/setup"

require 'tuhura/common/logger'
require 'tuhura/common/oml'
require 'tuhura/common/database'

require 'tuhura/ingestion'

$default_provider = 'aws'

module Tuhura::Ingestion

  OPTS = {
    reader: 'json',
    kafka: {
      offset: -1,
      host: 'cloud1.tempo.nicta.org.au',
      state_domain: 'kafka_bridge'  # part of the state prefix - change if private namespace is desired
    },
    json: {
      skip_lines: 0
    }

  }

  class AbstractIngestion


    include Tuhura::Common::Logger
    include Tuhura::Common::OML
    include Tuhura::Common::Database
    # include Tuhura::Common::Zookeeper
    # include Tuhura::Common::HBase
    #enable_logger

    def self.create(options = {}, &block)
      options.merge!(OPTS) {|k, v1, v2| v1 } # reverse hash
      OML4R::init(ARGV, OML_OPTS) do |op|
        op.on( '-D', '--drop-tables', "Drop all table" ) do
          options[:task] = :drop_tables
        end
        op.on( '-m', '--max-messages NUM', "Number of messages to process [ALL]" ) do |n|
          options[:max_msgs] = n.to_i
        end
        op.on( '-r', '--reader READER', "Reader to use for fetching records [#{OPTS[:reader]}]" ) do |r|
          options[:reader] = r
        end
        op.separator ""
        op.separator "Kafka Reader options:"
        k_opts = OPTS[:kafka]
        op.on('-o', '--offset NUM', "Offset into Kafka queue [#{k_opts[:offset]}]" ) do |n|
          k_opts[:offset] = n.to_i
        end
        #Tuhura::Ingestion::Kafka::KAFKA_OPTS[:topic] = options[:def_topic] if options[:def_topic]
        op.on('-t', '--topic TOPIC', "Name of Kafka queue to read from [#{k_opts[:topic]}]" ) do |s|
          k_opts[:topic] = s
        end
        op.on('--host HOST', "Name of Kafka host [#{k_opts[:host]}]" ) do |h|
          k_opts[:host] = h
        end

        op.separator ""
        op.separator "JSON Reader options:"
        j_opts = OPTS[:json]
        op.on('-f', '--json-file-name IN_FILE', "Name of file to read json records from" ) do |n|
          j_opts[:file_name] = n
        end
        op.on('--skip-lines NUM', "Number of lines from the input file to skip initially [#{j_opts[:skip_lines]}]" ) do |n|
          j_opts[:skip_lines] = n.to_i
        end


        if $default_provider == 'aws'
          require 'tuhura/aws'
          Tuhura::AWS.configure_opts(op)
        end

        op.separator ""
        op.separator "Storage options:"
        op.on('--db-provider PROVIDER', "Provider for database capability [#{Tuhura::Common::Database::DB_OPTS[:provider]}]" ) do |provider|
          (options[:database] ||= {})[:provider] = provider
        end
        op.on('--db-format FORMAT', "Format used for S3 provider [avro]" ) do |format|
          (options[:database] ||= {})[:format] = format
        end
        op.on('--db-noinsert', "Test mode. Do NOT insert data into database [#{Tuhura::Common::Database::DB_OPTS[:no_insert]}]" ) do
          (options[:database] ||= {})[:no_insert] = true
        end
        op.on('-s', '--state-domain DOMAIN', "State domain for keeping ingestion state [#{KAFKA_OPTS[:state_domain]}]" ) do |domain|
          (options[:kafka] ||= {})[:state_domain] =  domain
        end
        op.on('-a', '--avro-decl FILE', "File containing AVRO definitions for records" ) do |af|
          options[:avro_file] = af
        end
        op.separator ""
        op.separator "Testing options:"
        op.on('--test', "Test mode. Append '-test' to all created HBASE tables [#{Tuhura::Common::Database::DB_OPTS[:test_mode]}]" ) do
          (options[:database] ||= {})[:test_mode] = true
        end
        op.separator ""
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
          case opts[:reader]
          when 'kafka'
            require 'tuhura/common/state'
            require 'tuhura/ingestion/kafka_reader'
            self.class.send(:include, Tuhura::Common::State)  # needed for keeping track of Kafka pointer
            self.class.send(:include, Tuhura::Ingestion::KafkaReader)
            kafka_init(opts[:kafka])
            kafka_inject(opts[:max_msgs])
          when 'json'
            require 'tuhura/ingestion/json_file_reader'
            self.class.send(:include, Tuhura::Ingestion::JsonFileReader)
            json_file_reader_init(opts[:json])
            json_file_reader_inject(opts[:max_msgs])
          else
            error "Unknown reader '#{opts[:reader]}' - #{opts}"
          end
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
    end


  end # class
end # module






