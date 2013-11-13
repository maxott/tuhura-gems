#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
#require "bundler/setup"

require 'tuhura/common/logger'
require 'tuhura/common/oml'
require 'tuhura/common/database'

require 'tuhura/ingestion'
require 'tuhura/ingestion/file_stream'

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
    },
    avro: {
      skip_lines: 0
    },
    # pb: {
      # skip_lines: 0
    # },
    logging: {}

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
        op.on( '-t', '--task READER', "File containing task description. SHOULD BE FIRST FLAG" ) do |t|
          _process_task_file(t, options)
        end

        additional_config_parameters(op, options)

        # op.separator ""
        # op.separator "Kafka Reader options:"
        # k_opts = OPTS[:kafka]
        # op.on('--offset NUM', "Offset into Kafka queue [#{k_opts[:offset]}]" ) do |n|
          # k_opts[:offset] = n.to_i
        # end
        # #Tuhura::Ingestion::Kafka::KAFKA_OPTS[:topic] = options[:def_topic] if options[:def_topic]
        # op.on('--topic TOPIC', "Name of Kafka queue to read from [#{k_opts[:topic]}]" ) do |s|
          # k_opts[:topic] = s
        # end
        # op.on('--host HOST', "Name of Kafka host [#{k_opts[:host]}]" ) do |h|
          # k_opts[:host] = h
        # end

        op.separator ""
        op.separator "JSON Reader options:"
        j_opts = OPTS[:json]
        op.on('--json-source-uri SOURCE', "Name of file/glob or URI to read json records from" ) do |n|
          j_opts[:source_stream] = Tuhura::Ingestion::FileStream.new(n)
        end
        op.on('--skip-lines NUM', "Number of lines from the input file to skip initially [#{j_opts[:skip_lines]}]" ) do |n|
          j_opts[:skip_lines] = n.to_i
        end

        op.separator ""
        op.separator "AVRO Reader options:"
        av_opts = OPTS[:avro]
        op.on('--avro-file-name AVRO_FILE', "Name of file/glob to read avro records from" ) do |n|
          av_opts[:file_name] = n
          options[:reader] = 'avro'
        end
        op.on('--avro-event-type EVENT_TYPE', "Name of file to read avro records from" ) do |t|
          av_opts[:event_type] = t
        end

        require 'tuhura/ingestion/proto_buf_reader'
        Tuhura::Ingestion::ProtoBufReader.configure_opts(op, options)

        # op.separator ""
        # op.separator "ProtoBuf Reader options:"
        # pb_opts = OPTS[:pb]
        # op.on('--pb-source-uri SOURCE', "Name of file/glob or URI to read protobuf records from" ) do |n|
          # pb_opts[:source_stream] = Tuhura::Ingestion::FileStream.new(n)
          # options[:reader] = 'protobuf'
        # end
        # op.on('--pb-type TYPE', "Protobuffer declaration of type contained in file" ) do |t|
          # pb_opts[:type] = t
        # end

        op.on('--skip-records NUM', "Number of records from the input file to skip initially [#{av_opts[:skip_lines]}]" ) do |n|
          av_opts[:skip_lines] = n.to_i
        end

        require 'tuhura/aws'
        Tuhura::AWS.configure_opts(op)

        require 'tuhura/level_db'
        Tuhura::LevelDB.configure_opts(op)

        # op.separator ""
        # op.separator "Storage options:"
        # op.on('--db-provider PROVIDER', "Provider for database capability [#{Tuhura::Common::Database::DB_OPTS[:provider]}]" ) do |provider|
          # (options[:database] ||= {})[:provider] = provider
        # end
        # op.on('--db-format FORMAT', "Format used for S3 provider [avro]" ) do |format|
          # (options[:database] ||= {})[:format] = format
        # end
        # op.on('--db-noinsert', "Test mode. Do NOT insert data into database [#{Tuhura::Common::Database::DB_OPTS[:no_insert]}]" ) do
          # (options[:database] ||= {})[:no_insert] = true
        # end
        # op.on('-s', '--state-domain DOMAIN', "State domain for keeping ingestion state [???]" ) do |domain|
          # (options[:kafka] ||= {})[:state_domain] =  domain
        # end
        # op.on('-a', '--avro-decl FILE', "File containing AVRO definitions for records" ) do |af|
          # options[:avro_file] = af
        # end

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

        Tuhura::Common::Logger.global_init(options[:logging])
        block.call(op) if block
      end
      self.new(options)
    end

    # Allow sub classes to add additional config parameters
    def self.additional_config_parameters(op, options)
      # nothing
    end

    def work(opts = {}, &block)
      Signal.trap("SIGINT") do
        puts "Terminating..."
        db_close()
        exit
      end

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
            require 'tuhura/ingestion/json_reader'
            self.class.send(:include, Tuhura::Ingestion::JsonReader)
            json_reader_init(opts[:json])
            json_reader_inject(opts[:max_msgs])
          when 'avro'
            require 'tuhura/ingestion/avro_file_reader'
            self.class.send(:include, Tuhura::Ingestion::AvroFileReader)
            avro_file_reader_init(opts[:avro])
            avro_file_reader_inject(opts[:max_msgs])
          when 'protobuf'
            require 'tuhura/ingestion/proto_buf_reader'
            self.class.send(:include, Tuhura::Ingestion::ProtoBufReader)
            pb_reader_init(opts[:protobuf])
            pb_reader_inject(opts[:max_msgs])
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
      #puts opts.inspect
      logger_init()
      oml_init(opts[:oml])
      db_init(opts[:database])
    end

    def self._process_task_file(tfile, options)
      def self._rec_sym_keys(hash)
        h = {}
        hash.each do |k, v|
          if v.is_a? Hash
            v = _rec_sym_keys(v)
          elsif v.is_a? Array
            v = v.map {|e| e.is_a?(Hash) ? _rec_sym_keys(e) : e }
          end
          h[k.to_sym] = v
        end
        h
      end

      cfg_s = YAML.load_file(tfile)
      cfg = _rec_sym_keys(cfg_s)
      unless cfg = cfg[:ingestion]
        raise "Task file needs to start with 'ingestion:'"
      end
      #puts ">>> #{cfg.inspect}"

      unless src_cfg = cfg.delete(:source)
        raise "Task file needs to contain 'source:' description"
      end
      unless src_provider = src_cfg.delete(:provider)
        raise "Task file needs to contain 'provider:' declaration in 'source:' description"
      end
      if stream_cfg = src_cfg.delete(:stream)
        # turn stream def into object
        unless spv = stream_cfg.delete(:provider)
          raise "Missing 'provider' in 'stream' in 'source'"
        end
        case spv
        when 'file'
          src_cfg[:stream] = Tuhura::Ingestion::FileStream.new(stream_cfg)
        when 'gcs'
          require 'tuhura/ingestion/gs_file_stream'
          src_cfg[:stream] = Tuhura::Ingestion::GsFileStream.new(stream_cfg)
        else
          raise "Unknown stream provider 'spv'"
        end
      end
      options[:reader] = src_provider
      options[src_provider.to_sym] = src_cfg

      unless sink_cfg = cfg.delete(:sink)
        raise "Task file needs to contain 'sink:' description"
      end
      unless sink_provider = sink_cfg[:provider]
        raise "Task file needs to contain 'provider:' declaration in 'sink:' description"
      end
      options[:database] = sink_cfg

      options.merge!(cfg)
      puts "Options: #{options.inspect}"
    end


  end # class
end # module






