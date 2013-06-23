
require 'json'
#require 'tuhura/common/sensation'
require 'tuhura/ingestion/abstract_kafka_bridge2'
require 'active_support/core_ext'

module Tuhura::Ingestion
  Tuhura::Ingestion::AbstractKafkaBridge2::KAFKA_OPTS[:topic] = 'user'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'user_from_kafka'

  # Read user information from Kafka queues
  #
  class UserFromKafka < AbstractKafkaBridge2
    #include Tuhura::Common::Sensation

    def process(r, payload)
      puts r.inspect
      puts payload.inspect
      exit

      r.delete("user_key")
      r.delete("access_token_hash")

      user_id = r['user_id']
      timestamp = r["timestamp"] / 1000
      ts_day = (timestamp / 86400).to_i
      ts_week = (ts_day / 7).to_i
      ts_month = (ts_day / 24).to_i

      evt_type = r.delete("event_type")
      range = "#{user_id}-#{r["timestamp"]}-#{@r.rand(10**3)}"
      [["sen_#{evt_type}_w#{ts_week}", {'day' => ts_day, 'range' => range}, r]]
    end

    # def get_table_for_group(group_name)
      # get_table("s#{group_name}")
    # end

    def get_schema_for_table(table_name)
      schema_name = table_name.split('_')[0 ... -1].join('_')
      unless fields = @avro[schema_name]
        warn "Unknown sensation ID '#{table_name}'"
        return nil
      end
      schema = [
        ['day', :integer], ['range', :string],

        ["sensation_id", :long],

        ["client_ip", :string],

        ["version", :string],
        #["timestamp", :long],
        ["device", :string, '???'],
        ["tz", :int],

        ["session_time", :long],
        ["session_start", :long],
        ["ingestion_since_ts", :long]
      ]

      #puts "FIELDS: #{fields}"
      fields.each do |f|
        type = f['type'].is_a?(Hash) ? f['type'] : f['type'].to_sym
        schema << [f['name'].underscore, type]
      end
      schema
    end

    def get_table_for_group(group_name)
      t = super
      if t.respond_to? :before_dropping
        t.before_dropping do |r, schema, tries|
          if tries > 1
            warn "Didn't seem to fix record for '#{group_name}' - #{r}"
            next nil
          end

          res = nil
          # case group_name
          # when /^sen_1_/, /^sen_9_/
            # unless (video_ids = r['video_ids']).is_a?(Array)
              # r['video_ids'] = [video_ids]
              # res = r
            # end
          # when /^sen_29_/
            # index = ["connected", "connecting", "disconnected", "disconnecting", "suspended"].index(r['state'].downcase)
            # unless index.nil?
              # r['state'] = index
              # res = r
            # end
#
          # when /^sen_24_/
            # index = ["gps", "network"].index(r['provider'].downcase)
            # unless index.nil?
              # r['provider'] = index
              # res = r
            # end
#
          # end
          #puts "--- #{group_name} -- #{res}"
          res
        end
      end
      t
    end


    def initialize(opts)
      super

      # unless af = opts.delete(:avro_file)
        # raise "Missing AVRO mapping file"
      # end
      # @avro = {}
      # avro = JSON.load(File.open(af))
      # avro.each do |r|
        # name = r['name']
        # _add_avro_declaration(r['name'], r)
        # (r['aliases'] || []).each {|a| _add_avro_declaration(a, r)}
      # end

      #sensation_init
      @table_regex = db_test_mode? ? /^user+_test$/ : /^user+$/
      @r = Random.new
    end

  end
end

if $0 == __FILE__
  options = {task: :inject, max_msgs: -1}
  Tuhura::Ingestion::UserFromKafka.create(options).work(options)
end

