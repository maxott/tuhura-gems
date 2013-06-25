
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

    USER_SCHEMA = [
      ["user_id", :long],
      # ["ab_100", :string],
      # ["ab_103", :string],
      # ["active_time", :string],
      # ["active_time_ago", :long],
      # ["active_time_epoch", :string],
      ["age", :string],
      # ["aggregation_sources", {"type"=>"array", "items"=>"string"}],
      # ["autodata_time", :string],
      # ["autodata_time_ago", :long],
      # ["autodata_time_epoch", :string],

      #["client_ip_cityhour", Hash],
      # ["client_ip", {
         # "type" => "record",
         # "fields" => [
           # {"name" => "country", "type" => "string"},
           # {"name" => "city", "type" => "string"},
           # {"name" => "lat", "type" => "long"},
           # {"name" => "lon", "type" => "long"}
         # ]
      # }],
      ["client_ip_country", :string],
      ["client_ip_city_name", :string],
      ["client_ip_city_lat", :float],
      ["client_ip_city_lon", :float],

      ["created", :string],
      # ["created_ago", :long],
      ["created_epoch", :string],
      ["email", :string],
      ["email_sub", :string],
      # ["inserted_timestamp", :long],
      ["operator", :string],
      ["referrer", :string],
      ["sim_operator", :string],
      ["user_key", :string],
      # ["ms_user_key", :string],
      ["user_pic", :string],
      # ["user_status_code", :long],
      # ["user_status_count", :long],
      # ["user_status_fix", :boolean],
      # ["user_status_msg", :string],
      # ["user_status_since", :string],
      # ["user_status_since_ago", :long],
      # ["user_status_since_epoch", :string],
      ["timezone", :float],
      ["username", :string],
      ["name", :string]
    ]

    AB_SCHEMA = [
      ["user_id", :long],
      ["value", :string],
    ]

    def process(r, payload)
      unless user_id = r['user_id']
        error "Dropping record because of missing 'user_id' - #{r.inspect}"
      end
      recs = []

      recs << [ 'user', ur = {} ]
      @user_schema_key.each do |k|
        next unless v = r[k]
        ur[k] = v
      end
      # Unfold client_ip_cityhour
      if ch = r['client_ip_cityhour']
        ch.each do |k, v|
          if k != '?' && v.is_a?(Hash)
            # key is city name, hash should be
            ur["client_ip_city_name"] = k
            if latlng = v['latlng']
              lat, lon = latlng.split(',')
              ur["client_ip_city_lat"] = lat.to_f
              ur["client_ip_city_lon"] = lon.to_f
            end
          end
        end
      end

      #Create differnt table entries for each AB test related record
      r.each do |k, v|
        next unless k.start_with? 'ab_'
        recs << [ k, {'user_id' => user_id, 'value' => v.to_json} ]
      end

      recs
    end

    def get_schema_for_table(table_name)
      case table_name
      when 'user'
        return {name: 'user', primary: 'user_id', cols: USER_SCHEMA}
      when /^ab_/
        return {name: table_name, primary: 'user_id', cols: AB_SCHEMA}
      else
        warn "Unknown table '#{table_name}'"
        return {name: table_name}
      end
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
          res
        end
      end
      t
    end

    def initialize(opts)
      super
      @user_schema_key = USER_SCHEMA.map {|k, t| k}
      @table_regex = db_test_mode? ? /^user+_test$/ : /^user+$/
      @r = Random.new
    end

  end
end

if $0 == __FILE__
  options = {task: :inject, max_msgs: -1}
  Tuhura::Ingestion::UserFromKafka.create(options).work(options)
end

