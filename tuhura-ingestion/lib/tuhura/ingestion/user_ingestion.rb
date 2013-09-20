#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'json'
require 'tuhura/ingestion/abstract_ingestion'
require 'active_support/core_ext'

module Tuhura::Ingestion
  OPTS[:kafka][:topic] = 'user'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'user_ingestion'

  # Read user information from Kafka queues
  #
  class UserIngestion < AbstractIngestion
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
      ["created_epoch", :long],
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

    def ingest_message(r)
      #puts ">>>> #{r['created']} -- #{r['created_epoch']} -- #{r.keys}"
      unless user_id = r['user_id']
        error "Dropping record because of missing 'user_id' - #{r.inspect}"
      end
      user_id = r['user_id'] = r['user_id'].to_i
      r['created_epoch'] = (r['created_epoch'] || 0).to_i

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
      when /^user/
        return {name: 'user', primary: 'user_id', range: 'created_epoch', cols: USER_SCHEMA, version: 1}
      when /^ab_/
        return {name: table_name, primary: 'user_id', cols: AB_SCHEMA, version: 1}
      end
      raise "Unknown table '#{table_name}'"
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
  options = {task: :inject, max_msgs: -1, def_topic: 'user'}
  Tuhura::Ingestion::UserIngestion.create(options).work(options)
end
