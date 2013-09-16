#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'json'
#require 'tuhura/common/sensation'
require 'tuhura/ingestion/abstract_kafka_bridge2'
require 'active_support/core_ext'

module Tuhura::Ingestion
  Tuhura::Ingestion::AbstractKafkaBridge2::KAFKA_OPTS[:topic] = 'sensation0'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'sensation_from_kafka'

  class SensationFromKafka < AbstractKafkaBridge2
    #include Tuhura::Common::Sensation

    def process(r, payload)
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
        return {name: schema_name}
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
      {name: schema_name, primary: 'day', range: 'range', cols: schema}
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
          case group_name
          when /^sen_1_/, /^sen_9_/
            unless (video_ids = r['video_ids']).is_a?(Array)
              r['video_ids'] = [video_ids]
              res = r
            end
          when /^sen_29_/
            index = ["connected", "connecting", "disconnected", "disconnecting", "suspended"].index(r['state'].downcase)
            unless index.nil?
              r['state'] = index
              res = r
            end

          when /^sen_24_/
            index = ["gps", "network"].index(r['provider'].downcase)
            unless index.nil?
              r['provider'] = index
              res = r
            end

          end
          #puts "--- #{group_name} -- #{res}"
          res
        end
      end
      t
    end


    def initialize(opts)
      super

      unless af = opts.delete(:avro_file)
        raise "Missing AVRO mapping file"
      end
      @avro = {}
      avro = JSON.load(File.open(af))
      avro.each do |r|
        name = r['name']
        _add_avro_declaration(r['name'], r)
        (r['aliases'] || []).each {|a| _add_avro_declaration(a, r)}
      end

      #sensation_init
      @table_regex = db_test_mode? ? /^sen[0-9]+_test$/ : /^sen[0-9]+$/
      @r = Random.new
    end

    def _add_avro_declaration(name, declaration)
      return unless name.start_with? 'sen_'
      #puts declaration['fields'].inspect
      if @avro.key? name
        warn "Duplicate type declaration '#{name}'"
      else
        @avro[name] = declaration['fields']
      end
    end
  end
end

if $0 == __FILE__
  options = {task: :inject, max_msgs: -1}
  Tuhura::Ingestion::SensationFromKafka.create(options).work(options)
end

