
require 'json'
require 'tuhura/ingestion/abstract_ingestion'
require 'active_support/core_ext'

module Tuhura::Ingestion
  Tuhura::Ingestion::Kafka::KAFKA_OPTS[:topic] = 'sensation0'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'sensation_from_kafka'

  class SensationIngestion < AbstractIngestion

    def ingest_kafka_message(r, payload)
      r.delete("user_key")
      r.delete("access_token_hash")

      user_id = r['user_id']
      timestamp = r["timestamp"] / 1000
      r['day'] = ts_day = (timestamp / 86400).to_i
      ts_week = (ts_day / 7).to_i
      ts_month = (ts_day / 24).to_i

      evt_type = r.delete("event_type")
      r['range'] = "#{user_id}-#{r["timestamp"]}-#{@r.rand(10**3)}"

      [["sen_#{evt_type}_w#{ts_week}", r]]
    end

    def get_schema_for_table(table_name)
      schema_name = table_name.split('_')[0, 2].join('_')
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

          res = r
          case group_name
          when /^sen_1_/, /^sen_9_/
            unless (video_ids = r['video_ids']).is_a?(Array)
              r['video_ids'] = [video_ids]
            end
          when /^sen_29_/
            index = ["connected", "connecting", "disconnected", "disconnecting", "suspended"].index(r['state'].downcase)
            unless index.nil?
              r['state'] = index
            end

          when /^sen_24_/
            index = ["gps", "network"].index(r['provider'].downcase)
            unless index.nil?
              r['provider'] = index
            end

          when /^sen_40_/
            r['ts_download_complete'] = r['ts_download_complete'].to_i
            r['data'] = r['data'].to_f
            r['success'] = r['success'].to_i
            r['space_available'] = r['space_available'].to_i
            r['space_total'] = r['space_total'].to_i

          else
            res = nil
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
  Tuhura::Ingestion::SensationIngestion.create(options).work(options)
end

