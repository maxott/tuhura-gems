
require 'json'
#require 'tuhura/common/sensation'
require 'tuhura/ingestion/abstract_kafka_bridge2'

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
      [["sen_#{evt_type}_w#{ts_week}", {day: ts_day, range: range}, r]]
    end

    # def get_table_for_group(group_name)
      # get_table("s#{group_name}")
    # end

    def get_schema_for_table(table_name)
      [[:day, :integer], [:range, :string]]
    end

    def initialize(opts)
      super
      #sensation_init
      @table_regex = db_test_mode? ? /^sen[0-9]+_test$/ : /^sen[0-9]+$/
      @r = Random.new
    end
  end
end

if $0 == __FILE__
  options = {task: :inject, max_msgs: -1}
  Tuhura::Ingestion::SensationFromKafka.create(options).work(options)
end

