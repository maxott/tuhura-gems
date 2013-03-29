
require 'json'
require 'tuhura/common/sensation'
require 'tuhura/ingestion/abstract_kafka_bridge'

module Tuhura::Ingestion
  KAFKA_OPTS[:topic] = 'sensation0'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'sensation_from_kafka'
  
  class SensationFromKafka < AbstractKafkaBridge
    include Tuhura::Common::Sensation

    def process(r, payload)
      r.delete("user_key")
      r.delete("access_token_hash")
      evt_type = r.delete("event_type")
      user_id = r["user_id"]
      timestamp = r["timestamp"] / 1000
      key = sensation_create_key(timestamp, user_id, payload)
      cg = {'f:raw_msg' => payload}
      r.each do |k, v|
        cg["f:#{k}"] = v
      end
      [["sen#{evt_type}", key, cg]]
    end
  
    # def get_table_for_group(group_name)
      # get_table("s#{group_name}")
    # end
  
    def initialize(opts)
      super
      sensation_init
      @table_regex = hbase_test_mode? ? /^sensation[0-9]+_test$/ : /^sensation[0-9]+$/
    end
  end
end

