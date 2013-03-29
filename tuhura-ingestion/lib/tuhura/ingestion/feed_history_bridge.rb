
require 'json'
load 'abstract_kafka_bridge.rb'

AbstractKafkaBridge::KAFKA_OPTS[:topic] = 'feedhistory0'
AbstractKafkaBridge::OML_OPTS[:appName] = 'feedhistory_bridge'

class FeedHistoryBridge < AbstractKafkaBridge
  def process(r, payload)
    user_id = r['user_id']
    served = r['served_epoch'].to_i
    ts_day = (served / 86400).to_i
    r['videos'].map do |v|
      v['user_id'] = user_id
      v['served'] = served
      crc32 = Zlib::crc32()
      key = create_key(ts_day, user_id, payload + v['video_id'])
      ['default', key, {'f:uid' => user_id, 'f:msg' => v.to_json}]
    end
  end

  def get_table_for_group(group_name)
    get_table("feed_history")
  end

  def initialize(zk_opts, kafka_opts, hbase_opts, opts)
    super
    @table_regex = HBASE_OPTS[:test_mode] ? /^feed_history_test$/ : /^feed_history+$/
  end
end


options = {task: :inject, max_msgs: -1}
FeedHistoryBridge.create(options).work(options)
