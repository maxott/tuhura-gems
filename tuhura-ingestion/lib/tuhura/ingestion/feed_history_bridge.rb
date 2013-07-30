
require 'json'
#require 'tuhura/common/sensation'
require 'tuhura/ingestion/abstract_kafka_bridge2'

module Tuhura::Ingestion
  Tuhura::Ingestion::AbstractKafkaBridge2::KAFKA_OPTS[:topic] = 'feedhistory0'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'feedhistory_from_kafka'

  class FeedHistoryBridge < AbstractKafkaBridge2
    #include Tuhura::Common::Sensation

    def process(r, payload)
      user_id = r['user_id']
      served = r['served_epoch'].to_i
      ts_day = (served / 86400).to_i
      ts_week = (ts_day / 7).to_i
      ts_month = (ts_day / 24).to_i
      res = []
      r['videos'].map do |v|
        unless video_id = v['video_id']
          warn "Missing 'video_id' in record '#{v}'"
          next
        end

        if recs = v.delete('recommendations')
          t_name = "recommendation_w#{ts_week}"
          recs.each do |rec|
            id = rec['recommendation_id']
            res << [t_name, {recommendation_id: id}, rec]
          end
        end

        if v['stemtags']
          v['stemtags'] = v['stemtags'].map { |k, v| "#{k}:#{v.join(',')}" }
        end
        v.delete('thumbnails') # causing problems
        res << ["video_m#{ts_month}", {video_id: video_id}, v]

        puts '----'
        puts v.map { |k,v| "#{k}(#{v.class}): #{v.inspect}" }


        event = {served: served, user_id: user_id, video_id: video_id}
        res << ["feed_w#{ts_week}", {day: ts_day, video_key: "#{video_id}_#{user_id}_#{served}"}, event]
      end
      res
    end


    def get_schema_for_table(table_name)
      case table_name
      when /^recommendation_w/
        [[:recommendation_id, :string]]
      when /^video_m/
        [[:video_id, :string]]
      when /^feed_w/
        [[:day, :integer], [:video_key, :string]]
      else
        raise "Unknown table '#{table_name}'"
      end
    end

    def initialize(opts)
      super
      @table_regex = db_test_mode? ? /^feed_history_test$/ : /^feed_history+$/
      @r = Random.new
    end

    private
  end
end


if $0 == __FILE__
  options = {
    task: :inject,
    max_msgs: -1

  }
  Tuhura::Ingestion::FeedHistoryBridge.create(options).work(options)
end
