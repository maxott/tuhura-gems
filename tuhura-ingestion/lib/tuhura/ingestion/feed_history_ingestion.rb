#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'json'
#require 'tuhura/common/sensation'
require 'tuhura/ingestion/abstract_ingestion'

module Tuhura::Ingestion
  OPTS[:kafka][:topic] = 'feedhistory0'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'feedhistory_ingestion'

  class FeedHistoryIngestion < AbstractIngestion

    RECOMMENDATION_SCHEMA = [
      ["recommendation_id", :string],
      ['retrieved_epoch', :long],
      ["source_name", :string],
      ["aggregation_key", :string],
      ["channel", :string],
      ["discovered_epoch", :long],
      ["updated", :string],
      ["updated_epoch", :long],
      ["updated_ago", :long]
    ]

    VIDEO_SCHEMA = [
      ["video_id", :string],
      ['retrieved_epoch', :long],
      ["title", :string],
      ["prefetchable", :boolean],
      ["author", :string],
      ["author_name", :string],
      ["author_pic", :string],
      ["duration", :integer],
      ["plays", :integer],
      ["comments", :integer],
      ["url_stream", :string],
      ["channel", :string],
      ["description", :string],
      ["dislikes", :integer],
      ["likes", :integer],
      ["control_syndicate", :string],
      ["published", :string],
      ["stem_scores", :string],
      ["stemtags", :string],
      ["stems", :string],
      ["stem_scores_version", :integer],
      ["stems_scored", :integer],
      ["stems_unscored", :integer]
    ]

    FEED_EVENT_SCHEMA = [
      ['day', :long],
      ['user_id', :string],
      ['video_id', :string] ,
      ['served_epoch', :long]
    ]
    #include Tuhura::Common::Sensation

    def ingest_message(r)
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
            rec['retrieved_epoch'] = served
            rec['discovered_epoch'] = rec['discovered_epoch'].to_i
            rec['updated_epoch'] = rec['updated_epoch'].to_i
            res << [t_name, rec]
          end
        end

        if v['stemtags']
          v['stemtags'] = v['stemtags'].map { |k, v| "#{k}:#{v.join(',')}" }
          # TODO: Should keep as array
          v['stemtags'] = v['stemtags'].join('|')
        end
        v.delete('thumbnails') # causing problems
        v['retrieved_epoch'] = served
        res << ["video_w#{ts_week}", v]

        #puts '----'
        #puts v.map { |k,v| "#{k}(#{v.class}): #{v.inspect}" }


        event = {day: ts_day, served_epoch: served, user_id: user_id, video_id: video_id}
        res << ["feed_event_w#{ts_week}", event]
      end
      res
    end


    def get_schema_for_table(table_name)
      case table_name
      when /^recommendation_/
        return {name: 'recommendation', primary: 'recommendation_id', range: 'updated_epoch',
          cols: RECOMMENDATION_SCHEMA, version: 1}
      when /^video_/
        return {name: 'video', primary: 'video_id', range: 'retrieved_epoch',
          cols: VIDEO_SCHEMA, version: 1}
      when /^feed_event_/
        return {name: 'feed_event', primary: 'day', range: 'user_id',
          cols: FEED_EVENT_SCHEMA, version: 1}

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
  Tuhura::Ingestion::FeedHistoryIngestion.create(options).work(options)
end
