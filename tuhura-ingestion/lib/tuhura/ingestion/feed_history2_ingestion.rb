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
  Tuhura::Common::OML::OML_OPTS[:appName] = 'feedhistory2_ingestion'

  class FeedHistory2Ingestion < AbstractIngestion

    RECOMMENDATION_SCHEMA = [
      ["recommendation_id", :string],
      ["video_id", :string],
      ["author", :string],
      ["source_name", :string],
      ["aggregation_key", :string],
      ["comments", :string],
      ["likes", :int],
      ["published_epoch", :long],
      ["discovered_epoch", :long]
    ]

    VIDEO_SCHEMA = [
      ["video_id", :string],
      ["plays", :integer],
      ["dislikes", :integer],
      ["likes", :integer],
      ["updated_epoch", :long]
    ]

    FEED_EVENT_SCHEMA = [
      ['day', :long],
      ['user_id', :string],
      ['video_id', :string] ,
      ['served_epoch', :long]
    ]
    #include Tuhura::Common::Sensation

    DEF_RECOMMENDATION = {
      author: '',
      source_name: '',
      aggregation_key: '',
      comments: '',
      likes: -1,
      published_epoch: -1,
      discovered_epoch: -1
    }

    DEF_VIDEO = {
      plays: -1,
      dislikes: -1,
      likes: -1,
      updated_epoch: -1
    }

    def ingest_message(r)
      user_id = r[:user_id]
      served = r[:served_epoch].to_i
      ts_day = (served / 86400).to_i
      ts_week = (ts_day / 7).to_i
      ts_month = (ts_day / 24).to_i
      res = []
      r.delete(:videos).each do |v|
        unless video_id = v[:video_id]
          warn "Missing 'video_id' in record '#{v}'"
          next
        end

        if recs = v.delete(:recommendations)
          t_name = "recommendation_w#{ts_week}"
          recs.each do |rec|
            rec[:video_id] = video_id
            #rec['retrieved_epoch'] = served
            #rec['discovered_epoch'] = rec['discovered_epoch'].to_i
            #rec['updated_epoch'] = rec['updated_epoch'].to_i
            res << [t_name, DEF_RECOMMENDATION.merge(rec)]
          end
        end

        #v[:retrieved_epoch] = served
        res << ["video_w#{ts_week}", DEF_VIDEO.merge(v)]
        res << ["feed_event_w#{ts_week}", day: ts_day, user_id: user_id, video_id: video_id, served_epoch: served]
      end
      res
    end


    def get_schema_for_table(table_name)
      case table_name
      when /^recommendation_/
        return {name: 'recommendation', primary: :recommendation_id, range: :updated_epoch,
          cols: RECOMMENDATION_SCHEMA, version: 1}
      when /^video_/
        return {name: 'video', primary: :video_id, range: :retrieved_epoch,
          cols: VIDEO_SCHEMA, version: 1}
      when /^feed_event_/
        return {name: 'feed_event', primary: :day, range: :user_id,
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
  Tuhura::Ingestion::FeedHistory2Ingestion.create(options).work(options)
end
