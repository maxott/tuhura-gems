##
# This file is auto-generated. DO NOT EDIT!
#
require 'protobuf/message'

module Tuhura
  module Ingestion

    ##
    # Message Classes
    #
    class FeedHistory < ::Protobuf::Message
      class Recommendation < ::Protobuf::Message; end
      class Video < ::Protobuf::Message; end

    end

    class FeedHistoryEnvelope < ::Protobuf::Message; end


    ##
    # Message Fields
    #
    class FeedHistory
      class Recommendation
        optional ::Protobuf::Field::StringField, :recommendation_id, 1
        optional ::Protobuf::Field::StringField, :author, 2
        optional ::Protobuf::Field::StringField, :source_name, 3
        optional ::Protobuf::Field::StringField, :aggregation_key, 4
        optional ::Protobuf::Field::Uint32Field, :comments, 5
        optional ::Protobuf::Field::Uint32Field, :likes, 6
        optional ::Protobuf::Field::Uint64Field, :published_epoch, 7
      end

      class Video
        required ::Protobuf::Field::StringField, :video_id, 1
        optional ::Protobuf::Field::Uint32Field, :plays, 2
        optional ::Protobuf::Field::Uint32Field, :dislikes, 3
        optional ::Protobuf::Field::Uint32Field, :likes, 4
        repeated ::Tuhura::Ingestion::FeedHistory::Recommendation, :recommendations, 5
      end

      required ::Protobuf::Field::Uint64Field, :feedhistory_id, 1
      required ::Protobuf::Field::Uint64Field, :user_id, 2
      optional ::Protobuf::Field::Uint64Field, :served_epoch, 3
      optional ::Protobuf::Field::Uint64Field, :retrieved_epoch, 4
      repeated ::Tuhura::Ingestion::FeedHistory::Video, :videos, 5
    end

    class FeedHistoryEnvelope
      required ::Tuhura::Ingestion::FeedHistory, :msg, 30
    end

  end

end

