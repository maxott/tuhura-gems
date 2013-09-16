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
  OPTS[:kafka][:topic] = 'identity'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'identity_ingestion'

  # Read identity information from Kafka queues
  class IdentityIngestion < AbstractIngestion
    IDENTITY_SCHEMA = [
                       ["user_id", :long],
                       ["network_id", :int],
                       ["identity_pic", :string],
                       ["uri", :string],
                       ["identity_name", :string],
                       ["identity_status", :string],
                       ["identity_id", :long],
                       ["identity_ref", :string],
                       ["identity_status_code", :int],
                       ["permissions", :string]
                      ]

    def ingest_message(r)
      #puts ">>>> #{r['created']} -- #{r['created_epoch']} -- #{r.keys}"
      unless user_id = r['user_id'] and identiy_id = r['identity_id']
        error "Dropping record because of missing data - #{r.inspect}"
      end
      user_id = r['user_id'] = r['user_id'].to_i


      recs = []

      recs << [ 'identity', ur = {} ]
      @identity_schema_key.each do |k|
        next unless v = r[k]
        ur[k] = v
      end

      recs
    end

    def get_schema_for_table(table_name)
      case table_name
      when /^identity/
        return {name: 'identity', primary: 'identity_id', cols: IDENTITY_SCHEMA, version: 1}
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
      @identity_schema_key = IDENTITY_SCHEMA.map {|k, t| k}
      @table_regex = db_test_mode? ? /^identity+_test$/ : /^identity+$/
      @r = Random.new
    end

  end
end

if $0 == __FILE__
  options = {task: :inject, max_msgs: -1, def_topic: 'identity'}
  Tuhura::Ingestion::IdentityIngestion.create(options).work(options)
end

