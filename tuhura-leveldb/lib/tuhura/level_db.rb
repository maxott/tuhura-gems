#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/common/logger'
require 'tuhura/level_db/table'

module Tuhura
  module LevelDB
    @@connector = nil # singleton

    def self.create(opts)
      Connector.new(opts)
    end

    CONFIG_OPTS = {
      db_dir: '/tmp'
    }

    def self.configure_opts(op)
      op.separator ""
      op.separator "LevelDB options:"
      op.on('--ldb-dir DIRECTORY', "Directory to store Level database files [#{CONFIG_OPTS[:db_dir]}]" ) do |dir|
        CONFIG_OPTS[:db_dir] = dir
      end
    end

    def self.init(opts = {})
      CONFIG_OPTS.merge!(opts) {|k, v1, v2| v1 || v2 }
    end


    class Connector
      include Tuhura::Common::Logger

      def get_table(table_name, create_if_missing = false, schema = nil, &get_schema)
        Table.get(table_name, create_if_missing, schema, self, &get_schema)
      end

      def close()
        Table.close_all
      end

      attr_reader :opts

      # Constructor
      #
      # @param [Hash] opts the options to establish a connection to AWS
      def initialize(opts)
        @opts = opts
        #logger_init(@opts[:logger], top: false)
        logger_init()
      end

      def no_insert_mode?
        @opts[:no_insert]
      end
    end
  end
end
