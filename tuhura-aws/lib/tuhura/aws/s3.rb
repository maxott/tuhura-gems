#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/aws'
require 'aws/dynamo_db'
require 'tuhura/common/logger'

module Tuhura::AWS
  module S3
    DEFAULTS = {
      data_dir: '/tmp',
      writer: 'avro'
    }

    def self.create(opts)
      Connector.new(opts)
    end
  end
end

require 'tuhura/aws/s3/table'

module Tuhura::AWS::S3
  class Connector
    include Tuhura::Common::Logger

    def get_table(table_name, create_if_missing = false, schema = nil, &get_schema)
      Table.get(table_name, create_if_missing, schema, self, @opts, &get_schema)
    end

    def close()
      Table.close_all
    end

    attr_reader :opts

    # Constructor
    #
    # @param [Hash] opts the options to establish a connection to AWS
    def initialize(opts)
      #puts ">>>> #{opts}"
      @opts = Tuhura::AWS::S3::DEFAULTS.merge(opts)
      #logger_init(@opts[:logger], top: false)
      logger_init()
    end

    def no_insert_mode?
      @opts[:no_insert]
    end

    def connector()
      @db
    end
  end
end
