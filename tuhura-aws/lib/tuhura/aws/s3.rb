
require 'tuhura/aws'
require 'aws/dynamo_db'
require 'tuhura/common/logger'

module Tuhura::AWS
  module S3
    DEFAULTS = {
      data_dir: '/tmp'
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
      Table.get(table_name, create_if_missing, schema, self, &get_schema)
    end

    def self.close_all_table()
      Table.close_all
    end

    attr_reader :opts

    # Constructor
    #
    # @param [Hash] opts the options to establish a connection to AWS
    def initialize(opts)
      @opts = Tuhura::AWS::S3::DEFAULTS.merge(opts)
      logger_init(@opts[:logger], top: false)
    end

    def no_insert_mode?
      @opts[:no_insert]
    end

    def connector()
      @db
    end
  end
end