
require 'tuhura/aws'
require 'aws/dynamo_db'
require 'tuhura/common/logger'

module Tuhura::AWS
  module DynamoDB

    def self.create(opts)
      Connector.new(opts)
    end
  end
end

require 'tuhura/aws/dynamo_db/table'

module Tuhura::AWS::DynamoDB
  class Connector
    include Tuhura::Common::Logger

    def get_table(table_name, create_if_missing = false, &get_schema)
      Table.get(table_name, create_if_missing, @db, &get_schema)
    end

    # Constructor
    #
    # @param [Hash] opts the options to establish a connection to AWS
    # @option opts [String] :aws_access_key_id The subject
    # @option opts [String] :aws_secret_access_key From address
    def initialize(opts)
      unless  token = opts[:aws_creds]
        raise "Missing option ':aws_creds'"
      end
      key, secret = token.split(':')
      co = {
        access_key_id: key,
        secret_access_key: secret
      }
      @db = AWS::DynamoDB.new(co)
    end
  end
end
