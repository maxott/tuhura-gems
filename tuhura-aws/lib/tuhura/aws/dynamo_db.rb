
require 'tuhura/aws'
require 'aws/dynamo_db'
require 'tuhura/common/logger'

module Tuhura::AWS
  module DynamoDB
    @@connector = nil # singleton

    def self.create(opts)
      # Not thread safe
      @@connector ||= Connector.new(opts)
    end
  end
end

require 'tuhura/aws/dynamo_db/table'

module Tuhura::AWS::DynamoDB
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
    # @option opts [String] :aws_access_key_id The subject
    # @option opts [String] :aws_secret_access_key From address
    def initialize(opts)
      logger_init(nil, top: false)
      @opts = opts
puts "DB: #{opts}"
#raise "EXIT"
      # unless  token = opts[:aws_creds]
        # raise "Missing option ':aws_creds'"
      # end
      # key, secret = token.split(':')
      # co = {
        # access_key_id: key,
        # secret_access_key: secret
      # }.merge(opts[:dynamo_db] || {})
      # info "Connecting to DynamoDB with '#{co}'"
      # @db = AWS::DynamoDB.new(co)
      @db = AWS::DynamoDB.new()
    end

    def no_insert_mode?
      @opts[:no_insert]
    end

    def connector()
      @db
    end
  end
end
