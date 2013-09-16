#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/aws'
require 'tuhura/aws/dynamo_db'
require 'tuhura/common/logger'
require 'json'

module Tuhura::AWS::DynamoDB
  class StateProvider
    include Tuhura::Common::Logger

    def self.create(consumer, opts)
      self.new(consumer, opts)
    end

    # class StateException < Exception; end
#
    # class NonExistingPathException < StateException

    attr_reader :state_opts

    def put(path, value, create_if_not_exist)
      path = path.gsub '//', '/'
      value_s = value.to_json
      info "PUT #{path}: #{value_s}\n"
      @table.put([{key: path, value: value_s}])
      value
    end

    def get(path, def_value = nil)
      path = path.gsub '//', '/'
      row = @table.get(path)
      debug "get: #{[path]}: #{row}"
      if v = row['value']
        JSON.parse("[#{v}]")[0] # can't parse string or numbers directly
      else
        def_value
      end
    end

    def delete(path)
      # zk_call do
        # r = @zk.delete(path: path)
      # end
#
    end

    # Return the list of children for 'path'. Returns nil if
    # path does not exist.
    #
    def children(path)
      # r = nil
      # zk_call do
        # r = @zk.get_children(path: path)
      # end
      # debug "ZK: Children '#{path}' => '#{r}'"
      # raise NonExistingPathException.new(path) unless r[:stat].exists?
      # r[:children]
    end

    # Check if path exists. If 'create_if_not_exist' is set to
    # true, create the path and return true.
    #
    def path_exists?(path, create_if_not_exist = false)
      return true if path.empty?


      # if @zk.get(path: path)[:stat].exists?
        # @zk_validated_paths << path
        # return true
      # end
      # return false unless create_if_not_exist
#
      # p = path.split('/')[0 ... -1].join('/')
      # zk_path_exists?(p, true)
      # @zk.create(path: path)
      # @zk_validated_paths << path
      # true
    end

    def initialize(consumer, opts = {})
      logger_init(nil, top: false)
      schema = {primary: 'key', cols: [['key', :string], ['value', :string]]}
      @table = Tuhura::AWS::DynamoDB::create(opts).get_table('__tuhura_state__', true, )
    end

    # Use this methods for any operation on @table. It will retry
    # the operation multiple times before giving up
    #
    def _call(retries = 100, &block)
      i = 0
      loop do
        begin
          return block.call
        rescue ::Zookeeper::Exceptions::NotConnected => ncex
          if (i += 1) <= retries
            # warn "Lost connection to zookeeper. Will try again."
            # sleep 10 * i
            @zk.reopen
          else
            raise ncex
          end
        end
      end
    end
  end
end
