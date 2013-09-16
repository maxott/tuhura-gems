#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/level_db'
require 'tuhura/common/logger'
require 'json'

module Tuhura::LevelDB
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
    end

    def initialize(consumer, opts = {})
      logger_init(nil, top: false)
      schema = {primary: 'key', cols: [['key', :string], ['value', :string]]}
      @table = Tuhura::LevelDB::create(opts).get_table('__tuhura_state__', true, )
    end

  end
end
