
require 'tuhura/common/logger'

module Tuhura
  module LevelDB
    @@connector = nil # singleton

    def self.create(opts)
      require 'tuhura/leveldb/table'
      Table.create(opts)
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

  end
end

