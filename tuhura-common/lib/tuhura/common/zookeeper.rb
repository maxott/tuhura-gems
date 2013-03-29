require 'set'
require 'zookeeper'


module Tuhura::Common
  module Zookeeper
    
    ZOOKEEPER_OPTS = {
      url: 'zk.incmg.net'
    }

    class NonExistingPathException < Exception
      attr_reader :missing_path
      
      def initialize(missing_path)
        @missing_path = missing_path
      end
    end
    
    attr_reader :zk_opts
    
    def zk_put(path, value, create_if_not_exist = true)
      unless @zk_validated_paths.include? path
        # validate that it exists
        unless zk_path_exists?(path, create_if_not_exist)
          raise NonExistingPathException.new(path)
        end
      end
      zk_call do
        @zk.set path: path, data: value
      end
    end
    
    def zk_get(path, def_value = nil)
      r = nil
      zk_call do
        r = @zk.get(path: path)
      end
      @logger.debug "ZK: Read '#{path}' => '#{r}'"
      r[:stat].exists? ? r[:data] : def_value
    end
    
    # Check if path exists. If 'create_if_not_exist' is set to
    # true, create the path and return true.
    #
    def zk_path_exists?(path, create_if_not_exist = false)
      return true if path.empty?
      
      
      if @zk.get(path: path)[:stat].exists?
        @zk_validated_paths << path        
        return true
      end
      return false unless create_if_not_exist

      p = path.split('/')[0 ... -1].join('/')
      zk_path_exists?(p, true)
      @zk.create(path: path)
      @zk_validated_paths << path
      true
    end
    
    def zk_init(opts = {})
      @zk_opts = ZOOKEEPER_OPTS.merge(opts)
      @zk = ::Zookeeper.new(zk_opts[:url])
      @zk_validated_paths = Set.new
    end
    
    # Use this methods for any operation on @zk. It will retry 
    # the operation multiple times before giving up
    #
    def zk_call(retries = 3, &block)
      loop do
        begin
          block.call
        rescue Zookeeper::Exceptions::NotConnected => ncex
          if (tries -= 1) >= 0
            @@logger.warn "Lost connection to zookeeper. Will try again."
            sleep 10
          else
            raise ncex
          end            
        end
      end
    end
  end
end