
module Tuhura::Common

  # Provide a simple service to persist configuration
  # and state information.
  #
  module State

    STATE_OPTS = {
      provider: 'dynamo_db',
    }

    class StateException < Exception; end

    class NonExistingPathException < StateException
      attr_reader :missing_path

      def initialize(missing_path)
        @missing_path = missing_path
      end
    end

    attr_reader :state_opts

    # Return the value stored at 'path'. If it doesn't exist, return 'def_value'
    #
    def state_get(path, def_value = nil)
      @handle.get(path, def_value)
    end

    def state_put(path, value, create_if_not_exist = true)
      @handle.put(path, value, create_if_not_exist)
    end

    def state_delete(path)
      @handle.delete(path)
    end

    # Return the list of children for 'path'. Returns nil if
    # path does not exist.
    #
    def state_children(path)
      @handle.children(path)
    end

    # Check if path exists. If 'create_if_not_exist' is set to
    # true, create the path and return true.
    #
    def state_path_exists?(path, create_if_not_exist = false)
      @handle.path_exists?(path, create_if_not_exist)
    end

    def state_init(opts = {})
      @state_opts = opts = STATE_OPTS.merge(opts || {})

      case provider = (opts[:provider] || :unknown).to_s.downcase
      when 'dynamo_db'
        require 'tuhura/aws/dynamo_db/state_provider'
        @handle = Tuhura::AWS::DynamoDB::StateProvider.create(self, opts)
      else
        raise "Unknown state provider '#{provider}'"
      end

    end

  end
end