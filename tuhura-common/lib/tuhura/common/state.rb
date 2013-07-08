require 'tuhura/common'

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
      @state_provider.get(path, def_value)
    end

    def state_put(path, value, create_if_not_exist = true)
      @state_provider.put(path, value, create_if_not_exist)
    end

    def state_delete(path)
      @state_provider.delete(path)
    end

    # Return the list of children for 'path'. Returns nil if
    # path does not exist.
    #
    def state_children(path)
      @state_provider.children(path)
    end

    # Check if path exists. If 'create_if_not_exist' is set to
    # true, create the path and return true.
    #
    def state_path_exists?(path, create_if_not_exist = false)
      @state_provider.path_exists?(path, create_if_not_exist)
    end

    def state_init(opts = {})
      @state_opts = opts = STATE_OPTS.merge(opts || {})

      case provider = (opts[:provider] || :unknown).to_s.downcase
      when 'dynamo_db'
        require 'tuhura/aws/dynamo_db/state_provider'
        @state_provider = Tuhura::AWS::DynamoDB::StateProvider.create(self, opts)
      else
        raise "Unknown state provider '#{provider}'"
      end

    end

  end
end

if __FILE__ == $0

  require 'tuhura/common/logger'
  require 'tuhura/common/oml'
  require 'tuhura/common/database'

  class StateTester
    include Tuhura::Common::Logger
    include Tuhura::Common::OML
    include Tuhura::Common::State

    def initialize(opts = {})
      puts opts.inspect
      logger_init()
      oml_init()
      state_init()
    end
  end

  st = StateTester.new
  puts st.state_get('/incmg/kafka_bridge/feedhistory7/offset')

end