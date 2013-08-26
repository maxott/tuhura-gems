
require 'tuhura/common'
require 'monitor'

#org.apache.log4j.BasicConfigurator.configure();



module Tuhura::Common
  module Logger
    LOGGER_OPTS = {
      provider: 'builtin',
      tracing: true,
      level: :debug,
      log4j_config: 'log4j.properties'
    }

    #@@lock = Mutex.new
    @@logger_factory = nil

    def self.global_init(opts = {})
      opts = LOGGER_OPTS.merge(opts)
      case provider = opts[:provider].to_s.downcase
      when 'log4j'
        _global_init_log4j(opts)
      when 'builtin'
        _global_init_builtin(opts)
      else
        raise "Unknown logging provider '#{provider}'"
      end
    end

    def self._global_init_log4j(opts)
      require 'java'
      require 'log4j'
      if log4j_config = opts[:log4j_config]
        Logger.configure_log4j_from_file(log4j_config)
      end

      require 'log4jruby'
      self.class.enable_logger

      @@logger_factory = lambda {|name| Log4jruby::Logger.get(name, opts)}
    end

    def self._global_init_builtin(opts)
      require 'logging'

      # Use global default logger from logging gem
      #self.class.include Logging.globally
      Logging.appenders.stdout(
        'default_stdout',
        :layout => Logging.layouts.pattern(:date_pattern => '%F %T %z',
                                           :pattern => '[%d] %-5l %c: %m\n',
                                           :color_scheme => 'default'))
      Logging.logger.root.appenders = 'default_stdout'
      Logging.logger.root.level = :info
      @@logger_factory = lambda {|name| Logging::Logger.new(name)}
    end

    def self.configure_log4j_from_file(file_name)
      org.apache.log4j.PropertyConfigurator.configure(file_name)
    end

    def logger_init(name = nil, unused = {})
      # case provider = (opts[:provider] || :builtin).to_s.downcase
      # when 'log4j'
      # when 'builtin'
        # _logger_init_builtin(name, opts)
      # else
        # raise "Unknown logging provider '#{provider}'"
      # end
      @logger = @@logger_factory.call(name || self.class.to_s)
    end

    def error(*msg)
      @logger.error msg.join(' ')
    end

    def warn(*msg)
      @logger.warn msg.join(' ')
    end

    def info(*msg)
      (@logger || _logger).info msg.join(' ')
    end

    def debug(*msg)
      @logger.debug msg.join(' ')
    end

    private

    def _logger
      unless @logger
        @logger = @@logger_factory.call((self.is_a?(Class) ? self : self.class).to_s)
      end
      @logger
    end

  end
end