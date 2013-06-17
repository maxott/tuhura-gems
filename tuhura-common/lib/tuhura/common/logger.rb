
require 'tuhura/common'
#org.apache.log4j.BasicConfigurator.configure();



module Tuhura::Common
  module Logger
    LOGGER_OPTS = {
      provider: 'builtin',
      tracing: true,
      level: :debug,
      log4j_config: 'log4j.properties'
    }

    def self.configure_log4j_from_file(file_name)
      org.apache.log4j.PropertyConfigurator.configure(file_name)
    end

    def logger_init(name = nil, opts = LOGGER_OPTS)
      case provider = (opts[:provider] || :builtin).to_s.downcase
      when 'log4j'
        require 'java'
        require 'log4j'
        if log4j_config = opts[:log4j_config]
          Logger.configure_log4j_from_file(log4j_config)
        end

        require 'log4jruby'
        self.class.enable_logger

        name ||= File.basename($0, ".*")
        @logger = Log4jruby::Logger.get(name, opts)
      when 'builtin'
        _logger_init_builtin(name, opts)
      else
        raise "Unknown logging provider '#{provider}'"
      end

    end

    def error(*msg)
      @logger.error msg.join(' ')
    end

    def warn(*msg)
      @logger.warn msg.join(' ')
    end

    def info(*msg)
      @logger.info msg.join(' ')
    end

    def debug(*msg)
      @logger.debug msg.join(' ')
    end

    private

    def _logger_init_builtin(name, opts)
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

      @logger = Logging::Logger.new(name)


    end

  end
end