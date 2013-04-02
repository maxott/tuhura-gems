
require 'tuhura/common'
#org.apache.log4j.BasicConfigurator.configure();



module Tuhura::Common
  module Logger
    LOGGER_OPTS = {
      tracing: true,
      level: :debug,
      log4j_config: 'log4j.properties'
    }
    
    
    def logger_init(name = nil, opts = LOGGER_OPTS)
      require 'java'
      require 'log4j'
      if log4j_config = opts[:log4j_config]
        org.apache.log4j.PropertyConfigurator.configure(log4j_config)
      end

      require 'log4jruby'
      self.class.enable_logger

      name ||= File.basename($0, ".*")
      @logger = Log4jruby::Logger.get(name, opts)
    end
  end
end