
require 'tuhura/common'
#org.apache.log4j.BasicConfigurator.configure();



module Tuhura::Common
  module Logger
    LOGGER_OPTS = {
      tracing: true,
      level: :debug
    }
    
    
    def logger_init(name = nil, opts = LOGGER_OPTS)
      require 'java'
      require 'log4j'
      org.apache.log4j.PropertyConfigurator.configure('log4j.properties');

      require 'log4jruby'
      self.class.enable_logger

      name ||= File.basename($0, ".*")
      @logger = Log4jruby::Logger.get(name, opts)
    end
  end
end