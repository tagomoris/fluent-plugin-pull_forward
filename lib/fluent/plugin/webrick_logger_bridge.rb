# -*- coding: utf-8 -*-
module Fluent
  module PullForward
    class WEBrickLogger
      FATAL = 1
      ERROR = 2
      WARN  = 3
      INFO  = 4
      DEBUG = 5

      def initialize(logger)
        @logger = logger
      end

      def <<(str)
        self.log(INFO, str.to_s)
      end

      def close
        # NOP
      end

      def debug(msg)
        self.log(DEBUG, msg)
      end

      def debug?
        @logger.level > Fluent::Log::LEVEL_TRACE
      end

      def error(msg)
        self.log(ERROR, msg)
      end

      def error?
        @logger.level > Fluent::Log::LEVEL_WARN
      end

      def fatal(msg)
        self.log(FATAL, msg)
      end

      def fatal?
        @logger.level > Fluent::Log::LEVEL_ERROR
      end

      def info(msg)
        self.log(INFO, msg)
      end

      def info?
        @logger.level > Fluent::Log::LEVEL_DEBUG
      end

      def level
        # (Fluentd logger level num) -> (Webrick level num)
        # 5 -> 1
        # 4 -> 2
        # 3 -> 3
        # 2 -> 4
        # 1 -> 5
        # (6 - level)
        6 - @logger.level
      end

      def level=(lv)
        @logger.level = case lv
                        when FATAL then 'fatal'
                        when ERROR then 'error'
                        when WARN then 'warn'
                        when INFO then 'info'
                        when DEBUG then 'debug'
                        else
                          raise ArgumentError, "Invalid loglevel for webrick bridge logger: #{lv}"
                        end
      end

      def log(level, msg)
        case level
        when FATAL
          @logger.fatal(msg)
        when ERROR
          @logger.error(msg)
        when WARN
          @logger.warn(msg)
        when INFO
          @logger.info(msg)
        when DEBUG
          @logger.debug(msg)
        else
          raise ArgumentError, "Invalid loglevel for webrick bridge logger: #{lv}"
        end
      end

      def warn(msg)
        self.log(WARN, msg)
      end

      def warn?
        @logger.level > Fluent::Log::LEVEL_INFO
      end
    end
  end
end
