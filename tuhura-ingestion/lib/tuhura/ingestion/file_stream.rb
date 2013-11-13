#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'time'
require 'tuhura/ingestion'

module Tuhura::Ingestion

  # Read content from a single file or a bunch of files and provide it as
  # a continuous stream.
  #
  class FileStream
    include Tuhura::Common::Logger

    def eof?
      return true if @stream.nil?
      if @stream.eof?
        _next
        return eof?
      end
      false
    end

    def readbyte
      begin
        return @stream.readbyte
      rescue EOFError => ex
        if eof?()
          raise ex
        else
          return readbyte()
        end
      end
    end

    def read(len)
      begin
        return @stream.read(len)
      rescue EOFError => ex
        if eof?()
          raise ex
        else
          return read(len)
        end
      end
    end

    def close
      @stream.close if @stream
    end

    def each_line(&block)
      while @stream
        @stream.each_line(&block)
        _next
      end
    end

    def initialize(opts = {})
      logger_init()
      unless file_pattern = opts[:file_pattern] || opts[:file] || opts[:uri]
        raise "Missing 'file_pattern', 'file', or 'uri' option (#{opts})"
      end
      debug "Sourcing '#{file_pattern}'"
      @f_iter = (file_pattern == '-' ? [STDIN] : Dir.glob(file_pattern)).each
      @stream = nil
      _next
    end

    def _next
      begin
        @stream.close if @stream
        fn = @f_iter.next
        debug "Opening #{fn}"
        @stream = (fn == STDIN ? STDIN : File.open(fn, 'r'))
      rescue StopIteration => ex
        @stream = nil
      end
    end
  end
end

