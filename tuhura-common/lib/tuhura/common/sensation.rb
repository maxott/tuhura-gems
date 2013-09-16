#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'thread/pool'
#require 'monitor'
require 'json'
require 'zlib'
require 'tuhura/common'

module Tuhura::Common
  module Sensation
    
    SENSATION_OPTS = {
      pool_size: 30
    }
    
    def sensation_scan_day_range(from_date, to_date, bm_name = nil)
      from = sensation_date_to_key(from_date) #date_to_key options[:from] #'2013-01-22'
      to = sensation_date_to_key(to_date) #date_to_key options[:to] #'2013-01-23'
  
      pool = Thread::Pool.new(@sensation_opts[:pool_size])
      bm_name ||= File.basename($0, ".*")
      bm = oml_bm(bm_name, periodic: 1)
      bm.start
      days = ((to_date - from_date) / 86400).to_int
      days.times do |i|
        day = from_date + i * 86400
        pool.process do
          begin
            process_single_day(day, bm)
          rescue Exception => ex
            @logger.error "While processing message - #{ex}"
            @logger.error ex.backtrace.join("\n\t")
          end
        end
      end
      sleep 5 # wait a bit to give the above threads to spin up
      pool.shutdown
      bm.stop
    end
    
    #
    #
    def sensation_scan_day(day, sensation_id, &block)
      from = sensation_date_to_key(day)
      to = sensation_date_to_key(day + 86400)
      hbase_get_table("sen#{sensation_id}", false).range(from, to).each do |row|
        # puts row.fixnum('f:sensation_id')
        # puts row.fixnum('f:user_id')
        payload = row.string('f:raw_msg')
        next unless (m = sensation_parse_message(payload))
        block.call m
      end
    end
    
    def sensation_date_to_key(date)
      ts = date.to_i
      #ts = (ts - ts % 86400)
      sensation_create_key(ts)
    end
    
    # Return a rowkey for a specific date an optional user as well as payload
    #
    def sensation_create_key(ts_sec, user_id = nil, payload = nil)
      key = (ts_sec / 86400) << 64
      if user_id
        key += (user_id << 32)
      end
      if payload
        key += Zlib::crc32(payload)
      end
      key.to_java.toByteArray
    end
    
    # Return sensation message as hash or nil if message doesn't pass validation
    #
    def sensation_parse_message(json_str)
      m = JSON.parse(json_str)
      if m["duration_watched"] > 100000
        @logger.warn "Dropping message '#{m.inspect}'"
        return nil
      end
      m
    end
    
    def sensation_init(opts = SENSATION_OPTS)
      @sensation_opts = opts
    end
  end
end
