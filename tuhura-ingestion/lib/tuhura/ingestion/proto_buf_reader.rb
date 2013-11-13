#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'time'
require 'tuhura/ingestion'

module Tuhura::Ingestion

  module ProtoBufReader
    PB_OPTS = {
      skip_lines: 0
    }

    def self.configure_opts(op, options)
      op.separator ""
      op.separator "ProtoBuf Reader options:"
      pb_opts = options[:protobuf] = PB_OPTS
      op.on('--pb-source-uri SOURCE', "Name of file/glob or URI to read protobuf records from" ) do |n|
        pb_opts[:source_uri] = n
        options[:reader] = 'protobuf'
      end
      op.on('--pb-type TYPE', "Protobuffer declaration of type contained in file" ) do |t|
        pb_opts[:type] = t
      end
    end

    def pb_reader_inject(max_count = -1)
      reporting_interval = 10
      cfg = OpenStruct.new
      cfg.max_count = max_count
      cfg.total_count = 0
      cfg.total_line_count = 0
      cfg.indv_counts = {}
      cfg.bm_r = OML4R::Benchmark.bm('pb_read')
      cfg.bm_w = OML4R::Benchmark.bm('db_write')

      #Adding to see how fast we're ingesting per minute
      cfg.rate_cnt = 0
      cfg.lines_in_chunk = 1000 # how many records to read before writing to database
      cfg.lines_in_chunk = 2 # how many records to read before writing to database
      cfg.groups = {}
      cfg.tables = {}
      cfg.offset = @pb_opts[:offset] || 0 # skip that many records
      cfg.error_cnt = 0

      cfg.bm_r.start
      OML4R::Benchmark.bm('overall', periodic: reporting_interval) do |bm|
        _pb_reader_inject(cfg, bm)
      end
      cfg.bm_r.stop
      cfg.bm_w.stop
      db_close()
      info ">>>> SUMMARY: Wrote #{cfg.total_count} record(s)"
      cfg.indv_counts.each do |group_id, cnt|
        info ">>>>      #{group_id}:\t#{cnt}"
      end
    end

    def _pb_reader_inject(cfg, bm)
      return if (cfg.max_count > 0 && cfg.total_count >= cfg.max_count)

      line_cnt = 0
      rate_start = Time.new
      rate_cnt = 0
      until @pb_stream.eof?
        next if (cfg.total_line_count += 1) <= cfg.offset

        key, type = Protobuf::Decoder.read_key(@pb_stream)
        unless key == 30
          raise "Illformated protobuf stream"
        end
        ms = Protobuf::Decoder.read_length_delimited(@pb_stream)
        r = Tuhura::Ingestion::Pb::FeedHistory.decode(ms).to_hash
        begin
          msgs = ingest_message(r)
          # msgs.each {|k, v| puts "#{k} -- #{v}" if k.to_s.start_with? 'fee' }
          # raise "XXX"
        rescue Exception => ex
          @logger.error "While processing message - #{ex}::#{ex.class}"
          @logger.warn ex.backtrace.join("\n\t")
          next
        end

        msgs.each do |group_id, r|
          next if group_id.nil?
          (cfg.groups[group_id] ||= []) << r
          cfg.indv_counts[group_id] = (cfg.indv_counts[group_id] || 0) + 1
        end

        next unless (line_cnt += 1) > cfg.lines_in_chunk

        # Now write out the entire chunk stored in 'groups'
        cfg.bm_r.step line_cnt
        cfg.bm_r.pause; cfg.bm_r.report
        bm.step line_cnt
        line_cnt = 0

        cnt = _pb_write(cfg.groups, cfg.tables, cfg.bm_w)

        cfg.total_count += cnt
        rate_cnt += cnt
        cfg.bm_w.report
        #info "Wrote #{cnt}/#{total_count} record(s)"

        if (duration = Time.now - rate_start) > 60
          info "Ingestion rate: #{(1.0 * rate_cnt / duration * 60).to_i} records/min - lines: #{cfg.total_line_cnt}"
          rate_start += duration
          rate_cnt = 0
        end

        break if (cfg.max_count > 0 && cfg.total_line_count >= cfg.max_count)
        cfg.bm_r.resume
      end
      @pb_stream.close
      cfg.total_count += _pb_write(cfg.groups, cfg.tables, cfg.bm_w) # flush out any remaining ones
    end


    def _pb_write(groups, tables, bm_w)
      cnt = 0
      bm_w.task do
        groups.each do |group_id, events|
          table = tables[group_id] ||= get_table_for_group(group_id)
          cnt += table.put(events)
        end
        bm_w.step cnt
      end
      groups.clear
      cnt
    end

    def pb_reader_init(opts = {})
      #puts ">>>> #{opts}"
      @pb_opts = opts

      unless @pb_stream = opts[:stream]
        unless uri = opts[:source_uri]
          raise "Missing '--pb-source-uri' option - #@pb_opts"
        end
        unless @pb_stream = Tuhura::Ingestion::FileStream.new(uri: uri)
          raise "Can't open stream uri '#{uri}'"
        end
      end

      unless pb_type = opts[:type]
        raise "Missing ProtoBuf type '--pb-type'"
      end
      require 'protobuf'
      require "tuhura/ingestion/#{pb_type}.pb"
      info "ProtoBufReader options: #{@pb_opts}"
    end


  end
end

if __FILE__ == $0
  require 'protobuf'
  require 'protobuf/message'
  require 'pp'
  require 'tuhura/ingestion/feed_history.pb'

  f = File.open '/Users/max/Downloads/JFeedHistory-short.pb', 'r'
  #f = File.open '/Users/max/Downloads/JFeedHistory-1579908911982A50B2D2E-output', 'r'
  count = 0
  until f.eof?
    key, type = Protobuf::Decoder.read_key(f)
    unless key == 30
      raise "Illformated protobuf stream"
    end
    ms = Protobuf::Decoder.read_length_delimited(f)
    fh = Tuhura::Ingestion::Pb::FeedHistory.decode(ms)
    if (count == 0)
      puts fh.to_json('indent' => '  ')
      puts fh.feedhistory_id
    end
    puts count if (count % 1000) == 0
    count += 1
  end
  puts "Read #{count} record(s)"
end
