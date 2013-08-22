
require 'time'

module Tuhura::Ingestion

  module JsonFileReader

    def json_file_reader_inject(max_count = -1)
      reporting_interval = 10
      total_count = 0
      indv_counts = {}
      bm_r = OML4R::Benchmark.bm('json_read')
      bm_w = OML4R::Benchmark.bm('db_write')

      #Adding to see how fast we're ingesting per minute
      rate_start = Time.now
      rate_cnt = 0
      line_cnt = 0
      lines_in_chunk = 100 # how many lines to read before writing to database
      groups = {}
      tables = {}
      offset = @json_opts[:offset] || 0 # skip that many lines
      error_cnt = 0

      OML4R::Benchmark.bm('overall', periodic: reporting_interval) do |bm|
        bm_r.start
        File.open(@file_name, 'r').each_line do |line|
          next if (line_cnt += 1) <= offset
          begin
            msg = JSON.parse(line)
          rescue Exception => ex
            @logger.error "While parsing JSON - #{ex} - #{line}"
            next
          end
          begin
            msgs = ingest_message(msg)
          rescue Exception => ex
            @logger.error "While processing message - #{ex}::#{ex.class}"
            @logger.warn ex.backtrace.join("\n\t")
            next
          end
          msgs.each do |group_id, r|
            next if group_id.nil?
            (groups[group_id] ||= []) << r
            indv_counts[group_id] = (indv_counts[group_id] || 0) + 1
          end

          next unless line_cnt > lines_in_chunk
          # Now write out the entire chunk stored in 'groups'
          bm_r.step line_cnt
          bm_r.pause; bm_r.report
          bm.step line_cnt
          line_cnt = 0

          cnt = 0
          bm_w.task do
            groups.each do |group_id, events|
              table = tables[group_id] ||= get_table_for_group(group_id)
              cnt += table.put(events)
            end
            bm_w.step cnt
          end
          groups = {}
          total_count += cnt
          rate_cnt += cnt
          bm_w.report
          #info "Wrote #{cnt}/#{total_count} record(s)"

          rate_cnt += line_cnt
          if (duration = Time.now - rate_start) > 60
            info "Ingestion rate: #{(1.0 * rate_cnt / duration * 60).to_i} records/min"
            rate_start += duration
            rate_cnt = 0
          end

          break if (max_count > 0 && total_count >= max_count)
          bm_r.resume
        end
      end
      bm_r.stop
      bm_w.stop
      db_close()
      info ">>>> SUMMARY: Wrote #{total_count} record(s)"
      indv_counts.each do |group_id, cnt|
        info ">>>>      #{group_id}:\t#{cnt}"
      end
    end

    def json_file_reader_init(opts = {})
      @json_opts = opts
      unless @file_name = @json_opts[:file_name]
        raise "Missing 'file_name' option - #@json_opts"
      end
      unless File.readable?(@file_name)
        raise "Can't read file '#{file_name}'"
      end
      info "JsonFileReader options: #{@json_opts}"
    end

  end
end
