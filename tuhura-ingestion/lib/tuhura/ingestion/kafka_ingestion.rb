
require 'kafka'
require 'time'

module Tuhura::Ingestion

  module Kafka

    KAFKA_OPTS = {
      #offset: -1,
      host: 'cloud1.tempo.nicta.org.au',
      state_domain: 'kafka_bridge'  # part of the state prefix - change if private namespace is desired
    }

    def kafka_inject(max_count = -1)
      reporting_interval = 10
      total_count = 0
      indv_counts = {}
      bm_r = OML4R::Benchmark.bm('kafka_read')
      bm_w = OML4R::Benchmark.bm('db_write')

      #Adding to see how fast we're ingesting per minute
      rate_start = Time.now
      rate_cnt = 0

      OML4R::Benchmark.bm('overall', periodic: reporting_interval) do |bm|
        loop do
          groups = {}
          msg_cnt = 0
          bm_r.task do
            #puts ">>>> READ"
            records = @kafka_consumer.consume
            break if records.nil? || records.empty?
            records.each do |m|
              #puts "MMM>>> #{m.inspect}"
              _kafka_parse_and_process(m).each do |group_id, r|
                next if group_id.nil?
                (groups[group_id] ||= []) << r
                indv_counts[group_id] = (indv_counts[group_id] || 0) + 1
              end
              msg_cnt += 1
            end
            bm_r.step msg_cnt
            bm.step msg_cnt
          end
          break if msg_cnt == 0
          bm_r.report
          @logger.info ">>>> Read #{msg_cnt} record(s) - offset: #{@kafka_consumer.offset}"

          
          cnt = 0
          bm_w.task do
            groups.each do |group_id, events|
              table = get_table_for_group(group_id)
              cnt += table.put(events)
            end
            bm_w.step cnt
          end
          total_count += cnt
          # Persist progress
          _kafka_persist_offset(@kafka_consumer.offset)
          bm_w.report
          info ">>>> Wrote #{cnt}/#{total_count} record(s)" if @verbose

          rate_cnt += msg_cnt
          if (duration = Time.now - rate_start) > 60
            info "Ingestion rate: #{(1.0 * rate_cnt / duration * 60).to_i} records/min"
            rate_start += duration
            rate_cnt = 0
          end

          break if (max_count > 0 && total_count >= max_count)
        end
      end
      bm_r.stop
      bm_w.stop
      db_close()
      info ">>>> SUMMARY: Wrote #{total_count} record(s) - offset: #{@kafka_consumer.offset}"
      indv_counts.each do |group_id, cnt|
        info ">>>>      #{group_id}:\t#{cnt}"
      end
    end

    def kafka_init(opts = {})
      @kafka_opts = KAFKA_OPTS.merge(opts || {})
      @kafka_topic = topic = @kafka_opts[:topic]
      state_domain = @kafka_opts[:state_domain]
      unless db_test_mode?
        path_prefix = "/incmg/#{state_domain}/#{topic}"
      else
        path_prefix = "/incmg/#{state_domain}/test/#{topic}"
      end
      @kafka_state_offset = "#{path_prefix}/offset"
      debug "STATE OFFSET #{@kafka_state_offset} - #{@kafka_opts}"
      unless offset = @kafka_opts[:offset]
        if offset_s = state_get(@kafka_state_offset)
          offset = offset_s.to_i
        else
          offset = 0
        end
        @kafka_opts[:offset] = offset
      end
      info "Kafka options: #{@kafka_opts}"
      @kafka_consumer = ::Kafka::Consumer.new(@kafka_opts)

    end

    def _kafka_parse_and_process(kafka_msg)
      begin
        payload = kafka_msg.payload
        msg = JSON.parse(payload)
      rescue Exception => ex
        @logger.error "While parsing JSON - #{ex} - #{URI::encode(payload)}"
        nil
      end
      begin
        ingest_kafka_message(msg, payload)
      rescue Exception => ex
        @logger.error "While processing message - #{ex}::#{ex.class}"
        @logger.warn ex.backtrace.join("\n\t")
        []
      end
    end

    def _kafka_persist_offset(offset)
      state_put(@kafka_state_offset, offset)
    end

  end
end
