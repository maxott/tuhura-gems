#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'tuhura/ingestion'


require 'optparse'
require 'tuhura/common/logger'

require 'avro'
require 'json'
require 'csv'

module Tuhura::Ingestion

  class AvroTools
    include Tuhura::Common::Logger


    def self.create(options = {}, &block)
      @@op = OptionParser.new do |op|
        # op.on( '-D', '--drop-tables', "Drop all table" ) do
          # options[:task] = :drop_tables
        # end
        # op.on( '-m', '--max-messages NUM', "Number of messages to process [ALL]" ) do |n|
          # options[:max_msgs] = n.to_i
        # end
        op.on('-v', '--verbose', "Verbose mode [#{options[:verbose] == true}]" ) do
          options[:verbose] = true
        end
        op.on_tail('-h', '--help', 'Display this screen') do
          puts op
          exit
        end

        block.call(op) if block
      end
      @@op.parse!
      options[:task] = ARGV.shift
      self.new(options)
    end

    def work(opts = {}, &block)

      if block
        block.call(self)
      else
        unless task = opts[:task]
          usage
          exit(0)
        end

        task = task.to_sym
        unless self.respond_to? task
          error "Unknown task '#{opts[:task]}'"
          usage
          exit(-1)
        end

        send(task)
      end
      info "Done"
    end

    def usage()
      puts %{
Usage: ... avro task [options]

  Execute various AVRO tasks, such as:

    to_csv  AVRO_FILE CSV_FILE     ... Convert an AVRO file into a CSV file
    user_json_to_csv JSON_FILE CSV_FILE ... Convert from json dump of {'key1': {...}, 'key2': ..} to CSV
}
    end

    def to_csv_task
      unless avro_file = ARGV.shift
        error "to_csv task requires additional parameter 'avro_file'"
        exit(-1)
      end
      unless File.readable?(avro_file)
        error "Can't find or read file '#{avro_file}'"
        exit(-1)
      end

      unless out_file_name = ARGV.shift
        error "to_csv task requires additional parameter 'csv_file'"
        exit(-1)
      end
      writer = File.open(out_file_name, 'w')

      reader = Avro::DataFile.open(avro_file, 'r')
      fields = reader.datum_reader.writers_schema.fields.map {|f| f.name}
      #puts fields

      cnt = 0
      reader.each do |r|
        cols = fields.map {|k| r[k]}
        writer << cols.join("\t")
        cnt += 1
      end
      writer.close
      info "Written #{cnt} rows to #{out_file_name}"
    end

    CLASS2CAST = {
      String => lambda {|s| s},
      Float => lambda {|s| s.to_f},
      Fixnum => lambda {|s| s.to_i},
      Array => lambda {|a| a}
    }

    def user_json_to_csv
      fields = nil
      _process_json_records do |id, rec, writer, date|
        rows = []
        unless fields
          fields = rec.map do |k, v2|
            next if k == 'videos'
            t = v2.class
            unless p = CLASS2CAST[t]
              error "Don't know how to cast '#{t}'"
              exit(-1)
            end
            [k, p]
          end.compact
          schema = fields.map {|k, p| k}
          schema.insert(0, 'date') if date
          rows << "# key #{schema.join(' ')}\n"
        end

        cols = fields.map do |fn, cp|
          v = cp.call(rec[fn])
          v.to_json
        end
        cols.insert(0, date) if date
        cols.insert(0, id)
        rows << cols.to_csv
      end
    end

    def video_user_json_to_csv
      first = true
      _process_json_records do |user_id, rec, writer, date|
        rows = []
        if first
          rows << "# user_id bigint, video_id bigint, date date\n"
          first = false
        end

        rec['videos'].each do |video_id|
          r = [user_id, video_id, date]
          rows << r.to_csv
        end
        rows
      end
    end



    def _process_json_records(&block)
      readers, writer = _get_in_out_files_from_argv(:json_to_csv)
      cnt = 0

      fields = nil
      readers.each do |reader|
        date = (reader.path.match(/[0-9][-0-9]+/) || '').to_s
        begin
          h = JSON.parse(reader.read)
        rescue Exception => ex
          warn "while parsing file #{reader.path} - #{ex.to_s[0 .. 40]}"
          next
        end

        h.each do |id, rec|
          block.call(id, rec, writer, date).each do |row|
            writer << row
            cnt += 1
          end
        end
        writer.flush
        info "Written #{cnt} rows to #{writer.path}"
      end
      writer.close
    end


    def _get_in_out_files_from_argv(task_name)
      unless in_file_pattern = ARGV.shift
        error "#{task_name} task requires additional parameter for input file"
        exit(-1)
      end
      info "Input pattern #{in_file_pattern}"
      readers = []
      Dir.glob(in_file_pattern).each do |in_file_name|
        unless File.readable?(in_file_name)
          error "Can't find or read file '#{in_file_name}'"
          exit(-1)
        end
        readers << File.open(in_file_name, 'r')
      end

      unless out_file_name = ARGV.shift
        error "#{task_name} task requires additional parameter output file"
        exit(-1)
      end
      info "Writing results to #{out_file_name}"
      writer = File.open(out_file_name, 'w')
      [readers, writer]
    end

    def initialize(opts)
      logger_init(opts[:logger])
      #
    end

  end # class
end # module

if $0 == __FILE__
  options = {}
  t = Tuhura::Ingestion::AvroTools.create(options)
  t.work(options)
end


