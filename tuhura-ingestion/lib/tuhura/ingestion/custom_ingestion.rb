#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'json'
require 'tuhura/ingestion/abstract_ingestion'
require 'active_support/core_ext'

module Tuhura::Ingestion
  #OPTS[:kafka][:topic] = 'user'
  Tuhura::Common::OML::OML_OPTS[:appName] = 'custom_ingestion'


  # Read records from various sources and process them according to
  # a block defined in a YAML file
  #
  class CustomIngestion < AbstractIngestion
    C_OPTS = {

    }

    # Allow sub classes to add additional config parameters
    def self.additional_config_parameters(op, options)
      op.separator ""
      op.separator "Custom process options:"
      opts = options[:custom] = C_OPTS
      op.on('--custom-config-file FILE', "Name of config file holding processing instructions" ) do |n|
        opts[:file_name] = n
      end
    end

    def ingest_message(r)
      recs = @filter_proc.call(r)
      #puts "INGEST>> #{recs}"
      recs
    end

    def get_schema_for_table(table_name)
      @schema
    end

    def get_table_for_group(group_name)
      t = super
      t
    end

    def initialize(opts)
      super
      copts = opts[:custom]

      unless file_name = copts[:file_name]
        raise "Missing '--custom-config-file' option"
      end
      unless File.readable? file_name
        raise "Can't read config file '#{file_name}'"
      end
      cfg = YAML.load_file(file_name)
      unless filter = cfg['filter']
        raise "Missing 'filter' block in config file '#{file_name}'"
      end
      @filter_proc = eval(filter)

      unless schema = cfg['schema']
        raise "Missing 'schema' block in config file '#{file_name}'"
      end
      @schema = schema.inject({}){|h, (k,v)| h[k.to_sym] = v; h}

      #puts "OPTS>> #{cfg}"
    end

  end
end

if $0 == __FILE__
  options = {task: :inject, max_msgs: -1}
  Tuhura::Ingestion::CustomIngestion.create(options).work(options)
end



