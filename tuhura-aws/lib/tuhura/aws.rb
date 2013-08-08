
require 'aws'
require 'tuhura/aws/s3'

module Tuhura
  module AWS
    DEFAULT_CONFIG = {
      region: 'us-west-2'
    }

    def self.configure_opts(op)
      ::AWS.config(DEFAULT_CONFIG)

      op.separator ""
      op.separator "AWS options:"
      op.on('--aws-creds ACCESS:SECRET', "AWS Credentials " ) do |token|
        puts "AWS_CREDS >>>> #{token}"
        key, secret = token.split(':')
        DEFAULT_CONFIG[:access_key_id] = key
        DEFAULT_CONFIG[:secret_access_key] = secret
      end
      op.on('--aws-region REGION', "AWS Region [#{DEFAULT_CONFIG[:region]}]" ) do |region|
        DEFAULT_CONFIG[:region] = region
      end
      op.on('--aws-s3-data-dir DIR', "Local directory to store temporary S3 file [#{Tuhura::AWS::S3::DEFAULTS[:data_dir]}]" ) do |dir|
        Tuhura::AWS::S3::DEFAULTS[:data_dir] = dir
      end
    end

    def self.init(config_opts = {})
      opts = DEFAULT_CONFIG.merge(config_opts)
      unless opts[:access_key_id] || ENV['AWS_ACCESS_KEY_ID']
        raise "Missing AWS Access Key. Set through either ----aws-creds or AWS_ACCESS_KEY_ID environment"
      end
      unless opts[:secret_access_key] || ENV['AWS_SECRET_ACCESS_KEY']
        raise "Missing AWS Secret Access Key. Set through either ----aws-creds or AWS_SECRET_ACCESS_KEY environment"
      end
      ::AWS.config(opts)
    end
  end # module
end # module