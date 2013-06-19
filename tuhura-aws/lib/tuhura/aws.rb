
require 'aws'

module Tuhura
  module AWS
    DEFAULT_CONFIG = {
      region: 'us-west-2'
    }

    def self.configure_opts(op)
      ::AWS.config(DEFAULT_CONFIG)

      op.on('', '--aws-creds IDSECRET', "AWS Credentials" ) do |token|
        key, secret = token.split(':')
        ::AWS.config(access_key_id: key, secret_access_key: secret)
      end
      op.on('', '--aws-region REGION', "AWS Region" ) do |region|
        ::AWS.config(region: region) #'us-west-2'
      end
    end
  end
end