#-------------------------------------------------------------------------------
# Copyright (c) 2013 Incoming Media, Inc.
# This software may be used and distributed solely under the terms of the MIT license (License).
# You should find a copy of the License in LICENSE.TXT or at http://opensource.org/licenses/MIT.
# By downloading or using this software you accept the terms and the liability disclaimer in the License.
#-------------------------------------------------------------------------------
require 'time'
require 'tuhura/ingestion'
require 'tuhura/common/logger'
require 'google/api_client'
require 'httparty'

module Tuhura::Ingestion

  # Read content from a single file or a bunch of files and provide it as
  # a continuous stream.
  #
  class GsFileStream
    include Tuhura::Common::Logger

    SCOPE = 'https://www.googleapis.com/auth/devstorage.read_only'
    MAX_BUFFER_SIZE = 1000000

    def eof?
      @eof_f && (@buf_size == 0)
    end

    def readbyte
      #puts ">>>>> READ_BYTE"
      read(1).getbyte(0)
    end

    def read(len)
      puts ">>>>> READ - #{len} (#{@buf_size})"
      @mutex.synchronize do
        while (@buf_size < len)
          raise EOFError.new if @eof_f # no more coming
          @write_sem.wait(@mutex)
        end
        @buf_size -= len
        @read_sem.signal()
        return @buf.slice!(0, len)
      end
    end

    def close
      @read_thread.kill if @read_thread
    end

    def each_line(&block)
      while @stream
        @stream.each_line(&block)
        _next
      end
    end

    def initialize(opts)
      logger_init()
      @opts = opts
      @mutex = Mutex.new
      @read_sem = ConditionVariable.new
      @write_sem = ConditionVariable.new
      @eof_f = false

      @buf = ''
      @buf_size = 0


      unless service_account = opts[:account]

      end
      unless key_file = opts[:key_file]

      end
      unless bucket = opts[:bucket]

      end
      unless object = opts[:object]

      end

      @apiClient = Google::APIClient.new({'application_name' => 'myApp'})
      key = Google::APIClient::PKCS12.load_key(key_file, 'notasecret')
      service_account = Google::APIClient::JWTAsserter.new(
          service_account, SCOPE, key
      )
      @apiClient.authorization = service_account.authorize
      @storage = @apiClient.discovered_api('storage', 'v1beta2')

      _fetch(bucket, object)

    end

    def to_s
      "#<Tuhura::Ingestion::GsFileStream:#{object_id} #{@opts}>"
    end

    def _fetch(bucket, object)

      get_result = @apiClient.execute(
          api_method: @storage.objects.get,
          parameters: {bucket: bucket, object: object, :alt=>'media'}
      )
      #puts ">>> RESPONSE >>>> #{get_result.response.status}"
      #return

      url = get_result.response.env[:response_headers]['location']
      token = "Bearer #{get_result.request.authorization.access_token}"
      uri = URI(url)
      @read_thread = Thread.new do
        Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
          request = Net::HTTP::Get.new uri.request_uri, {"Authorization" => token}
          http.request(request) do |response|
            debug response.inspect
            response.read_body do |chunk|
              @mutex.synchronize do
                @buf << chunk
                @buf_size += chunk.size
                puts ">>>RECV - #{chunk.size}"
                @write_sem.signal
                while @buf_size > MAX_BUFFER_SIZE
                  # block getting more data until queue is serviced
                  @read_sem.wait(@mutex)
                end
              end
            end
            puts ">>>> EOF (@buf_size)"
            @eof_f = true
          end
        end
      end
    end
  end
end

if __FILE__ == $0


  Tuhura::Common::Logger.global_init()
  opts = {
    bucket: 'incoming-prod',
    object: 'JFeedHistory-15798513078055FC070AA-output',
    key_file: '/Users/max/Downloads/ef24466ccc7dd2628cf202f5d2427acfec809cf6-privatekey.p12',
    account: '622452296626-urnt17hi8mb0i0pmjirm3dc8k9302a9f@developer.gserviceaccount.com'
  }
  gs = Tuhura::Ingestion::GsFileStream.new(opts)
  until gs.eof?
    gs.read(500)
    puts 'READ'
    sleep 1
  end
end


