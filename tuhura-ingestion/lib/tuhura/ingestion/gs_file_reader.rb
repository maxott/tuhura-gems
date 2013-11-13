

require 'google/api_client'
require 'httparty'


project_id = '622452296626'
SERVICE_ACCOUNT_PKCS12_FILE_PATH = '/Users/max/Downloads/ef24466ccc7dd2628cf202f5d2427acfec809cf6-privatekey.p12'
SERVICE_ACCOUNT_EMAIL = '622452296626-urnt17hi8mb0i0pmjirm3dc8k9302a9f@developer.gserviceaccount.com'
SCOPE = 'https://www.googleapis.com/auth/devstorage.read_only'

# max.ott@gmail account
# project_id = '330233479891'
# client = '330233479891.apps.googleusercontent.com'
# client = '330233479891@developer.gserviceaccount.com'
# key_file = '/Users/max/Downloads/235ab858cb7958dbe2dd0df581ca0a8967533193-privatekey.p12'

# client = '330233479891-duo7oa7vec931qati8515fqfupst8m72@developer.gserviceaccount.com'
# key_file = '/Users/max/Downloads/0473fa33bc49e70c8eb0c8cb988b121365de2bf4-privatekey.p12'
# client = '622452296626-n0uq8pp8ut72v0am7j5eqgpoucta253i.apps.googleusercontent.com'
# client_secret = "tRT0elKKHrpy3UuRJSDSi2Or"

def build_client
  apiClient = Google::APIClient.new({'application_name' => 'myApp'})
  key    = Google::APIClient::KeyUtils.load_from_pkcs12(SERVICE_ACCOUNT_PKCS12_FILE_PATH, 'notasecret')
  apiClient.authorization = Signet::OAuth2::Client.new(
    :token_credential_uri => 'https://accounts.google.com/o/oauth2/token',
    :audience => 'https://accounts.google.com/o/oauth2/token',
    :scope => SCOPE,
    :issuer => SERVICE_ACCOUNT_EMAIL,
    :authorization_uri => 'https://accounts.google.com/o/oauth2/auth',
    #:client_id => client,
    #:client_secret => client_secret
    :signing_key => key
  )
  apiClient
end

def build_client2
  client = Google::APIClient.new({'application_name' => 'myApp'})
  key = Google::APIClient::PKCS12.load_key(SERVICE_ACCOUNT_PKCS12_FILE_PATH, 'notasecret')
  service_account = Google::APIClient::JWTAsserter.new(
      SERVICE_ACCOUNT_EMAIL, SCOPE,
      #'https://www.googleapis.com/auth/devstorage.full_control',
      key
  )
  client.authorization = service_account.authorize
  client
end

object = 'JFeedHistory-15798513078055FC070AA-output' # large
#object = 'JFeedHistory-1579885712811F662EC18-output' # small
apiClient = build_client2
storage = apiClient.discovered_api('storage', 'v1beta2')
get_result = apiClient.execute(
    api_method: storage.objects.get,
    parameters: {bucket: 'incoming-prod', object: object, :alt=>'media'}
    #parameters: {bucket: 'incoming-prod', object: 'JFeedHistory-1579885712811F662EC18-output'})
    #parameters: {bucket: 'max_bcket', object: 'feed_history.proto'})
)
puts get_result.response.status


url = get_result.response.env[:response_headers]['location']
token = "Bearer #{get_result.request.authorization.access_token}"

#response = HTTParty.get(url, :headers => {"Authorization" => token})
#puts response.code, response.message, response.headers.inspect

uri = URI(url)
puts uri.inspect
  size = 0
Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
  request = Net::HTTP::Get.new uri.request_uri, {"Authorization" => token}

  http.request request do |response|
    puts response.inspect
    open 'large_file', 'w' do |io|
      response.read_body do |chunk|
        io.write chunk
        STDOUT.write '.'
        size += chunk.size
      end
    end
  end
end
puts
puts size




if $0 == __FILE__



end