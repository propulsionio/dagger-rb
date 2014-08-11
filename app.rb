require 'sinatra'
require 'yaml'
require 'json'

require_relative 'lib/configuration'
require_relative 'lib/aggregate'
require_relative 'lib/data'
require_relative 'lib/data/work'
require_relative 'lib/data/collection'

helpers do
  include Data::Work
  include Data::Collection

  def jsonp structure
    if params[:callback]
      content_type 'text/javascript'
      "#{params[:callback]}(#{structure.to_json});"
    else
      content_type 'application/json'
      structure.to_json
    end
  end
end

configure do
  include Configuration
  include DataConf
  include Aggregate

  load_configuration('conf.yaml').each do |agency, config|
    prepare_database(agency, config)
    prepare_schedule(agency, config)

    set "#{agency}_branding", config['branding']
  end
end

get '/data/:agency/tallies' do
  jsonp fetch_tallies(params[:agency])
end

get '/data/:agency/breakdowns' do
  jsonp fetch_breakdowns(params[:agency])
end

get '/data/:agency/publishers' do
  jsonp fetch_publishers(params[:agency])
end

get '/data/:agency/branding' do
  jsonp settings.send("#{params[:agency]}_branding")
end

get '/data/:agency/collections' do
  jsonp fetch_collections(params[:agency])
end

get '/data/:agency/tally-table' do
  jsonp fetch_tally_table(params[:agency])
end

get '/data/:agency/publisher-table' do
  jsonp fetch_publisher_table(params[:agency])
end

