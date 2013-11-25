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
    content_type 'application/json'
    if params[:callback]
      "#{params[:callback]}(#{structure.to_json});"
    else
      structure.to_json
    end
  end
end

configure do
  include Configuration
  include DataConf
  include Aggregate
  
  config = load_configuration('conf.yaml')
  prepare_database(config)
  prepare_schedule(config)

  set :branding, config['branding']
end

get '/tallies' do
  jsonp(fetch_tallies)
end

get '/breakdowns' do
  jsonp(fetch_breakdowns)
end

get '/publishers' do
  jsonp(fetch_publishers)
end

get '/branding' do
  jsonp(settings.branding)
end

get '/collections' do
  jsonp(fetch_collections)
end

get '/tally-table' do
  jsonp(fetch_tally_table)
end

get '/publisher-table' do
  jsonp(fetch_publisher_table)
end

