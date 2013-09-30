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
end

configure do
  include Configuration
  include DataConf
  include Aggregate
  
  config = load_configuration("conf.yaml")
  prepare_database(config)
  prepare_schedule(config)

  set :branding, config['branding']
end

get '/tallies' do
  content_type 'application/json'
  fetch_tallies.to_json
end

get '/breakdowns' do
  content_type 'application/json'
  fetch_breakdowns.to_json
end

get '/branding' do
  content_type 'application/json'
  settings.branding.to_json
end
