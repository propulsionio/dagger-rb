require 'sinatra'
require 'yaml'

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
end

get '/tallies' do
  fetch_tallies
end

get '/breakdowns' do
  fetch_breakdowns
end
