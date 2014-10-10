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
    set "#{agency}_modules", config['module'];
  end
end

post '/:agency/works' do
  headers 'Access-Control-Allow-Origin' => '*',
          'Access-Control-Allow-Methods' => ['POST'],
          'Access-Control-Allow-Headers' => 'Content-Type'
  options = { :url_accessible => params[:url_accessible] }
  status (update_work(params[:agency], params[:doi], options) ? 200 : 404)
end

get '/:agency/tallies' do
  jsonp fetch_tallies(params[:agency])
end

get '/:agency/breakdowns' do
  jsonp fetch_breakdowns(params[:agency])
end

get '/:agency/publishers' do
  jsonp fetch_publishers(params[:agency])
end

get '/:agency/branding' do
  jsonp settings.send("#{params[:agency]}_branding")
end

get '/:agency/collections' do
  jsonp fetch_collections(params[:agency])
end

get '/:agency/tally-table' do
  jsonp fetch_tally_table(params[:agency])
end

get '/:agency/publisher-table' do
  jsonp fetch_publisher_table(params[:agency])
end

get '/:agency/publisher/:name' do
  headers 'Access-Control-Allow-Origin' => '*',
          'Access-Control-Allow-Methods' => ['OPTIONS', 'GET', 'POST']

  jsonp fetch_publisher_works(params, settings.send("#{params[:agency]}_modules"));
end

get '/:agency/tallies/:year/:month/:day' do

  headers 'Access-Control-Allow-Origin' => '*',
          'Access-Control-Allow-Methods' => ['OPTIONS', 'GET', 'POST']

  jsonp fetch_tally_works(params, settings.send("#{params[:agency]}_modules"));
end
