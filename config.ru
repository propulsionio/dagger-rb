require './app.rb'
require 'sinatra'

set :protection, :except => [:json_csrf]
run Sinatra::Application
