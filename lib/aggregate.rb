require 'mongo'
require 'json'
require 'rufus/scheduler'
require 'faraday'

require_relative 'data'
require_relative 'data/collection'
require_relative 'data/work'

module Aggregate
  include DataConf
  include Data::Collection
  include Data::Work

  def make_collection_path collection
    case collection['type']
    when 'funder'
      "/v1/funders/#{collection['id']}/works"
    when 'publisher'
      "/v1/publisher/#{collection['id']}/works"
    end
  end

  def collect_works collection, offset
    conn = Faraday.new({:url => collection['server']})
    rows = collection['rows-per-request']
    success = false

    begin
      works = []
      response = conn.get do |req|
        req.url(make_collection_path(collection))
        req.headers['Accept'] = 'application/json'
        req.headers['Content-Type'] = 'application/json'
        req.body = {:offset => offset, :rows => rows}.to_json
      end

      if response.success?
        works = JSON.parse(response.body)['message']['items']
        if works.empty?
          insert_success
          success = true
        else
          insert_works(works)
        end
      else
        insert_failure(response.status)
      end
    
      offset = offset + rows
    end while !works.empty?

    success
  end

  def aggregate_works modules
    modules = modules.reduce({}) do |memo, obj|
      memo[obj['name']] = obj
      memo
    end

    right_now = DateTime.now

    fulltext_query = {:query => {'link' => {'$exists' => true}}}
    license_query = {:license => {'$in' => modules['license']['acceptable']}}
    archive_query = {:archive => {'$in' => modules['archive']['acceptable']}}

    work_count = works_coll.count
    fulltext_ok_count = works_coll.count({:query => fulltext_query})
    license_ok_count = works_coll.count({:query => license_query})
    archive_ok_count = works_coll.count({:query => archive_query})
    
    tallies_coll.update({:year => right_now.year, 
                          :month => right_now.month, 
                          :day => right_now.day},
                        {:year => right_now.year,
                          :month => right_now.month,
                          :day => right_now.day,
                          :work_count => work_count,
                          :work_count_ok_fulltext => fulltext_ok_count,
                          :work_count_ok_license => license_ok_count,
                          :work_count_ok_archive => archive_ok_count},
                        {:upsert => true})
  end

  def do_works config, scheduler, retries
    puts "Running work collection (try #{retries})"
    success = collect_works(config['collection'], 0)
    
    if success
      aggregate_works(config['module'])
    elsif retries < config['collection']['retry-attempts']
      scheduler.in(config['collection']['retry-interval']) do
        do_works(config, scheduler, retries + 1)
      end
    end
  end

  def prepare_schedule config
    scheduler = Rufus::Scheduler.new
    set :scheduler, Rufus::Scheduler.new

    scheduler.every(config['collection']['interval']) do
      do_works(config, scheduler, 0)
    end

    # Perform an immediate refresh of our database
    scheduler.in('1s') do
      do_works(config, scheduler, 0)
    end
  end

end
