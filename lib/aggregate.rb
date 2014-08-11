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
    query_str = ""

    if collection['custom-filters']
      query_str = "?filter=#{collection['custom-filters']}"
    end

    case collection['type']
    when 'funder'
      "/v1/funders/#{collection['id']}/works#{query_str}"
    when 'publisher'
      "/v1/publishers/#{collection['id']}/works#{query_str}"
    end
  end

  def aggregate_publishers modules, right_now
    group = {'$group' => {
        '_id' => {
          'prefix' => '$prefix',
          'name' => '$publisher'
        },
        'work_count' => {'$sum' => 1}}}

    works_coll.aggregate([group]).each do |doc|
      insert_doc = {
        :prefix => doc['_id']['prefix'],
        :name => doc['_id']['name'],
        :work_count => doc['work_count']
      }

      today = {
        :year => right_now.year,
        :month => right_now.month,
        :day => right_now.day
      }

      prefix_query = {:prefix => insert_doc[:prefix]}

      missing_license_query = {:license => {'$exists' => false}}.merge(prefix_query)
      missing_archive_query = {:archive => {'$exists' => false}}.merge(prefix_query)
      missing_fulltext_query = {:link => {'$exists' => false}}.merge(prefix_query)

      has_fulltext_query = {:link => {'$exists' => true}}.merge(prefix_query)

      # TODO Improve to include version (am, vor), max days from publication
      # TODO Check fulltexts for resolvability, check fulltext version, fulltext mime
      ok_license_query = {'license.URL' => {'$in' => modules['license']['acceptable']}}
        .merge(prefix_query)
      ok_archive_query = {:archive => {'$in' => modules['archive']['acceptable']}}
        .merge(prefix_query)
      acceptable_query = {'$and' => [has_fulltext_query, ok_license_query, ok_archive_query]}

      work_count = works_coll.count({:query => prefix_query})
      fulltext_ok_count = works_coll.count({:query => has_fulltext_query})
      license_ok_count = works_coll.count({:query => ok_license_query})
      archive_ok_count = works_coll.count({:query => ok_archive_query})
      acceptable_count = works_coll.count({:query => acceptable_query})

      fulltext_missing_count = works_coll.count({:query => missing_fulltext_query})
      license_missing_count = works_coll.count({:query => missing_license_query})
      archive_missing_count = works_coll.count({:query => missing_archive_query})

      fulltext_bad_count = work_count - (fulltext_missing_count + fulltext_ok_count)
      license_bad_count = work_count - (license_missing_count + license_ok_count)
      archive_bad_count = work_count - (archive_missing_count + archive_ok_count)

      unacceptable_count = work_count - acceptable_count

      insert_doc = insert_doc.merge(today)
        .merge({:work_count => work_count,
                 :work_count_bad_fulltext => fulltext_bad_count,
                 :work_count_bad_license => license_bad_count,
                 :work_count_bad_archive => archive_bad_count,
                 :work_count_missing_fulltext => fulltext_missing_count,
                 :work_count_missing_license => license_missing_count,
                 :work_count_missing_archive => archive_missing_count,
                 :work_count_ok_fulltext => fulltext_ok_count,
                 :work_count_ok_license => license_ok_count,
                 :work_count_ok_archive => archive_ok_count,
                 :work_count_acceptable => acceptable_count,
                 :work_count_unacceptable => unacceptable_count})

      breakdowns_coll.update({:prefix => insert_doc[:prefix]}.merge(today),
                             insert_doc, {:upsert => true})
      publishers_coll.update({:prefix => insert_doc[:prefix]},
                             insert_doc, {:upsert => true})
    end
  end

  def apply_work_rules collection, works
    if collection['prefixes']['include']
      include_list = collection['prefixes']['include'].map do |prefix|
        "http://id.crossref.org/prefix/#{prefix}"
      end

      works.reject do |work|
        !include_list.include?(work['prefix'])
      end
    elsif collection['prefixes']['exclude']
      exclude_list = collection['prefixes']['exclude'].map do |prefix|
        "http://id.crossref.org/prefix/#{prefix}"
      end

      works.reject do |work|
        exclude_list.include?(work['prefix'])
      end
    else
      works
    end
  end

  def collect_works collection, offset
    conn = Faraday.new({:url => collection['server']})
    rows = collection['rows-per-request']
    success = false

    begin
      works = []
      response = conn.get do |req|
        req.url(make_collection_path(collection) << "&offset=#{offset}&rows=#{rows}")
      end

      if response.success?
        works = JSON.parse(response.body)['message']['items']
        if works.empty?
          insert_success
          success = true
        else
          insert_works(apply_work_rules(collection, works))
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

    # When collection type of :publisher is chosen, this should do the
    # reverse - group by funder
    aggregate_publishers(modules, right_now)

    missing_license_query = {:license => {'$exists' => false}}
    missing_archive_query = {:archive => {'$exists' => false}}
    missing_fulltext_query = {:link => {'$exists' => false}}

    has_fulltext_query = {:link => {'$exists' => true}}

    # TODO Improve to include version (am, vor), max days from publication
    # TODO Check fulltexts for resolvability, check fulltext version, fulltext mime
    ok_license_query = {'license.URL' => {'$in' => modules['license']['acceptable']}}
    ok_archive_query = {:archive => {'$in' => modules['archive']['acceptable']}}
    acceptable_query = {'$and' => [has_fulltext_query, ok_license_query, ok_archive_query]}

    work_count = works_coll.count
    fulltext_ok_count = works_coll.count({:query => has_fulltext_query})
    license_ok_count = works_coll.count({:query => ok_license_query})
    archive_ok_count = works_coll.count({:query => ok_archive_query})
    acceptable_count = works_coll.count({:query => acceptable_query})

    fulltext_missing_count = works_coll.count({:query => missing_fulltext_query})
    license_missing_count = works_coll.count({:query => missing_license_query})
    archive_missing_count = works_coll.count({:query => missing_archive_query})

    fulltext_bad_count = work_count - (fulltext_missing_count + fulltext_ok_count)
    license_bad_count = work_count - (license_missing_count + license_ok_count)
    archive_bad_count = work_count - (archive_missing_count + archive_ok_count)

    unacceptable_count = work_count - acceptable_count

    tallies_coll.update({:year => right_now.year,
                          :month => right_now.month,
                          :day => right_now.day},
                        {:year => right_now.year,
                          :month => right_now.month,
                          :day => right_now.day,
                          :work_count => work_count,
                          :work_count_bad_fulltext => fulltext_bad_count,
                          :work_count_bad_license => license_bad_count,
                          :work_count_bad_archive => archive_bad_count,
                          :work_count_missing_fulltext => fulltext_missing_count,
                          :work_count_missing_license => license_missing_count,
                          :work_count_missing_archive => archive_missing_count,
                          :work_count_ok_fulltext => fulltext_ok_count,
                          :work_count_ok_license => license_ok_count,
                          :work_count_ok_archive => archive_ok_count,
                          :work_count_acceptable => acceptable_count,
                          :work_count_unacceptable => unacceptable_count},
                        {:upsert => true})
  end

  def do_works config, scheduler, retries
    if File.exists?('tmp/pause.txt')
      puts 'Skipping work sync due to tmp/pause.txt file'
    else
      puts "Attempting work sync (retry #{retries})"

      success = collect_works(config['collection'], 0)

      if success
        aggregate_works(config['module'])
      elsif retries < config['collection']['retry-attempts']
        scheduler.in(config['collection']['retry-interval']) do
          do_works(config, scheduler, retries + 1)
        end
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
