require 'mongo'

module Data::Collection

  def insert_success agency
    collections_coll(agency).insert({:at => Time.now, :success => true})
  end

  def insert_failure agency, code
    collections_coll(agency).insert({:at => Time.now, :success => false, :http_code => code})
  end

  def fetch_tallies agency
    date_sort = [['year', 1], ['month', 1], ['day', 1]]
    tallies = tallies_coll(agency).find(
      { :$and => [ { year: { :$gte => 2014 } }, { month: { :$gte => 8 } }, { day: { :$gte => 1 } } ] },
      { sort: date_sort }
    ).map do |tally_doc|
      {
        :date => {
          :year => tally_doc['year'],
          :month => tally_doc['month'],
          :day => tally_doc['day']
        },
        :count => {
          :total => tally_doc['work_count'],
          :fulltext => tally_doc['work_count_ok_fulltext'],
          :license => tally_doc['work_count_ok_license'],
          :archive => tally_doc['work_count_ok_archive'],
          :acceptable => tally_doc['work_count_acceptable']
        }
      }
    end

    series = []

    [:total, :fulltext, :license, :archive, :acceptable].each do |count_val|
      series << {
        :key => count_val,
        :values => tallies.map do |d|
          dt = Date.new(d[:date][:year],
                        d[:date][:month],
                        d[:date][:day]).to_time.to_i * 1000
          {:x => dt, :y => d[:count][count_val]}
        end
      }
    end

    {
      :latest => tallies.last,
      :series => series
    }
  end

  def fetch_breakdowns agency
    date_sort = [['year', -1], ['month', -1], ['day', -1]]
    latest = tallies_coll(agency).find_one({}, {:sort => date_sort})
    pies = {}

    ['fulltext', 'license', 'archive'].each do |k|
      values = []
      values << {:label => 'OK', :value => latest["work_count_ok_#{k}"]}
      values << {:label => 'Unknown', :value => latest["work_count_missing_#{k}"]}
      values << {:label => 'Not OK', :value => latest["work_count_bad_#{k}"]}

      pies[k] = [{:key => k, :values => values}]
    end

    pies
  end

  def fetch_publishers agency
    publishers = publishers_coll(agency).find({}).map do |doc|
      {
        :name => doc['name'],
        :prefix => doc['prefix'].split('/').last().gsub(/\./, ''),
        :measures => [doc['work_count']],
        :markers => [doc['work_count']],
        :ranges => [0, 250, 500, 1000, doc['work_count']]
      }
    end

    publishers.each { |publisher| 
      if(publisher[:name] == 'American Institute of Physics (AIP)') then 
        publisher[:name] = "AIP Publishing" 
      end
    };
    
    wiley_blackwell_publishers = publishers.select { |publisher| publisher[:name] == 'Wiley-Blackwell' }
    wiley_blackwell_total_work_count =  wiley_blackwell_publishers.inject(0) do |work_count, hash|
                                          work_count += hash[:measures][0]
                                          work_count
                                        end
    wiley_blackwell_publisher = wiley_blackwell_publishers.first
    wiley_blackwell_publisher.delete(:prefix)
    wiley_blackwell_publisher.merge!({:measures => [wiley_blackwell_total_work_count],
                                      :markers => [wiley_blackwell_total_work_count],
                                      :ranges => [0, 250, 500, 1000, wiley_blackwell_total_work_count]})
    publishers = publishers.reject { |publisher| publisher[:name] == 'Wiley-Blackwell' }
    publishers + [wiley_blackwell_publisher]
    publishers.sort_by { |publisher| publisher[:name] }
  end

  def fetch_collections agency
    collections_coll(agency).find({}, {:sort => ['at', -1]}).map do |doc|
      {:at => doc['at'], :success => doc['success'], :http_code => doc['http_code']}
    end
  end

  def fetch_tally_table agency
    date_sort = [['year', 1], ['month', 1], ['day', 1]]
    tallies = tallies_coll(agency).find({}, {:sort => date_sort}).map do |doc|
      ["#{doc['year']}/#{doc['month']}/#{doc['day']}",
       doc['work_count'],
       doc['work_count_ok_archive'],
       doc['work_count_missing_archive'],
       doc['work_count_bad_archive'],
       doc['work_count_ok_fulltext'],
       doc['work_count_missing_fulltext'],
       doc['work_count_bad_fulltext'],
       doc['work_count_ok_license'],
       doc['work_count_missing_license'],
       doc['work_count_bad_license'],
       doc['work_count_acceptable']]
    end
  end

  def fetch_publisher_table agency
    publishers_coll(agency).find({}).map do |doc|
      [doc['name'],
       doc['work_count'],
       doc['work_count_ok_archive'],
       doc['work_count_missing_archive'],
       doc['work_count_bad_archive'],
       doc['work_count_ok_fulltext'],
       doc['work_count_missing_fulltext'],
       doc['work_count_bad_fulltext'],
       doc['work_count_ok_license'],
       doc['work_count_missing_license'],
       doc['work_count_bad_license'],
       doc['work_count_acceptable']]
    end
  end

  def fetch_publisher_works params, modules
    data = [];
    query = {};

    modules = modules.reduce({}) do |memo, obj|
      memo[obj['name']] = obj
      memo
    end

    if (params[:category] && !params[:category].eql?("all")) then

      if(params[:category].eql?("archive")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:archive => {'$in' => modules['archive']['acceptable']}}
        when 'unknown'
          query = {:archive => {'$exists' => false}}
        when 'unacceptable'
          puts 'In unacceptable'
          query = {'$and'=> [:archive => {'$exists' => true}]}
        end

      elsif(params[:category].eql?("fulltext")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:link => {'$exists' => true}}
        when 'unknown'
          query = {:link => {'$exists' => false}}
        when 'unacceptable'
          return data;
        end

      elsif(params[:category].eql?("license")) then
        case params[:subcategory]
        when 'acceptable'
          query = {'license.URL' => {'$in' => modules['license']['acceptable']}}
        when 'unknown'
          query = {:license => {'$exists' => false}}
        when 'unacceptable'
          query = {'$and'=> [:license => {'$exists' => true}, 'license.URL' => {'$nin' => modules['license']['acceptable']}]}
        end

      elsif (params[:category].eql?("total_acceptable"))
        query = {'$and' => [{:link => {'$exists' => true}}, {'license.URL' => {'$in' => modules['license']['acceptable']}}, {:archive => {'$in' => modules['archive']['acceptable']}}]}
      end

      works_coll(params[:agency]).find(query.merge({:publisher=> params[:name]})).each do |doc|

        if(params[:category].eql?("archive") && params[:subcategory].eql?("unacceptable")) then
          if((modules['archive']['acceptable'] & doc['archive']).length == 0) then
            data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
          end
        else
          data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
        end

      end

    else
      works_coll(params[:agency]).find({:publisher=> params[:name]}).each do |doc|
        puts doc;
        data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
      end
    end
    data
  end

  def fetch_tally_works params, modules
    data = [];
    query = {};

    #{ :$and => [ { year: { :$gte => 2014 } }, { month: { :$gte => 8 } }, { day: { :$gte => 1 } } ] },

    dateQuery = {:$and=> [{'indexed.date-parts.0.0' => {:$lte => params[:year].to_i}}, 
      {'indexed.date-parts.0.1' => {:$lte => params[:month].to_i}},
      {'indexed.date-parts.0.2' => {:$lte => params[:day].to_i}}]}

    puts dateQuery;

    modules = modules.reduce({}) do |memo, obj|
      memo[obj['name']] = obj
      memo
    end

    if (params[:category] && !params[:category].eql?("all")) then

      if(params[:category].eql?("archive")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:archive => {'$in' => modules['archive']['acceptable']}}
        when 'unknown'
          query = {:archive => {'$exists' => false}}
        when 'unacceptable'
          puts 'In unacceptable'
          query = {'$and'=> [:archive => {'$exists' => true}]}
        end

      elsif(params[:category].eql?("fulltext")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:link => {'$exists' => true}}
        when 'unknown'
          query = {:link => {'$exists' => false}}
        when 'unacceptable'
          return data;
        end

      elsif(params[:category].eql?("license")) then
        case params[:subcategory]
        when 'acceptable'
          query = {'license.URL' => {'$in' => modules['license']['acceptable']}}
        when 'unknown'
          query = {:license => {'$exists' => false}}
        when 'unacceptable'
          query = {'$and'=> [:license => {'$exists' => true}, 'license.URL' => {'$nin' => modules['license']['acceptable']}]}
        end

      elsif (params[:category].eql?("total_acceptable"))
        query = {'$and' => [{:link => {'$exists' => true}}, {'license.URL' => {'$in' => modules['license']['acceptable']}}, {:archive => {'$in' => modules['archive']['acceptable']}}]}
      end



      works_coll(params[:agency]).find(query.merge(dateQuery)).each do |doc|

        if(params[:category].eql?("archive") && params[:subcategory].eql?("unacceptable")) then
          if((modules['archive']['acceptable'] & doc['archive']).length == 0) then
            data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
          end
        else
          data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
        end

      end

    else
      works_coll(params[:agency]).find(dateQuery).each do |doc|
        data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
      end
    end
    data
  end

end
