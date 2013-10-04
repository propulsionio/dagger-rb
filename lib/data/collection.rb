require 'mongo'

module Data::Collection

  def insert_success
    collections_coll.insert({:at => Time.now, :success => true})
  end

  def insert_failure code
    collections_coll.insert({:at => Time.now, :success => false, :http_code => code})
  end

  def fetch_tallies
    date_sort = [['year', 1], ['month', 1], ['day', 1]]
    tallies = tallies_coll.find({}, {:sort => date_sort}).map do |tally_doc|
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

  def fetch_breakdowns
    date_sort = [['year', -1], ['month', -1], ['day', -1]]
    latest = tallies_coll.find_one({}, {:sort => date_sort})
    pies = {}

    ['fulltext', 'license', 'archive'].each do |k|
      values = []
      values << {:label => 'OK', :value => 0}
      values << {:label => 'Unknown', :value => latest['work_count'] - latest["work_count_ok_#{k}"]}
      values << {:label => 'Not OK', :value => 0}

      pies[k] = [{:key => k, :values => values}]
    end

    pies
  end

  def fetch_publishers
    publishers_coll.find({}).map do |doc|
      {
        :name => doc['name'],
        :prefix => doc['prefix'].split('/').last().gsub(/\./, ''),
        :measures => [doc['work_count']],
        :markers => [doc['work_count']],
        :ranges => [0, 250, 500, 1000, doc['work_count']]
      }
    end
  end

  def fetch_collections
    collections_coll.find({}, {:sort => ['at', -1]}).map do |doc|
      {:at => doc['at'], :success => doc['success'], :http_code => doc['http_code']}
    end
  end

end
