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
          :archive => tally_doc['work_count_ok_archive']
        }
      }
    end

    series = []

    series << {
      :key => 'Total publications',
      :values => tallies.map do |d| 
        dt = Date.new(d[:date][:year], d[:date][:month], d[:date][:day]).to_time.to_i
        {:x => dt, :y => d[:count][:total]}
      end
    }

    series << {
      :key => 'License OK',
      :values => tallies.map do |d|
        dt = Date.new(d[:date][:year], d[:date][:month], d[:date][:day]).to_time.to_i
        {:x => dt, :y => d[:count][:license]}
      end
    }

    series << {
      :key => 'Fulltext OK',
      :values => tallies.map do |d|
        dt = Date.new(d[:date][:year], d[:date][:month], d[:date][:day]).to_time.to_i
        {:x => dt, :y => d[:count][:fulltext]}
      end
    }

    series << {
      :key => 'Archive OK',
      :values => tallies.map do |d|
        dt = Date.new(d[:date][:year], d[:date][:month], d[:date][:day]).to_time.to_i
        {:x => dt, :y => d[:count][:archive]}
      end
    }
    
    {
      :latest => tallies.last,
      :series => series
    }
  end

  def fetch_breakdowns
  end

  def fetch_collections
    collections_coll.find({}, {:sort => ['at', -1]}).map do |doc|
      {:at => doc['at'], :success => doc['success'], :http_code => doc['http_code']}
    end
  end

end
