require 'mongo'

module Data::Collection

  def insert_success
    collections_coll.insert({:at => Time.now, :success => true})
  end

  def insert_failure code
    collections_coll.insert({:at => Time.now, :success => false, :http_code => code})
  end

  def fetch_tallies
    tallies_coll.find.map do |tally_doc|
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
  end

  def fetch_breakdowns
  end

end
