require 'mongo'

module Data::Collection

  def insert_success
    collections_coll.insert({:at => Time.now, :success => true})
  end

  def insert_failure code
    collections_coll.insert({:at => Time.now, :success => false, :http_code => code})
  end

  def fetch_tallies
    works_coll.find.map do |work_doc|
      {
        :date => {
          :year => work_doc['year'],
          :month => work_doc['month'],
          :day => work_doc['day']
        },
        :count => {
          :total => work_doc['work_count'],
          :fulltext => work_doc['work_count_ok_fulltext'],
          :license => work_doc['work_count_ok_license'],
          :archive => work_doc['work_count_ok_archive']
        }
      }
    end
  end

  def fetch_breakdowns
  end

end
