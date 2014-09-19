require 'mongo'
require 'date'

require_relative '../data'

module Data::Work
  include DataConf

  def insert_works agency, works

    right_now = DateTime.now
    timestamp = right_now.strftime('%Q').to_i

    works.each do |work|
      works_coll(agency).update(
        {:DOI => work['DOI']}, 
        {:$set=> work, 
          :$setOnInsert=> {:created_at => {'date-parts'=> [right_now.year, right_now.month, right_now.day], :timestamp=> timestamp}}}, 
        {:upsert => true})
    end
  end

  def update_work agency, doi, options
    works_coll(agency).update({:DOI => doi}, {"$set" => options})["updatedExisting"]
  end
end
