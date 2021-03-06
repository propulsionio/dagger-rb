require 'mongo'
require 'date'

require_relative '../data'

module Data::Work
  include DataConf

  def insert_works agency, works, modules

    right_now = DateTime.now
    timestamp = right_now.strftime('%Q').to_i

    works.each do |work|

      categories = [];

      #Archive-
      if(!work.key?('archive')) then
        categories << "UNKNOWN_ARCHIVE";
      elsif((modules['acceptableArchives'] & work['archive']).length > 0) then
        categories << "ACCEPTABLE_ARCHIVE"
      else
        categories << "UNACCEPTABLE_ARCHIVE"
      end

      #License-
      if(!work.key?('license')) then
        categories << "UNKNOWN_LICENSE"
      elsif(modules['acceptableLicenses'].include?work['license'][0]['URL']) then
        categories << "ACCEPTABLE_LICENSE"
      else
        categories << "UNACCEPTABLE_LICENSE"
      end

      #Fulltext-
      if(!work.key?('link')) then 
        categories << "UNKNOWN_FULLTEXT"
      elsif(work.key?('link')) then
        categories << "ACCEPTABLE_FULLTEXT"
      else
        categories << "UNACCEPTABLE_FULLTEXT"
      end

      work['current_date'] = {'date-parts'=> [right_now.year, right_now.month, right_now.day]};

      stat = {
        :date => {'date-parts'=> [right_now.year, right_now.month, right_now.day]},
        :categories => categories
      }
      
      works_coll(agency).update(
        {:DOI => work['DOI']}, 
        {:$set=> work,
          :$addToSet => {:stats => stat},
          :$setOnInsert=> {:created_at => {'date-parts'=> [right_now.year, right_now.month, right_now.day], :timestamp=> timestamp}}}, 
        {:upsert => true})
    end
  end

  def update_work agency, doi, options
    works_coll(agency).update({:DOI => doi}, {"$set" => options})["updatedExisting"]
  end
end
