require 'mongo'
require 'date'

require_relative '../data'

module Data::Work
  include DataConf

  def insert_works agency, works, modules

    right_now = DateTime.now
    timestamp = right_now.strftime('%Q').to_i

    modules = modules.reduce({}) do |memo, obj|
      memo[obj['name']] = obj
      memo
    end

    works.each do |work|

      categories = [];

      #Archive-
      if(!work.key?('archive')) then
        categories << "UNKNOWN_ARCHIVE";
      elsif((modules['archive']['acceptable'] & work['archive']).length > 0) then
        categories << "ACCEPTABLE_ARCHIVE"
      else
        categories << "UNACCEPTABLE_ARCHIVE"
      end

      #License-
      if(!work.key?('license')) then
        categories << "UNKNOWN_LICENSE"
      elsif(modules['license']['acceptable'].include?work['license'][0]['URL']) then
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

      stat = {
        :date => {'date-parts'=> [right_now.year, right_now.month, right_now.day], :timestamp=> timestamp},
        :categories => categories
      }
      
      works_coll(agency).update(
        {:DOI => work['DOI']}, 
        {:$set=> work,
          :$push => {:stats => stat},
          :$setOnInsert=> {:created_at => {'date-parts'=> [right_now.year, right_now.month, right_now.day], :timestamp=> timestamp}}}, 
        {:upsert => true})
    end
  end

  def update_work agency, doi, options
    works_coll(agency).update({:DOI => doi}, {"$set" => options})["updatedExisting"]
  end
end
