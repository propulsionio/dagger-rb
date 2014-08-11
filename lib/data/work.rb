require 'mongo'

require_relative '../data'

module Data::Work
  include DataConf

  def insert_works agency, works
    works.each do |work|
      works_coll(agency).update({:DOI => work['DOI']}, work, {:upsert => true})
    end
  end

end
