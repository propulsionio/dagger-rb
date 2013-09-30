require 'mongo'

require_relative '../data'

module Data::Work
  include DataConf

  def insert_works works
    works.each do |work|
      works_coll.update({:DOI => work['DOI']}, work, {:upsert => true})
    end
  end

end
