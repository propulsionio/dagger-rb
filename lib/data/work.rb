require 'mongo'

require_relative '../data'

module Data::Work
  include DataConf

  def insert_works works
    works_coll.insert(works)
  end

end
