require 'mongo'

module DataConf

  # Contains per day work metadata tallies
  def tallies_coll
    settings.db['tallies']
  end

  # Contains work metadata
  def works_coll
    settings.db['works']
  end

  # Contains info about the success/failure of collection attempts
  def collections_coll
    settings.db['collections']
  end

  # Contains the latest breakdown per publisher
  def publishers_coll
    settings.db['publishers']
  end

  # Contains historic breakdowns per publisher
  def breakdowns_coll
    settings.db['breakdowns']
  end

  def ensure_indexes
    works_coll.ensure_index({:DOI => 1})
    tallies_coll.ensure_index({:year => 1, :month => 1, :day => 1})
    publishers_coll.ensure_index({:prefix => 1, :year => 1, :month => 1, :day => 1})
    breakdowns_coll.ensure_index({:prefix => 1, :year => 1, :month => 1, :day => 1})
  end

  def prepare_database config
    set :db_conn, Mongo::Connection.new(config['mongo']['host'])
    set :db, settings.db_conn[config['mongo']['db']]
    ensure_indexes
  end

end
