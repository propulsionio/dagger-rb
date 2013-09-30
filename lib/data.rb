require 'mongo'

module DataConf

  def tallies_coll
    settings.db['tallies']
  end

  def works_coll
    settings.db['works']
  end

  def collections_coll
    settings.db['collections']
  end

  def breakdowns_coll
    settings.db['breakdowns']
  end

  def license_breakdowns_coll
    settings.db['license_breakdowns']
  end

  def archive_breakdowns_coll
    settings.db['archive_breakdowns']
  end

  def fulltext_breakdowns_coll
    settings.db['fulltext_breakdowns']
  end

  def ensure_indexes
    works_coll.ensure_index({:DOI => 1})
    tallies_coll.ensure_index({:year => 1, :month => 1, :day => 1})
    breakdowns_coll.ensure_index({:year => 1, :month => 1, :day => 1})
    license_breakdowns_coll.ensure_index({:year => 1, :month => 1, :day => 1})
    fulltext_breakdowns_coll.ensure_index({:year => 1, :month => 1, :day => 1})
    archive_breakdowns_coll.ensure_index({:year => 1, :month => 1, :day => 1})
  end

  def prepare_database config
    set :db_conn, Mongo::Connection.new(config['mongo']['host'])
    set :db, settings.db_conn[config['mongo']['db']]
    ensure_indexes
  end

end
