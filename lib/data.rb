require 'mongo'

module DataConf

  # Contains per day work metadata tallies
  def tallies_coll(agency)
    settings.send("#{agency}_db")['tallies']
  end

  # Contains work metadata
  def works_coll(agency)
    settings.send("#{agency}_db")['works']
  end

  # Contains info about the success/failure of collection attempts
  def collections_coll(agency)
    settings.send("#{agency}_db")['collections']
  end

  # Contains the latest breakdown per publisher
  def publishers_coll(agency)
    settings.send("#{agency}_db")['publishers']
  end

  # Contains historic breakdowns per publisher
  def breakdowns_coll(agency)
    settings.send("#{agency}_db")['breakdowns']
  end

  def ensure_indexes(agency)
    works_coll(agency).ensure_index({:DOI => 1})
    tallies_coll(agency).ensure_index({:year => 1, :month => 1, :day => 1})
    publishers_coll(agency).ensure_index({:prefix => 1, :year => 1, :month => 1, :day => 1})
    breakdowns_coll(agency).ensure_index({:prefix => 1, :year => 1, :month => 1, :day => 1})
  end

  def prepare_database agency, config
    set "#{agency}_db_conn", Mongo::Connection.new(config['mongo']['host'])
    set "#{agency}_db", settings.send("#{agency}_db_conn")[config['mongo']['db']]
    ensure_indexes(agency)
  end

end
