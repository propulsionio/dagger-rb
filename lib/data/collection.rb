require 'mongo'




#   VERY BAD IDEA!!!!

def hash_merge *hashes
  hashes.inject :merge
end



module Data::Collection

  def insert_success agency
    collections_coll(agency).insert({:at => Time.now, :success => true})
  end

  def insert_failure agency, code
    collections_coll(agency).insert({:at => Time.now, :success => false, :http_code => code})
  end

  def fetch_tallies agency
    date_sort = [['year', 1], ['month', 1], ['day', 1]]
    tallies = tallies_coll(agency).find(
      { :$and => [ { year: { :$gte => 2014 } }, { month: { :$gte => 8 } }, { day: { :$gte => 1 } } ] },
      { sort: date_sort }
    ).map do |tally_doc|
      {
        :date => {
          :year => tally_doc['year'],
          :month => tally_doc['month'],
          :day => tally_doc['day']
        },
        :count => {
          :total => tally_doc['work_count'],
          :fulltext => tally_doc['work_count_ok_fulltext'],
          :license => tally_doc['work_count_ok_license'],
          :archive => tally_doc['work_count_ok_archive'],
          :acceptable => tally_doc['work_count_acceptable']
        }
      }
    end

    series = []

    [:total, :fulltext, :license, :archive, :acceptable].each do |count_val|
      series << {
        :key => count_val,
        :values => tallies.map do |d|
          dt = Date.new(d[:date][:year],
                        d[:date][:month],
                        d[:date][:day]).to_time.to_i * 1000
          {:x => dt, :y => d[:count][count_val]}
        end
      }
    end

    
    #puts series.inspect
    #puts series
    #series[0][:values].unshift( { :x=> 1407715200000 , :y=> 0 }  )
    #series[1][:values].unshift( { :x=> 1407715200000 , :y=> 0 }  )
    #series[2][:values].unshift( { :x=> 1407715200000 , :y=> 0 }  )
    #series[3][:values].unshift( { :x=> 1407715200000 , :y=> 0 }  )
    #series[4][:values].unshift( { :x=> 1407715200000 , :y=> 0 }  )
    



    {
      :latest => tallies.last,
      :series => series
    }
  end

  def fetch_breakdowns agency
    date_sort = [['year', -1], ['month', -1], ['day', -1]]
    latest = tallies_coll(agency).find_one({}, {:sort => date_sort})
    pies = {}

    ['fulltext', 'license', 'archive'].each do |k|
      values = []
      values << {:label => 'OK', :value => latest["work_count_ok_#{k}"]}
      values << {:label => 'Unknown', :value => latest["work_count_missing_#{k}"]}
      values << {:label => 'Not OK', :value => latest["work_count_bad_#{k}"]}

      pies[k] = [{:key => k, :values => values}]
    end

    pies
  end

  def fetch_publishers agency

    final_publishers = Array.new()

    publishers = publishers_coll(agency).find({}).map do |doc|
      {
        :name => doc['name'],
        #:member => doc['member'].split('/').last(),
        :measures => [doc['work_count']],
        :markers => [doc['work_count']],
        :ranges => [0, 250, 500, 1000, doc['work_count']]
      }
    end

    publishers.each { |publisher| 
      if(publisher[:name] == 'American Institute of Physics (AIP)') then 
        publisher[:name] = "AIP Publishing" 
      end
    };
    
    # get list of unique names without duplicates
    publisher_names = publishers.uniq{|publisher| publisher[:name] };
    
    # step through each name and combine all of the publisher counts
    publisher_names.each { |publisher_list|

	    # list of all publishers with the same name on the exact line below
            same_publishers = publishers.select { |publisher| publisher[:name] == publisher_list[:name] }

	    # the field values below will but summed, use these variable names below
	    measures = 0
	    markers = 0
	    toprange = 0

	    # for each publisher, sum the measures, markers, and last range
	    # field value
	    same_publishers.each{ |simple|
		measures = measures + simple[:measures].first
		markers = markers + simple[:markers].first
		toprange = toprange + simple[:ranges].last
	    };	    

	    # use the first publish in the list of same publishers as a base line
	    # and update the merged field values
	    merged_publisher = same_publishers[0]

		    merged_publisher[:measures] = [measures]
		    merged_publisher[:markers] = [markers]
		    merged_publisher[:ranges][5] = toprange

	    # get all of the values here

	    merged_publisher.delete(:prefix)

	    publishers + [merged_publisher]

	    # append the one unique publisher to the final publisher list
            final_publishers << merged_publisher

    };

    # set the publishers returned to final publishers
    publishers = final_publishers

    # sort the publisher results
    publishers.sort_by { |publisher| publisher[:name] }

  end

  def fetch_collections agency
    collections_coll(agency).find({}, {:sort => ['at', -1]}).map do |doc|
      {:at => doc['at'], :success => doc['success'], :http_code => doc['http_code']}
    end
  end

  def fetch_tally_table agency
    date_sort = [['year', -1], ['month', -1], ['day', -1]]
    tallies = tallies_coll(agency).find({}, {:sort => date_sort}).map do |doc|
      ["#{doc['year']}/#{doc['month']}/#{doc['day']}",
       doc['work_count'],
       doc['work_count_ok_archive'],
       doc['work_count_missing_archive'],
       doc['work_count_bad_archive'],
       doc['work_count_ok_fulltext'],
       doc['work_count_missing_fulltext'],
       doc['work_count_bad_fulltext'],
       doc['work_count_ok_license'],
       doc['work_count_missing_license'],
       doc['work_count_bad_license'],
       doc['work_count_acceptable']]
    end
  end

  def fetch_publisher_table agency

	# temporary solution until we merge the 
	# publishers on the backend

	publishers = publishers_coll(agency).aggregate([
	  {"$project" => {
		_id: 0, 
		name: 1,
		work_count: 1, 
		work_count_ok_archive: 1,
		work_count_missing_archive: 1,
		work_count_bad_archive: 1,
		work_count_ok_fulltext: 1,
		work_count_missing_fulltext: 1,
		work_count_bad_fulltext: 1,
		work_count_ok_license: 1,
		work_count_missing_license: 1,
		work_count_bad_license: 1,
		work_count_acceptable: 1,
		}
	  },
	  {"$group" => {
		_id: "$name", 
		work_count: {"$max"=>"$work_count"}, 
		work_count_ok_archive: {"$max"=>"$work_count_ok_archive"},
		work_count_missing_archive: {"$max"=>"$work_count_missing_archive"},
		work_count_bad_archive: {"$max"=>"$work_count_bad_archive"},
		work_count_ok_fulltext: {"$max"=>"$work_count_ok_fulltext"},
		work_count_missing_fulltext: {"$max"=>"$work_count_missing_fulltext"},
		work_count_bad_fulltext: {"$max"=>"$work_count_bad_fulltext"},
		work_count_ok_license: {"$max"=>"$work_count_ok_license"},
		work_count_missing_license: {"$max"=>"$work_count_missing_license"},
		work_count_bad_license: {"$max"=>"$work_count_bad_license"},
		work_count_acceptable: {"$max"=>"$work_count_acceptable"},
	  	}
	  }
	]).map do |doc|
	      [doc['_id'],
	       doc['work_count'],
	       doc['work_count_ok_archive'],
	       doc['work_count_missing_archive'],
	       doc['work_count_bad_archive'],
	       doc['work_count_ok_fulltext'],
	       doc['work_count_missing_fulltext'],
	       doc['work_count_bad_fulltext'],
	       doc['work_count_ok_license'],
	       doc['work_count_missing_license'],
	       doc['work_count_bad_license'],
	       doc['work_count_acceptable']]
	    end

  #Temporary fix. Not sure if this is the right way to deal with AIP publisher
  #Similar thing was done in fetch_publishers
  publishers.each { |publisher|
    if(publisher[0] == 'American Institute of Physics (AIP)') then 
      publisher[0] = "AIP Publishing" 
    end
  };

  #Find all AIP publishers
  aip_publishers = publishers.select { |publisher| publisher[0] == 'AIP Publishing' }

  #Delete AIP publishers from publishers array
  publishers = publishers.reject { |publisher| 
    publisher[0] == ('AIP Publishing' || 'American Institute of Physics (AIP)')
  }
  
  #create a new AIP publisher array by assigning it first AIP publisher from AIP publishers array
  aip_publisher = aip_publishers.first

  #Delete first AIP publisher from, since we have already saved it to aip_publisher
  aip_publishers.shift

  #Evaluate aggregated counts for AIP publisher
  aip_publishers.each { |publisher|
    aip_publisher[1] += publisher[1]
    aip_publisher[2] += publisher[2]
    aip_publisher[3] += publisher[3]
    aip_publisher[4] += publisher[4]
    aip_publisher[5] += publisher[5]
    aip_publisher[6] += publisher[6]
    aip_publisher[7] += publisher[7]
    aip_publisher[8] += publisher[8]
    aip_publisher[9] += publisher[9]
    aip_publisher[10] += publisher[10]
    aip_publisher[11] += publisher[11]
  }

  #Add AIP publisher to publishers array
  publishers << aip_publisher;

	#puts final_publishers
  end

  def fetch_publisher_works params, modules

    puts modules;
    data = [];
    query = {};

    if (params[:category] && !params[:category].eql?("all")) then

      if(params[:category].eql?("archive")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:archive => {'$in' => modules['acceptableArchives']}}
        when 'unknown'
          query = {:archive => {'$exists' => false}}
        when 'unacceptable'
          query = {'$and'=> [:archive => {'$exists' => true}]}
        end

      elsif(params[:category].eql?("fulltext")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:link => {'$exists' => true}}
        when 'unknown'
          query = {:link => {'$exists' => false}}
        when 'unacceptable'
          return data;
        end

      elsif(params[:category].eql?("license")) then
        case params[:subcategory]
        when 'acceptable'
          query = {'license.URL' => {'$in' => modules['acceptableLicenses']}}
        when 'unknown'
          query = {:license => {'$exists' => false}}
        when 'unacceptable'
          query = {'$and'=> [:license => {'$exists' => true}, 'license.URL' => {'$nin' => modules['acceptableLicenses']}]}
        end

      elsif (params[:category].eql?("total_acceptable"))
        query = {'$and' => [{:link => {'$exists' => true}}, {'license.URL' => {'$in' => modules['acceptableLicenses']}}, {:archive => {'$in' => modules['acceptableArchives']}}]}
      end

      works_coll(params[:agency]).find(query.merge({:publisher=> params[:name]})).each do |doc|

        if(params[:category].eql?("archive") && params[:subcategory].eql?("unacceptable")) then
          if((modules['acceptableArchives'] & doc['archive']).length == 0) then
            data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
          end
        else
          data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
        end

      end

    else
      works_coll(params[:agency]).find({:publisher=> params[:name]}).each do |doc|
        data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
      end
    end
    data
  end

  def fetch_tally_works params
    data = [];
    query = {};
    category = "";
   
    if (params[:category] && !params[:category].eql?("all") && !params[:category].eql?("total_acceptable")) then

      if(params[:category].eql?("archive")) then
        case params[:subcategory]
        when 'acceptable'
          category = "ACCEPTABLE_ARCHIVE"
        when 'unknown'
          category = "UNKNOWN_ARCHIVE"
        when 'unacceptable'
          category = "UNACCEPTABLE_ARCHIVE"
        end

      elsif(params[:category].eql?("fulltext")) then
        case params[:subcategory]
        when 'acceptable'
          category = "ACCEPTABLE_FULLTEXT"
        when 'unknown'
          category = "UNKNOWN_FULLTEXT"
        when 'unacceptable'
          category = "UNACCEPTABLE_FULLTEXT"
        end

      elsif(params[:category].eql?("license")) then
        case params[:subcategory]
        when 'acceptable'
          category = "ACCEPTABLE_LICENSE"
        when 'unknown'
          category = "UNKNOWN_LICENSE"
        when 'unacceptable'
          category = "UNACCEPTABLE_LICENSE"
        end
      end

      query = {'stats' => {:$elemMatch => 
        {'date.date-parts.0' => params[:year].to_i, 
        'date.date-parts.1' => params[:month].to_i,
        'date.date-parts.2' => params[:day].to_i,
        'categories' => category}}}
    elsif (params[:category].eql?("total_acceptable")) then
      query = {'stats' => {:$elemMatch => 
        {'date.date-parts.0' => params[:year].to_i,
        'date.date-parts.1' => params[:month].to_i,
        'date.date-parts.2' => params[:day].to_i,
        'categories' => {:$all => ["ACCEPTABLE_LICENSE", "ACCEPTABLE_FULLTEXT", "ACCEPTABLE_ARCHIVE"]}}}}
    else
      query = {'stats' => {:$elemMatch => 
        {'date.date-parts.0' => params[:year].to_i,
        'date.date-parts.1' => params[:month].to_i,
        'date.date-parts.2' => params[:day].to_i}}}
    end

    works_coll(params[:agency]).find(query).each do |doc|
      data << {:funder=> doc['funder'], :publisher => doc['publisher'], :doi => doc['DOI'], :url => doc['URL']}
    end
    
    data
  end

end
