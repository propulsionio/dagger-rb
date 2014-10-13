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
    publishers_coll(agency).find({}).map do |doc|
      [doc['name'],
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

  def fetch_publisher_works params, modules
    data = [];
    query = {};

    modules = modules.reduce({}) do |memo, obj|
      memo[obj['name']] = obj
      memo
    end

    if (params[:category] && !params[:category].eql?("all")) then

      if(params[:category].eql?("archive")) then
        case params[:subcategory]
        when 'acceptable'
          query = {:archive => {'$in' => modules['archive']['acceptable']}}
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
          query = {'license.URL' => {'$in' => modules['license']['acceptable']}}
        when 'unknown'
          query = {:license => {'$exists' => false}}
        when 'unacceptable'
          query = {'$and'=> [:license => {'$exists' => true}, 'license.URL' => {'$nin' => modules['license']['acceptable']}]}
        end

      elsif (params[:category].eql?("total_acceptable"))
        query = {'$and' => [{:link => {'$exists' => true}}, {'license.URL' => {'$in' => modules['license']['acceptable']}}, {:archive => {'$in' => modules['archive']['acceptable']}}]}
      end

      works_coll(params[:agency]).find(query.merge({:publisher=> params[:name]})).each do |doc|

        if(params[:category].eql?("archive") && params[:subcategory].eql?("unacceptable")) then
          if((modules['archive']['acceptable'] & doc['archive']).length == 0) then
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

  def fetch_tally_works params, modules
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
