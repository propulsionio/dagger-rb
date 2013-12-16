# dagger-rb

The DOI aggregator middleware, in Ruby.

## Quick Start

1. Install MongoDB.
2. Install Ruby.
3. Install the bundler Ruby gem:

    $ gem install bundler

4. Clone this repository.

    $ git clone https://github.com/CrossRef/dagger-rb.git
	
5. Configure the middleware (see configuration section below.)
6. Run the middleware using:

    $ bundle install
	$ bundle exec rackup

7. Clone the dagger-ui repository.

    $ git clone https://github.com/CrossRef/dagger-ui.git

8. Configure the dagger-ui project to point to your dagger middleware
   instance (in the `/js/config.js` file.)

## Configuration

The dagger-rb middleware is configured by a `conf.yaml` file. A documented
template configuration can be found in [ `conf.example.yaml` ](conf.example.yaml). Changes should be made to this file before it is renamed to `conf.yaml`.

For information on the YAML file format see the [ YAML homepage ](http://yaml.org).

### Archive Lookup APIs

Archives may implement an API that allows anyone to lookup a DOI to perform
an archived check. The DOI lookup API consists of a single URL that embeds
a DOI. The URL must respond with a predefined form of JSON containing the
archive state of the document identified by the given DOI. For example, an
archive organisation may implement an archive URL at:

    http://archive.org/dois/{{DOI}}

Which responds with the JSON:

    {
	    "status": "ok"
	    "DOI": "10.5555/12345678",
		"archived": "yes",
		"archived-at": 124334435467,
		"archive-type": "dark"
    }

The dagger-rb configuration contains a list of archive lookup URLs, one for
each acceptable archive.
