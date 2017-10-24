# vrsk - Refresher for API caches

vrsk (pronounced /värsk/) is a daemon that refreshes the content of cached HTTP requests.

It's custom made for specific needs and does not (yet) handle any special configuration,
that is, the cache table must be structured as expected and has all fields in a specific format.

Compile and install as usual with go ("go get" will do), and run with "-help" for more information.

An example JSON configuration file is provided. Open ticket if interested and something more
configurable should be implemented.
