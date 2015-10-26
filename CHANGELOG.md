## Changelog

### 2.0.0 (to be released)
* Complete rewrite of driver management and option parsing
* New interface to support more drivers
* Improved logging
* Fixed never stopping to retry failed connections ([#80](https://github.com/mallocator/Elasticsearch-Exporter/issues/80)) (thanks @eboyme)
* New alternative method for retrieving documents using query api instead of scroll api
* Drivers can now be run in multi threaded / process mode (if both source and target drivers supports it)
* File driver now stores in directory instead of single file
* No longer supporting 0.x version of ElasticSearch

### 1.4.0
* Added option to use http proxy for communication with ElasticSearch (thanks @efuquen)
* Improved error output when server responds with an error ([#65](https://github.com/mallocator/Elasticsearch-Exporter/issues/65)) ([#64](https://github.com/mallocator/Elasticsearch-Exporter/issues/64))
* Added option to use https ([#73](https://github.com/mallocator/Elasticsearch-Exporter/issues/73)) (thanks @mpcarl-ibm)
* Changed the use of a proxy can now be set individually for source and target (options have changed!)
* Fixed problem when importing mapping with 1.x version of ElasticSearch ([#58](https://github.com/mallocator/Elasticsearch-Exporter/issues/58)) ([#68](https://github.com/mallocator/Elasticsearch-Exporter/issues/68)) (thanks @ceilingfish)
* Fixed options file not overriding defaults ([#66](https://github.com/mallocator/Elasticsearch-Exporter/issues/66)) ([#74](https://github.com/mallocator/Elasticsearch-Exporter/issues/74))

### 1.3.3
* Added option to set maximum number of sockets for global http agent
* Better support for Chinese characters in transfers (thanks @d0ngw)
* Improved handling of EsRejectedExecutionException (thanks @d0ngw)

### 1.3.2
* Fixed export to file not working when target file was non existent ([#51](https://github.com/mallocator/Elasticsearch-Exporter/issues/51))
* Fixed data requests from source database not using auth ([#53](https://github.com/mallocator/Elasticsearch-Exporter/pull/53))

### 1.3.1
* Fixed eexport script not running anything

### 1.3.0
* Support for ElasticSearch 1.0 (with auto-detection of servers)
* Deprecated the sourceCompression flag (it's useless since we're auto-detecting compression)
* ElasticSearch driver now supports basic authentication
* Mappings/Settings can now be overridden by using a file.
* Options can now be read from file additionally to parsing program arguments
* Added option to only index documents that don't exist yet (switch between create and index call)
* Added support for aliases in exports

### 1.2.0
* New File Layout (incompatible with exporter 1.1.x)
* Index settings are now also exported, when exporting in scope all or index
* On errors the connection will be retried for fetching and writing data
* Colors dependencies is now included explicitly
* Project is now available from npm repo
* Data files are now compressed by default (compressed source files are auto detected)
* Parent directories are now created for target file if they don't exist
* Tweaked V8 options in tools/ex.sh for better memory usage
* Added option to mute all standard output (errors will still be displayed)
* Tests for all operations

### 1.1.4
* ES driver can now fetch more hits per scroll request
* File driver is now set up properly so that data is streamed much faster
* Fixed attaching events to same file resource multiple times
* Fixed file driver not reading entire files
* Added percentage output to peak memory used
* Fixed some typos in the log messages

### 1.1.3
* Fixed bug where sockets would wait forever to be released (thanks @dplate)
* Fixed bug where the last few documents were not written to target driver (thanks @jostsg)
* Fixed bug where null was written to the target driver as first line
* Increased number of sockets used in es-driver, so that pumping data should now be faster in many cases.

### 1.1.2
* Process will now observe available memory and wait for writes to go through before fetching more data (if gc is available).
* Removed check for target files (which was non sense)

### 1.1.1
* Fixed a bug that would prevent the script from terminating
* Added a test driver to figure out where certain problems are
* Pushed dependency on node from version 0.6 to 0.10 since we're using new stream implementation

### 1.1.0
* Now supports importing/exporting into files
* Refactored most code to be better maintainable
* Removed some old dependencies

### 1.0.0
* First working implementation for pumping data from on database to another (or the same)