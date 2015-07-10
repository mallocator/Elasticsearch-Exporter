#Elasticsearch-Exporter

A command line script to import/export data from ElasticSearch to various other storage systems.

Features:
* Node.js based command line tool
* Export to another ElasticSearch instance, compressed flat files, MySQL, Mongo DB, etc.
* Recreates mapping and settings on target
* Source data and metadata can be filtered by query
* Specify scope as type, index or whole cluster
* Run in test mode without modifying any data
* Support for proxies, authentication and ssl
* Works with most ElasticSearch versions
* Run multiple requests in separate processed in parallel

# New: Version 2 !!!

This is a brand new implementation with lots of bugs and way too little time to test everything for one lonely developer, so please consider this __beta__ at
best and provide feedback, bug reports and maybe even patches.

This version will not be release to npm until it's been a little more tested out in the wild. Once it will be available consider double checking your package
declarations as this version is not backwards compatible to the 1.x branch.

## Usage

The exporter uses a number of drivers to import and export data and uses a plugin based system that lets developers extend the existing drivers with their own.
The script has an extensive description of all options for each driver which are shown when the script is run without any parameters. Per default only the
options for the ElasticSearch driver are shown. To see what options a particular driver offers either choose the driver as source or target driver on the
command line:
```
node exporter.js -s file -t mongodb
```
The options shown will be specific to the respective source or target driver. All source options are prefixed with 's' and all target options are prefixed
with 't'. Here are some examples on how to use the script. The script is trying to be smart enough to guess all missing options, so that e.g. if you don't
specify a target type, but a target index, the type will be copied over without any changes. If you find that any combination of configuration doesn't make
sense, please file a bug on [Github](https://github.com/mallocator/Elasticsearch-Exporter/issues).
```JavaScript
// copy all indices from machine a to b
node exporter.js -a localhost -b foreignhost

// copy entire index1 to index2 on same machine
node exporter.js -si index1 -ti index2

// copy type1 to type2 in same index
node exporter.js -si index -st type1 -tt type2

// copy type1 from index1 to type2 in index2
node exporter.js -si index1 -st type1 -ti index2 -tt type2

// copy entire index1 from machine a to b
node exporter.js -sh localhost -si index1 -th foreignhost

// copy index1 from machine1 to index2 on machine2
node exporter.js -sh localhost -si index1 -th foreignhost -ti index2

// only copy stuff from machine1 to machine2, that is in the query
node exporter.js -sh localhost -th foreignhost -sq '{"bool":{"must":{"term":{"field1":"value1"}}}}'

// do not execute any operation on machine2, just see the amount of data that would be queried
node exporter.js -sh localhost -th foreignhost -r true

// use basic authentication for source and target connection
node exporter.js -sh localhost -sa myuser:mypass -th foreignhost -ta myuser:mypass
```

From database to file or vice versa you can use the following commands. Note that data files are now stored in a directory structure.
```JavaScript
// Export to file from database
node exporter.js  -sh localhost -si index1 -st type1 -t file -tf filename

// Import from file to database
node exporter.js -s file -sf filename -th foreignhost -ti index2 -tt type2

```

You also read all configuration from a file. The properties of the json read match one to one the extended option names shown in the help output.
```JavaScript
// use an options file for configuration
node exporter.js -o myconfig.json

//myconfig.json
{
    "drivers"; {
        "source": "elasticsearch-query",
        "target": "mysql"
    },
    "source": {
        "host" : "localhost",
        "index" : "myindex"
    }
    "target": {
        "host" : "foreignhost",
    }
}
```

If memory is an issue pass these parameters and the process will try to run garbage collection before reaching memory limitations
```
node --nouse-idle-notification --expose-gc exporter.js ...
```

Or make use of the script in the tools folder:
```
tools/ex.sh ...
```

## Drivers
The script comes with a few drivers readily available that can be used as source or target. While it is possible to use non-ElasticSearch drivers for both
source and target, be advised that support for full transfers is usually limited so that e.g. a copy from mysql to mysql will probably include some transformation
of the data to accommodate for the ElasticSearch format which is used as representation internally.

To get a list of all active drivers run either ```node exporter.js -l``` or ```node exporter.js -ll```.

### ElasticSearch Scroll Driver (id: elasticsearch) (default)
This driver uses the scroll API to fetch data from an ElasticSearch cluster. It's activated by default and offers the old functionality that has been
available with the exporter v1.x. As an alternative you can try the query based driver which supports multi threading for higher performance.

### ElasticSearch Query Driver (id: elasticsearch-query)
This driver uses the query API to fetch data from ElasticSearch. Other than that all calls are the same and inserts are done using the same bulk mechanism
as the default driver. Note though that behaviour on a cluster that is changing state (documents are being inserted/updated) has not been tested. I would not be
surprised if the order between fetch requests of a match_all can change and documents are omitted.

### File Driver (id: file)
This driver has also been ported form the previous version and allows to store data on the local file system. Different than the previous version this driver now
stores data in a directory structure (uncompressed). This allows us to do more complex operations on top of the existing file data, such as partial imports.

### MySQL Driver (id: mysql)
tbd.

### MongoDB Driver (id: mongodb)
tbd.

### HBase Driver (id: hbase)
tbd.

### Google BigQuery Driver (id: bigquery)
tbd.

### AppEngine Datastore Driver (id: datastore)
tbd.

### CSV Driver (id: csv)
tbd.

## Requirements

To run this script you will need at least node v0.10. All required dependencies will be install using npm.

## Installation

Run the following command in the directory where you want the tools installed

	npm install elasticsearch-exporter --production

The required packages will be installed automatically as a dependency, you won't have to do anything else to use the tool. If you install the package with the
global flag (```npm -g```) there will also be a new executable available in the system called "eexport".

Notice that if you don't install released versions from npm and use the master instead, that it is in active development and might not be working. For help and
questions you can always [file an issue](https://github.com/mallocator/Elasticsearch-Exporter/issues).

## Improving Performance

If you're trying to export a large amount of data it can take quite a while to export that data. Here are some tips that might help you speed up the process.

### Reduce Network Hops

In most cases the limiting resource when running the exporter has not been CPU or Memory, but Network IO and response time from ElasticSearch. In some cases it
is possible to speed up the process by reducing network hops. The closer you can get to either the source or target database the better. Try running on one of
the nodes to reduce latency. If you're running a larger cluster try to run the script on the node where most shards of the data are available. This will further
prevent ElasticSearch to make internal hops.

### Increase Process Memory

In some cases the number of requests queue up filling up memory. When running with garbage collection enabled, the client will wait until memory has been freed
if it should fill up, but this might also cause the entire process to take longer to finish. Instead you can try and increase the amount of memory that the node
process has available. To set memory to a higher value just pass this option with your desired memory setting to the node executable: `--max-old-space-size=600`.
Note that there is an upper limit to the amount a node process can receive, so at some point it doesn't make much sense to increase it any further.

### Increase Concurrent Request limit

It might be the case that your network connection can handle a lot more than is typical and that the script is spending the most time waiting for additional
sockets to be free. To get around this you can increase the maximum number of sockets on the global http agent by using the option flag for it
(`--source.maxSockets`). Increase this to see if it will improve anything.

### Split up into multiple Jobs

It might be possible to run the script multiple times in parallel. Since the exporter is single threaded it will only make use of one core and performance can
be gained by querying ElasticSearch multiple times in parallel. To do so simple run the exporter tool against individual types or indexes instead of the entire
cluster. If the bulk of your data is contained one type make use of the query parameter to further partition the existing data. Since it is necessary to understand
the structure of the existing data it is not planned the exporter will attempt to do any of the optimization automatically.

### Export to file first

Sometimes the whole pipe from source to target cluster is simply slow, unstable and annoying. In such a case try to export to a local file first. This way you
have a complete backup with all the data and can transfer this to the target machine. While this might overall take more time, it might increase the speed of the
individual steps.

### Change the fetching size

It might help if you change the size of each scan request that fetches data. The current default of the option `--source.size` is set to 10. Increasing or decreasing
this value might have great performance impact on the actual export.

### Disable replication

The default setting for any index to create 1 replica for each shard. While this is generally speaking a good thing, it can be really bad while importing a new index.
To improve performance you can set the number of replicas to 0 until the import is done and then increase the number of replicas. Under the right circumstances you
might see a tremendous improvement in performance. The number of replicas can either be controlled by setting manual metadata or by using the `--target.replicas` flag

### Optimizing the ElasticSearch Cluster

This tool will only run as fast as your cluster can keep up. If the nodes are under heavy load, errors can occur and the entire process will take longer. How to
optimize your Cluster is a whole other chapter and depends on the version of ElasticSearch that you're running. Try the official documentation, google or the
ElasticSearch mailing groups for help. The topic is too complex for us to cover it here.

## Developing your own drivers

The exporter uses a plugable system for all drivers so that it can be extended and make use of more drivers supplied by the community. If you want to use a
custom driver point the exporter to the directory it resides in using ```node exporter.js -d <dir to my plugin>```. Note that drivers need to end with the
extension .driver.js to be recognized as a driver for the exporter.

When a driver is loaded the exporter will verify is the module signature is correct by scanning for required functions and their parameters. To keep things
in a unified format all parameters need to be named as specified by the exporter. You can add additional parameters for internal use to any function but, the
minimum set of parameters needs to be there.

For a more detailed example take a look at [driver.interface.js](drivers/driver.interface.js). All required functions are described there, the file is purely
for documentation purposes. It also showcases some of the data formats that are passed between the core and the drivers. For all formats assume that ElasticSearch
is used as a reference.

If your driver needs to store data that is not available in the standard ElasticSearch format, make use of the
[_meta](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/mapping-meta.html) field.

For more detailed help check out the [developers guide](DEVELOPERS.md)

## Tests

To run the tests you must install the development dependencies along with the production dependencies

	npm install elasticsearch-exporter

After that you can just run ```npm test``` to see an output of all existing tests.

## Bugs and Feature Requests

I try to find all the bugs and have tests to cover all cases, but since I'm working on this project alone, it's easy to miss something.
Also I'm trying to think of new features to implement, but most of the time I add new features because someone asked me for it.
So please report any bugs or feature request to mallox@pyxzl.net or file an issue directly on [Github](https://github.com/mallocator/Elasticsearch-Exporter/issues).
Before submitting a bug report specific to your problem, try running the same command and verbose mode `-v` so that I have some additional information to work with.
Thanks!

## Donations

Wow, apparently there are people who want to support me. If you're one of them you can do so via bitcoin over here: [mallox@coinbase](https://www.coinbase.com/Mallox)

## Changelog

### 2.0.0 (to be released)
* Complete rewrite of driver management and option parsing
* New interface to support more drivers
* Improved logging
* Fixed never stopping to retry failed connections ([#80](https://github.com/mallocator/Elasticsearch-Exporter/issues/80)) (thanks @eboyme)
* New alternative method for retrieving documents using query api instead of scroll api
* Drivers can now be run in multi threaded / process mode (if both source and target drivers supports it)
* File driver now stores in directory instead of single file

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
