#Elasticsearch-Exporter

A small script to export data from one Elasticsearch cluster into another.

Features:
* Node.js based command line tool
* Export to ElasticSearch or (compressed) flat files
* Recreates mapping on target
* Source data can be filtered by query
* Specify scope as type, index or whole cluster
* Sync Index settings along with existing mappings
* Run in test mode without modifying any data

## Usage

A number of combinations of the options can be used, of which some are listed here to give you an idea of what can be done. The complete list of options can be found when running the exporter without any parameters.
The script is trying to be smart enough to guess all missing options, so that e.g. if you don't specify a target type, but a target index, the type will be copied over without any changes.
If you find that any combination of configuration doesn't make sense, please file a bug on [Github](https://github.com/mallocator/Elasticsearch-Exporter/issues).
```JavaScript
// copy all indices from machine a to b
node exporter.js -a localhost -b foreignhost

// copy entire index1 to index2 on same machine
node exporter.js -i index1 -j index2

// copy type1 to type2 in same index
node exporter.js -i index -t type1 -u type2

// copy type1 from index1 to type2 in index2
node exporter.js -i index1 -t type1 -j index2 -u type2

// copy entire index1 from machine a to b
node exporter.js -a localhost -i index1 -b foreignhost

// copy index1 from machine1 to index2 on machine2
node exporter.js -a localhost -i index1 -b foreignhost -j index2

// only copy stuff from machine1 to machine2, that is in the query
node exporter.js -a localhost -b foreignhost -s '{"bool":{"must":{"term":{"field1":"value1"}}}}'

// do not execute any operation on machine2, just see the amount of data that would be queried
node exporter.js -a localhost -b foreignhost -r true

// use basic authentication for source and target connection
node exporter.js -a localhost -A myuser:mypass -b foreignhost -B myuser:mypass
```

You can now read all configuration from a file. The properties of the json read match one to one the extended option names shown in the help output.
```JavaScript
// use an options file for configuration
node exporter.js -o myconfig.json

//myconfig.json
{
    "sourceHost" : "localhost",
    "targetHost" : "foreignhost",
    "sourceIndex" : "myindex"
}
```

From database to file or vice versa you can use the following commands. Note that data files are now compressed by default. To disable this feature use additional flags:
```JavaScript
// Export to file from database
node exporter.js -a localhost -i index1 -t type1 -g filename

// Import from file to database
node exporter.js -f filename -b foreignhost -i index2 -t type2

// To override the compression for a given source file
node exporter.js -f filename -c false -b foreignhost -j index2 -u type2

// To override the compression for a target file
node exporter.js -a localhost -i index1 -t type1 -g filename -d false
```

The tool responds with a number of exit codes that might help determine what went wrong:

* ``` 0``` Operation successful / No documents found to export
* ``` 1``` invalid options
* ``` 2``` source or target database cluster health = red
* ```99``` Uncaught Exception

If memory is an issue pass these parameters and the process will try to run garbage collection before reaching memory limitations
```
node --nouse-idle-notification --expose-gc exporter.js ...
```

Or make use of the script in the tools folder:
```
tools/ex.sh ...
```

## Requirements

To run this script you will need at least node v0.10, as well as the nomnom, colors and through package installed (which will be installed automatically via npm).

## Installation

Run the following command in the directory where you want the tools installed

	npm install elasticsearch-exporter --production

The required packages will be installed automatically as a dependency, you won't have to do anything else to use the tool. If you install the package with the global flag (```npm -g```) there will also be a new executable available in the system called "eexport".

Notice that if you don't install released versions from npm and use the master instead, that it is in active development and might not be working. For help and questions you can always [file an issue](https://github.com/mallocator/Elasticsearch-Exporter/issues).

## Improving Performance

If you're trying to export a large amount of data it can take quite a while to export that data. Here are some tips that might help you speed up the process.

### Reduce Network Hops

In most cases the limiting resource when running the exporter has not been CPU or Memory, but Network IO and response time from ElasticSearch. In some cases it is possible to speed up the process by reducing network hops. The closer you can get to either the source or target database the better. Try running on one of the nodes to reduce latency. If you're running a larger cluster try to run the script on the node where most shards of the data are available. This will further prevent ElasticSearch to make internal hops.

### Increase Process Memory

In some cases the number of requests queue up filling up memory. When running with garbage collection enabled, the client will wait until memory has been freed if it should fill up, but this might also cause the entire process to take longer to finish. Instead you can try and increase the amount of memory that the node process has available. To set memory to a higher value just pass this option with your desired memory setting to the node executable: `--max-old-space-size=600`. Note that there is an upper limit to the amount a node process can receive, so at some point it doesn't make much sense to increase it any further.

### Increase Concurrent Request limit

It might be the case that your network connection can handle a lot more than is typical and that the script is spending the most time waiting for additional sockets to be free. To get around this you can increase the maximum number of sockets on the global http agent by using the option flag for it (`--maxSockets`). Increase this to see if it will improve anything.

### Split up into multiple Jobs

It might be possible to run the script multiple times in parallel. Since the exporter is single threaded it will only make use of one core and performance can be gained by querying ElasticSearch multiple times in parallel. To do so simple run the exporter tool against individual types or indexes instead of the entire cluster. If the bulk of your data is contained one type make use of the query parameter to further partition the existing data. Since it is necessary to understand the structure of the existing data it is not planned the exporter will attempt to do any of the optimization automatically.

### Export to file first

Sometimes the whole pipe from source to target cluster is simply slow, unstable and annoying. In such a case try to export to a local file first. This way you have a complete backup with all the data and can transfer this to the target machine. While this might overall take more time, it might increase the speed of the individual steps.

### Change the fetching size

It might help if you change the size of each scan request that fetches data. The current default of the option `--sourceSize` is set to 10. Increasing or decreasing this value might have great performance impact on the actual export.

### Optimizing the ElastichSearch Cluster

This tool will only run as fast as your cluster can keep up. If the nodes are under heavy load, errors can occur and the entire process will take longer. How to omptimize your Cluster is a whole other chapter and depends on the version of ElasticSearch that you're running. Try the official documentation, google or the ElasticSearch mailing groups for help. The topic is too complex for us to cover it here.

## Tests

To run the tests you must install the development dependencies along with the production dependencies

	npm install elasticsearch-exporter

After that you can just run ```npm test``` to see an output of all existing tests.

## Bugs and Feature Requests

I try to find all the bugs and have tests to cover all cases, but since I'm working on this project alone, it's easy to miss something.
Also I'm trying to think of new features to implement, but most of the time I add new features because someone asked me for it.
So please report any bugs or feature request to mallox@pyxzl.net or file an issue directly on [Github](https://github.com/mallocator/Elasticsearch-Exporter/issues).
Thanks!

## Changelog

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
* Support for ElasticSearch 1.0 (with autodetection of servers)
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
* Increased number of sockets used in es driver, so that pumping data should now be faster in many cases.

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
