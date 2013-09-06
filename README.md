#Elasticsearch-Exporter

A small script to export data from one Elasticsearch cluster into another.

Features:
* Node.js based command line tool
* Export to ElasticSearch or flat files
* Recreates mapping on target
* Source data can be filtered by query
* Specify scope as type, index or whole cluster
* Run in test mode without modifying any data

## Usage

From one database to another database
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
node exporter.js -a localhost -b foreignhost -s '{"bool":{"must":{"field":{"field1":"value1"}}}}'

// Do not execute any operation on machine2, just see the amount of data that would be queried
node exporter.js -a localhost -b foreignhost -r true
```

From database to file or vice versa
```JavaScript
// Export to file from database
node exports.js -a localhost -i index1 -t type1 -g filename

// Import from file to database
node exports.js -f filename -b foreignhost -i index2 -t type2
```    

If memory is an issue pass these parameters and the process will try to run garbage collection before reaching memory limitations
```
node --nouse-idle-notification --expose-gc exporter.js ...
```

Or make use of the script in the tools folder:
```
tools/ex.sh ...
```

## Requirements

To run this script you will need at least node v0.10, as well as the nomnom package installed (which will be installed automatically via npm).

## Installation

Run the following command in the directory where you want the tools installed

	npm install git://github.com/mallocator/Elasticsearch-Exporter.git
	
The nom-nom package will be installed automatically as a dependency, you won't have to do anything else to use the tool. If you install the package with the global flag (npm -g) there will also be a new executable available in the system called "eexport".

## Changelog

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
