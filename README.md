Elasticsearch-Exporter
======================

A small script to export data from one Elasticsearch cluster into another.

While copying, the script will attempt to create the mapping from the source database, if it doesn't exist already.

The data that should be exported can now also be filtered via a query, so that if you only want to export a part of the data in your cluster, you can do that now.

# Usage

	// From one database to database
	node exporter.js -a localhost -b foreignhost		// copy all indices from machine a to b
	node exporter.js -i index1 -j index2		// copy entire index1 to index2 on same machine
	node exporter.js -i index -t type1 -u type2		// copy type1 to type2 in same index
	node exporter.js -i index1 -t type1 -j index2 -u type2		// copy type1 from index1 to type2 in index2
	node exporter.js -a localhost -i index1 -b foreignhost		// copy entire index1 from machine a to b
	node exporter.js -a localhost -i index1 -b foreignhost -j index2		// copy index1 from machine1 to index2 on machine2
	node exporter.js -a localhost -b foreignhost -s '{"bool":{"must":{"field":{"field1":"value1"}}}}'		// only copy stuff from machine1 to machine2, that is in the query
	node exporter.js -a localhost -b foreignhost -r true		// Do not execute any operation on machine2, just see the amount of data that would be queried
	
	// From database to file or vice versa
	node exports.js -a localhost -i index1 -t type1 -f filename
	node exports.js -g filename -b foreignhost -i index2 -t type2
    
    // If memory is an issue pass these parameters and the process will try to run garbage collection
    node --nouse-idle-notification --expose-gc exporter.js ...
    
    // Or make use of the script in the tools folder:
    tools/ex.sh ...

# Requirements

To run this script you will need at least node v0.10, as well as the nomnom package installed.

# Installation

	npm install git://github.com/mallocator/Elasticsearch-Exporter.git

# Changelog

## 1.1.3
* Fixed bug where sockets would wait forever to be released (thanks @dplate)
* Fixed bug where the last few documents were not written to target driver (thanks @sjost)
* Fixed bug where null was written to the target driver as first line

## 1.1.2
* Process will now observe available memory and wait for writes to go through before fetching more data (if gc is available).
* Removed check for target files (which was non sense)

## 1.1.1
* Fixed a bug that would prevent the script from terminating
* Added a test driver to figure out where certain problems are
* Pushed dependency on node from version 0.6 to 0.10 since we're using new stream implementation

## 1.1.0
* Now supports importing/exporting into files
* Refactored most code to be better maintainable
* Removed some old dependencies

## 1.0.0
* First working implementation for pumping data from on database to another (or the same)
