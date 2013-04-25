Elasticsearch-Exporter
======================

A small script to export data from one Elasticsearch cluster into another.

While copying, the script will attempt to create the mapping from the source database, if it doesn't exist already.

The data that should be exported can now also be filtered via a query, so that if you only want to export a part of the data in your cluster, you can do that now.

# Usage

	node exporter.js -a localhost -b foreignhost				// copy all indices from machine a to b
	node exporter.js -i index1 -j index2					// copy entire index1 to index2 on same machine
	node exporter.js -i index -t type1 -u type2				// copy type1 to type2 in same index
	node exporter.js -i index1 -t type1 -j index2 -u type2			// copy type1 from index1 to type2 in index2
	node exporter.js -a localhost -i index1 -b foreignhost			// copy entire index1 from machine a to b
	node exporter.js -a localhost -i index1 -b foreignhost -j index2	// copy index1 from machine1 to index2 on machine2
    node exporter.js -a localhost -b foreignhost -s '{"bool":{"must":{"field":{"field1":"value1"}}}}'       // only copy stuff from machine1 to machine2, that is in the query
    node exporter.js -a localhost -b foreignhost -r true        // Do not execute any operation on machine2, just see the amount of data that would be queried

# Requirements

To run this script you will need at least node v0.6, as well as the nomnom and colors package installed.

# Installation

npm install git://github.com/mallocator/Elasticsearch-Exporter.git

# Changelog

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