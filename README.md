Elasticsearch-Exporter
======================

A small script to export data from one Elasticsearch cluster into another.

While copying, the script will attempt to create the mapping from the source database, if it doesn't exist already.

# Usage

	node exporter.js -a localhost -b foreignhost				// copy all indices from machine a to b
	node exporter.js -i index1 -j index2					// copy entire index1 to index2 on same machine
	node exporter.js -i index -t type1 -u type2				// copy type1 to type2 in same index
	node exporter.js -i index1 -t type1 -j index2 -u type2			// copy type1 from index1 to type2 in index2
	node exporter.js -a localhost -i index1 -b foreignhost			// copy entire index1 from machine a to b
	node exporter.js -a localhost -i index1 -b foreignhost -j index2	// copy index1 from machine1 to index2 on machine2

# Requirements

To run this script you will need at least node v0.6, as well as the nomnom and colors package installed.
