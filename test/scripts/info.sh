#!/bin/sh

curl -XGET 'http://localhost:9200/'
echo Warmer
curl -XGET 'http://localhost:9200/index1/_warmer/'
echo
echo Mapping
curl -XGET 'http://localhost:9200/_mapping'
echo
echo Setting
curl -XGET 'http://localhost:9200/index1/_settings'
echo
