#!/bin/sh

CLUSTER=$(curl -XGET 'http://localhost:9200/' 2> /dev/null);

curl -XPUT 'http://localhost:9200/index1/' --data-binary '@setting.json'

if [[ ${CLUSTER} =~ \"number\"( )*:( )*\"0\.9[0-9]+\.[0-9]+\" ]]
then
    curl -XPUT 'http://localhost:9200/index1/type1/_mapping' --data-binary "@mapping.json";
else
    curl -XPUT 'http://localhost:9200/index1/_mapping/type1' --data-binary "@mapping.json";
fi