#!/bin/sh

CLUSTER=$(curl -XGET 'http://localhost:9200/' 2> /dev/null);

if [[ ${CLUSTER} =~ \"number\"( )*:( )*\"0\.9[0-9]+\.[0-9]+\" ]]
then
    curl -XPOST 'http://localhost:9200/_aliases/' --data-binary '@alias.0.9.json'
    curl -XPUT 'http://localhost:9200/index1/' --data-binary '@index.0.9.json'
else
    curl -XPUT 'http://localhost:9200/index1/' --data-binary '@index.1.4.json'
fi