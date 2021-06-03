#!/bin/bash
pwd=$(pwd)
SPARQLFILE=$pwd'/filenames.txt'
SPARQLWIKIDATAFILE=$pwd'/filenames_wikidata.txt'



cat $SPARQLFILE | while read line
do
   ./virtuoso_isql_exec_script.sh $line'.rq'
done

cat $SPARQLWIKIDATAFILE | while read line
do
   ./virtuoso_isql_exec_script.sh $line'.rq' wd
done
