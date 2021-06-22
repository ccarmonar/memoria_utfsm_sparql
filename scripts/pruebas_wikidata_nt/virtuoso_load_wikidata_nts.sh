#!/bin/bash

system_password="161905";
current_path=$VIRTUOSO7_db"/latest-truthy-nt/split_"$1"_parts";
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";


exec="DB.DBA.TTLP_MT (file_to_string_output ('latest-truthy-nt/split_"$1"_parts/split_"$1"_parts"$2".nt'), '', 'http://wikidata.org');"
#echo $exec
echo "split $1 part $2"



echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$exec" &> errors_files/errors_"$1"_parts/error_"$1"_parts"$2" 

