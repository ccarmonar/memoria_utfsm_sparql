#!/bin/bash

system_password="161905";
current_path=$VIRTUOSO7_db"/latest-truthy-nt/split_"$1"_parts";
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";



echo -e $system_password | sudo -S grep -F "^^<http://www.opengis.net/ont/geosparql#wktLiteral> ." "$current_path/split_"$1"_parts"$2".nt" >> rdf_deleted/rdf_del_"$1"/rdf_del_"$1"_"$2".txt
echo -e $system_password | sudo -S grep --invert-match -F "^^<http://www.opengis.net/ont/geosparql#wktLiteral> ." "$current_path/split_"$1"_parts"$2".nt" >> temp_$1_$2 
echo -e $system_password | sudo -S mv temp_$1_$2 "$current_path/split_"$1"_parts"$2".nt"

