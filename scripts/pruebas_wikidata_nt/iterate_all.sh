#!/bin/bash

system_password="161905";
current_path=$VIRTUOSO7_db"/latest-truthy-nt/split_"$1"_parts";
error_path="errors_files/errors_"$1"_parts"
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";

arr=("aa ab")
for i in ${arr[@]}
do
	echo "$i"
	./iterate_insert_wikidata.sh "$i"
done

shutdown 5
