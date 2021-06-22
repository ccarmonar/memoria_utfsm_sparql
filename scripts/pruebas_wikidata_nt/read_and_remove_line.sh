#!/bin/bash

system_password="161905";
current_path=$VIRTUOSO7_db"/latest-truthy-nt/split_"$1"_parts";
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";

delete_file="$(($3-1))"



echo -e $system_password | sudo -S sed $delete_file'!d' "$current_path/split_"$1"_parts"$2".nt" >> rdf_deleted/rdf_del_"$1"/rdf_del_"$1"_"$2".txt
echo "Agregada en rdf_deleted/rdf_del_"$1"/rdf_del_"$1"_"$2".txt"
echo -e $system_password | sudo -S sed -i ""$delete_file"d" "$current_path/split_"$1"_parts"$2".nt"
echo "Removida en split_"$1"_parts"$2".nt linea $3"
