#!/bin/bash

system_password="161905";
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";

str_sparql='select distinct ?Concept where {[] a ?Concept} LIMIT 100'

sparql_translate="
SET BLOBS ON;
SET SPARQL_TRANSLATE ON;
SET VERT_ROW_OUTPUT OFF;
SPARQL $str_sparql;
SET SPARQL_TRANSLATE OFF;
"

explain_normal="

explain('SPARQL $str_sparql');
"

profile_normal="
SET BLOBS ON;
profile('SPARQL $str_sparql');
"

echo "Executando isql";
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_translate" > sparql_translate_file.txt
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$explain_normal" > explain_normal_file.txt
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal" > profile_normal_file.txt


sparql_translate_file=$(cat sparql_translate_file.txt)

sparql_explain="
SET EXPLAIN ON;
$sparql_translate_file;
SET EXPLAIN OFF;
"

sparql_profile="
SET PROFILE ON;
$sparql_translate_file;
SET PROFILE OFF;
"

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_explain" > sparql_explain_file.txt
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_profile" > sparql_profile_file.txt


