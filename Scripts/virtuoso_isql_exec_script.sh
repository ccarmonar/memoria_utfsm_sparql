#!/bin/bash
system_password="161905";
current_path="/home/c161905/Memoria/memoria_utfsm_sparql"
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";

str_sparql_file=$(<sparql_files/$1)



if [[ ! -f "sparql_files/$1" ]] ; then
    echo 'File is not there, aborting.'
    exit
fi

mkdir -p outputs
cd outputs



#str_sparql='select distinct ?Concept where {[] a ?Concept} LIMIT 100'
str_sparql=$str_sparql_file


file_from_arg=$1
filename="${file_from_arg%.*}"
mkdir -p outputs_$filename
cp "$current_path/Scripts/sparql_files/$1" outputs_$filename

sparql_execution="
SPARQL $str_sparql;
"
echo "$sparql_execution"

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
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$sparql_execution"
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$sparql_execution" > outputs_$filename/sparql_execution_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_translate" > outputs_$filename/sparql_translate_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$explain_normal" > outputs_$filename/explain_normal_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal" > outputs_$filename/profile_normal_file_$filename


sparql_translate_file=$(cat outputs_$filename/sparql_translate_file_$filename)

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

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_explain" > outputs_$filename/sparql_explain_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_profile" > outputs_$filename/sparql_profile_file_$filename


