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

#cp "$current_path/Scripts/sparql_files/$1" outputs_$filename
grep -o '^[^#]*' "$current_path/Scripts/sparql_files/$1" > outputs_$filename/$1