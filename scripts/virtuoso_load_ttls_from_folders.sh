#!/bin/bash

system_password="161905";
current_path="/home/c161905/Memoria/memoria_utfsm_sparql"
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";



cd $path_virtuoso_db/learning_sparql_ttl
for FILE in *; 
do 

filename="$FILE"

exec="
DB.DBA.TTLP_MT (file_to_string_output ('learning_sparql_ttl/$filename'), '', 'http://www.learningsparql.com/second_edition/');
"

echo $exec

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$exec"
done




#echo "--------------------------------------";
#echo "Executando isql";
#echo "--------------------------------------";
#