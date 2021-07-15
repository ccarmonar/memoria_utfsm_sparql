#!/bin/bash

system_password="161905";
current_path="/home/c161905/Memoria/memoria_utfsm_sparql/scripts"
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";


for file in $current_path'/temp/'*'.rq'; do
  ./virtuoso_isql_exec_csv.sh ${file##*/}
done
