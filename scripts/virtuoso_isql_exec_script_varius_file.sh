#!/bin/bash
pwd=$(pwd)

sparql_folder=$pwd'/sparql_files/'
sparql_folder_wikidata=$pwd'/sparql_files/wikidata_queries/'





if [ "$1" == "wd" ]; then

    echo 'ONLY WIKIDATA'
    for file in $sparql_folder_wikidata*'.rq'; do
      ./virtuoso_isql_exec_script.sh ${file##*/} wd
    done

    else

    for file in $sparql_folder*'.rq'; do
      ./virtuoso_isql_exec_script.sh ${file##*/}
    done
    echo 'WIKIDATA'
    for file in $sparql_folder_wikidata*'.rq'; do
      ./virtuoso_isql_exec_script.sh ${file##*/} wd
    done

fi