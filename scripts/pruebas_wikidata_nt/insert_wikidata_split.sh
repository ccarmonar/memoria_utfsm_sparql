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


./virtuoso_load_wikidata_nts.sh $1 $2

error_path=$(<$error_path/error_$1_parts$2)
string_error_path=($error_path)

if [[ "$error_path" == *"Error"* ]]; then
	if [[ "$error_path" == *"42000: [Virtuoso Driver][Virtuoso Server]TURTLE RDF loader, line "* ]]; then
		if [[ "$error_path" == *"DB.DBA.TTLP_MT (file_to_string_output ('latest-truthy-nt/split_$1_parts/split_$1_parts$2.nt'), '', 'http://wikidata.org')"* ]]; then
			for i in "${!string_error_path[@]}"
			do
				if [ "${string_error_path[i]}" == "loader," ] && [ "${string_error_path[i+1]}" == "line" ]; then
	    				aux="${string_error_path[i+2]}"
					remove_line="${aux::-1}"
					echo "Error en linea: "$remove_line
					./read_and_remove_line.sh $1 $2 $remove_line
					./$0 $1 $2 not_clean
				fi
			done
	
		fi
	fi	
fi

