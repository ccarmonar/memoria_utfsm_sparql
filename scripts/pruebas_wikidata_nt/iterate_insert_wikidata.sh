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

mkdir -p rdf_deleted/rdf_del_"$1"
mkdir -p errors_files/errors_"$1"_parts



#arr_ignore=("aa ab abc acc adc aec afc ac ad ae af ag ah ai aj ak al am an ao ap aq ar")
arr_ignore=("xx")


for file in $current_path/*'.nt'; do
	x=$(basename ${file%%.*/} '.nt')
	arrX=(${x//parts/ })
	subsplit=${arrX[1]}
	iterar=1
	for i in ${arr_ignore[@]}
	do
		if [ "$i" == "$subsplit" ]; then
			iterar=0
		fi			
	done
		
	if [ $iterar == 1 ]; then
		echo "Initial Clean en $1,$subsplit"
		./clean_data.sh $1 $subsplit
		#echo "inster en wikidata en $1,$subsplit"
		#./insert_wikidata_split.sh $1 $subsplit
	fi	


	#if [[ " ${arr_ignore[*]} " -neq *"$subsplit"* ]]; then
	#	echo "Initial Clean en $1,$subsplit"
	#	#./clean_data.sh $1 $subsplit
	#	echo "inster en wikidata en $1,$subsplit"
	#	#./insert_wikidata_split.sh $1 $subsplit
	#fi
done



