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
file_from_arg=$1
filename="${file_from_arg%.*}"
mkdir -p outputs_$filename
cd ..

if [ "$2" == "wd" ]; then
    echo "wikidata_file"
    str_prefixes_file=$(<sparql_files/wikidata_prefixes)
    str_sparql=$(<sparql_files/$1)
    str_sparql=$str_prefixes_file$str_sparql
    cp "$current_path/scripts/sparql_files/$1" "outputs/outputs_$filename"
    cat "sparql_files/wikidata_prefixes" "sparql_files/$1" > "outputs/outputs_$filename/$filename-prex.rq"
    else
    str_sparql=$(<sparql_files/$1)
    cp "$current_path/scripts/sparql_files/$1" "outputs/outputs_$filename"
fi


sparql_translate="
SET BLOBS ON;
SET SPARQL_TRANSLATE ON;
SET VERT_ROW_OUTPUT OFF;
SPARQL $str_sparql;
SET SPARQL_TRANSLATE OFF;
"

profile_normal="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
SET BLOBS ON;
profile('SPARQL $str_sparql');
"

profile_normal_explain_bajo="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 0);
SET BLOBS ON;
profile('SPARQL $str_sparql');
"

explain_parse_tree="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
dbg_obj_print('$1');
select dbg_obj_print(explain('sparql $str_sparql',-2));
"

echo "Executando isql $1";
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_translate" > outputs/outputs_$filename/sparql_translate_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal" > outputs/outputs_$filename/profile_normal_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal_explain_bajo" > outputs/outputs_$filename/profile_normal_explain_bajo_$filename

#echo "PARSE TREE";
#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$explain_parse_tree"