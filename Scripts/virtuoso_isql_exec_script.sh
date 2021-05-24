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


file_from_arg=$1
filename="${file_from_arg%.*}"
mkdir -p outputs_$filename

cp "$current_path/Scripts/sparql_files/$1" outputs_$filename
#grep -o '^[^#]*' "$current_path/Scripts/sparql_files/$1" > outputs_$filename/$1

str_sparql=$(<outputs_$filename/$1)


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
SET BLOBS ON;
explain('SPARQL $str_sparql');
"

explain_order_loop="
SET BLOBS ON;
explain('SPARQL DEFINE sql:select-option \"order,loop\" $str_sparql');
"

profile_normal="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
SET BLOBS ON;
prof_enable(1);
profile('SPARQL $str_sparql');
prof_enable(0);
"

profile_order_loop="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
SET BLOBS ON;
prof_enable(1);
profile('SPARQL DEFINE sql:select-option \"order,loop\" $str_sparql');
prof_enable(0);
"


profile_loop="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
SET BLOBS ON;
prof_enable(1);
profile('SPARQL DEFINE sql:select-option \"loop\" $str_sparql');
prof_enable(0);
"


profile_con_translate="
SET SPARQL_TRANSLATE ON;
SET VERT_ROW_OUTPUT OFF;
SPARQL $str_sparql;
SET SPARQL_TRANSLATE OFF;
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
__dbf_set('enable_joins_only', 1);
SET BLOBS ON;
profile('SPARQL $str_sparql');
"

profile_con_translate_explain_bajo="
SET SPARQL_TRANSLATE ON;
SET VERT_ROW_OUTPUT OFF;
SPARQL $str_sparql;
SET SPARQL_TRANSLATE OFF;
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 0);
__dbf_set('enable_joins_only', 1);
SET BLOBS ON;
profile('SPARQL $str_sparql');
"
profile_con_translate_y_order_loop="
SET SPARQL_TRANSLATE ON;
SET VERT_ROW_OUTPUT OFF;
SPARQL $str_sparql;
SET SPARQL_TRANSLATE OFF;
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
__dbf_set('enable_joins_only', 1);
SET BLOBS ON;
profile('SPARQL DEFINE sql:select-option \"order,loop\" $str_sparql');
"

profile_con_translate_explain_bajo_y_order_loop="
SET SPARQL_TRANSLATE ON;
SET VERT_ROW_OUTPUT OFF;
SPARQL $str_sparql;
SET SPARQL_TRANSLATE OFF;
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 0);
__dbf_set('enable_joins_only', 1);
SET BLOBS ON;
profile('SPARQL DEFINE sql:select-option \"order,loop\" $str_sparql');
"

explain_parse_tree="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
dbg_obj_print('$1');
select dbg_obj_print(explain('sparql $str_sparql',-2));

"

echo "Executando isql";
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$sparql_execution"
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$sparql_execution" > outputs_$filename/sparql_execution_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_translate" > outputs_$filename/sparql_translate_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$explain_normal" > outputs_$filename/explain_normal_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$explain_order_loop" > outputs_$filename/explain_order_loop_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal" > outputs_$filename/profile_normal_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_loop" > outputs_$filename/profile_loop_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_order_loop" > outputs_$filename/profile_order_loop_file_$filename

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$profile_con_translate" > outputs_$filename/profile_con_translate_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$profile_con_translate_y_order_loop" > outputs_$filename/profile_con_translate_y_order_loop_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$profile_con_translate_explain_bajo" > outputs_$filename/profile_con_translate_explain_bajo_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$profile_con_translate_explain_bajo_y_order_loop" > outputs_$filename/profile_con_translate_explain_bajo_y_order_loop_$filename



sparql_translate_file=$(cat outputs_$filename/sparql_translate_file_$filename)

sparql_explain="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
SET EXPLAIN ON;
$sparql_translate_file;
SET EXPLAIN OFF;
"

sparql_profile="
__dbf_set('enable_qr_comment', 1);             
__dbf_set('dbf_explain_level', 3);
SET PROFILE ON;
SET BLOBS ON;
$sparql_translate_file;
SET PROFILE OFF;
"

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_explain" > outputs_$filename/sparql_explain_file_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_profile" > outputs_$filename/sparql_profile_file_$filename

echo "PARSE TREE";
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$explain_parse_tree"
