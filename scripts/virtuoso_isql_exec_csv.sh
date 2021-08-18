#!/bin/bash
system_password="161905";
current_path="/home/c161905/Memoria/memoria_utfsm_sparql/scripts"
path_virtuoso_isql=$VIRTUOSO7_isql;
path_virtuoso_db=$VIRTUOSO7_db;
path_virtuoso_sparql_path=$VIRTUOSO7_test_sparql;
isql_host="1111";
isql_username="dba";
isql_password="dba";



cd $current_path
mkdir -p outputs
cd outputs
file_from_arg=$1
filename="${file_from_arg%.*}"
mkdir -p outputs_$filename


cd $current_path
str_sparql=$(<temp/$1)
cp "$current_path/temp/$1" "$current_path/outputs/outputs_$filename"



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

ql_id="SELECT TOP 1 ql_id FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_start_dt="SELECT TOP 1 ql_start_dt  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_rt_msec="SELECT TOP 1 ql_rt_msec  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_rt_clocks="SELECT TOP 1 ql_rt_clocks  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_client_ip="SELECT TOP 1 ql_client_ip  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_user="SELECT TOP 1 ql_user  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_sqlstate="SELECT TOP 1 ql_sqlstate  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_error="SELECT TOP 1 ql_error  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"

ql_swap="SELECT TOP 1 ql_swap  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_user_cpu="SELECT TOP 1 ql_user_cpu  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_sys_cpu="SELECT TOP 1 ql_sys_cpu  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_params="SELECT TOP 1 ql_params  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_plan_hash="SELECT TOP 1 ql_plan_hash  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_c_clocks="SELECT TOP 1 ql_c_clocks  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_c_msec="SELECT TOP 1 ql_c_msec  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
#ql_c_disk="SELECT TOP 1 ql_c_disk  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"

ql_c_disk_reads="SELECT TOP 1 ql_c_disk_reads  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_c_disk_wait="SELECT TOP 1 ql_c_disk_wait  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_c_cl_wait="SELECT TOP 1 ql_c_cl_wait  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_cl_messages="SELECT TOP 1 ql_cl_messages  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_c_rnd_rows="SELECT TOP 1 ql_c_rnd_rows  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_rnd_rows="SELECT TOP 1 ql_rnd_rows  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_seq_rows="SELECT TOP 1 ql_seq_rows  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_same_seg="SELECT TOP 1 ql_same_seg  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_same_page="SELECT TOP 1 ql_same_page  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_same_parent="SELECT TOP 1 ql_same_parent  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_thread_clocks="SELECT TOP 1 ql_thread_clocks  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_disk_wait_clocks="SELECT TOP 1 ql_disk_wait_clocks  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_cl_wait_clocks="SELECT TOP 1 ql_cl_wait_clocks  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_pg_wait_clocks="SELECT TOP 1 ql_pg_wait_clocks  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_disk_reads="SELECT TOP 1 ql_disk_reads  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_spec_disk_reads="SELECT TOP 1 ql_spec_disk_reads  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_messages="SELECT TOP 1 ql_messages  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_message_bytes="SELECT TOP 1 ql_message_bytes  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_qp_threads="SELECT TOP 1 ql_qp_threads  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
#ql_vec_bytes="SELECT TOP 1 ql_vec_bytes  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
#ql_vec_bytes_max="SELECT TOP 1 ql_vec_bytes_max  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"

ql_memory="SELECT TOP 1 ql_memory  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_memory_max="SELECT TOP 1 ql_memory_max  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"

ql_lock_waits="SELECT TOP 1 ql_lock_waits  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_lock_wait_msec="SELECT TOP 1 ql_lock_wait_msec  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_node_stat="SELECT TOP 1 ql_node_stat  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_c_memory="SELECT TOP 1 ql_c_memory  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
ql_rows_affected="SELECT TOP 1 ql_rows_affected FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"






#ECHO ql_text '\n';
#SELECT TOP 1 ql_text  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';

#ECHO ql_plan '\n';
#SELECT TOP 1 ql_plan  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';





echo "Executando isql $1";


echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$sparql_translate" > outputs/outputs_$filename/sparql_translate_file_$filename

echo -e $system_password | sudo rm -rf "/usr/local/virtuoso-opensource/var/lib/virtuoso/db/virtuoso.qrl"

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal" > outputs/outputs_$filename/profile_normal_file_$filename

echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal_explain_bajo" > outputs/outputs_$filename/profile_normal_explain_bajo_$filename

echo "EXPORTANDO GENERAL FEATURES DE $1"



echo "ql_id" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_id" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_start_dt" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_start_dt" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_rt_msec" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_rt_msec" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_rt_clocks" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_rt_clocks" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_client_ip" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_client_ip" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_user" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_user" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_sqlstate" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_sqlstate" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_error" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_error" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_swap" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_swap" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_user_cpu" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_user_cpu" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_sys_cpu" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_sys_cpu" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_params" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_params" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_plan_hash" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_plan_hash" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_c_clocks" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_clocks" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_c_msec" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_msec" >> outputs/outputs_$filename/gfeatures_$filename

#echo "ql_c_disk" >> outputs/outputs_$filename/gfeatures_$filename
#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_disk" > outputs/outputs_$filename/gfeatures_$filename

echo "ql_c_disk_reads" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_disk_reads" >> outputs/outputs_$filename/gfeatures_$filename



echo "ql_c_disk_wait" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_disk_wait" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_c_cl_wait" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_cl_wait" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_cl_messages" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_cl_messages" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_c_rnd_rows" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_rnd_rows" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_rnd_rows" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_rnd_rows" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_seq_rows" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_seq_rows" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_same_seg" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_same_seg" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_same_page" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_same_page" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_same_parent" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_same_parent" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_thread_clocks" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_thread_clocks" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_disk_wait_clocks" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_disk_wait_clocks" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_cl_wait_clocks" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_cl_wait_clocks" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_pg_wait_clocks" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_pg_wait_clocks" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_disk_reads" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_disk_reads" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_spec_disk_reads" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_spec_disk_reads" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_messages" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_messages" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_message_bytes" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_message_bytes" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_qp_threads" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_qp_threads" >> outputs/outputs_$filename/gfeatures_$filename

#echo "ql_client_ip" >> outputs/outputs_$filename/gfeatures_$filename
#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_vec_bytes" > outputs/outputs_$filename/gfeatures_$filename
#echo "ql_client_ip" >> outputs/outputs_$filename/gfeatures_$filename
#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_vec_bytes_max" > outputs/outputs_$filename/gfeatures_$filename

echo "ql_memory" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_memory" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_memory_max" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_memory_max" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_lock_waits" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_lock_waits" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_lock_wait_msec" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_lock_wait_msec" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_node_stat" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_node_stat" >> outputs/outputs_$filename/gfeatures_$filename

echo "ql_c_memory" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_c_memory" >> outputs/outputs_$filename/gfeatures_$filename


echo "ql_rows_affected" >> outputs/outputs_$filename/gfeatures_$filename
echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$ql_rows_affected" >> outputs/outputs_$filename/gfeatures_$filename













#isql-vt VERBOSE=OFF BANNER=OFF exec="select ll_file,ll_state,ll_started,ll_done from DB.DBA.load_list;" > /media/data/ccarmona/virtuoso6_ini/var/lib/dir_allowed/latest-truthy/test.txt

#echo "PARSE TREE";
#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password exec="$explain_parse_tree"

#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal" > sparql_profiles/profile_normal_file_$filename
#echo -e $system_password | sudo -S $path_virtuoso_isql $isql_host $isql_username $isql_password VERBOSE=OFF BANNER=OFF exec="$profile_normal_explain_bajo" > sparql_profiles/profile_normal_explain_bajo_$filename
