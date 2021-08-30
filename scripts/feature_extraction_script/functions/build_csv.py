import pandas as pd
from functions.general_features import GeneralFeaturesFromProfileFile, GeneralFeaturesFromPerformanceTuning, GeneralFeaturesFromOperators
from functions.matrix_format import MatrixFormat, MatrixNumpyFormat, DataFrameFormat
from functions.tree_format import IdentifyJoinType, OnlyScans, IterateBuildTree, BinaryTreeFormat
from functions.aux import HashStringId


def AllData(operators, profile, predicates, filename, sparql_file, general_features_pt_file):
    matrix_format = MatrixFormat(operators, predicates)
    binary_tree = BinaryTreeFormat(operators, matrix_format)
    general_features = GeneralFeaturesFromProfileFile(profile, operators)
    unique_id = HashStringId(str(predicates) + str(matrix_format) + str(binary_tree) + str(general_features))
    limit = general_features['GENERAL_FEATURES']['LIMIT']
    precompiled_list = list(general_features['GENERAL_FEATURES']['precompiled'].values())
    compiled_list = list(general_features['GENERAL_FEATURES']['compiled'].values())
    general_features_pt = GeneralFeaturesFromPerformanceTuning(general_features_pt_file)
    all_data = [unique_id, filename, sparql_file, profile, limit] + precompiled_list + compiled_list + general_features_pt
    all_data.append(str(matrix_format))
    all_data.append(str(binary_tree))
    operators = GeneralFeaturesFromOperators(operators)
    return all_data


def FullDataframe(list_of_features):
    columns= [
        ## general features - others
        'unique_id',
        'filename',
        'sparql_file',
        'profile',
        'limit',
        ## general features - precompiled
        'msec',
        'cpu_p',
        'rnd',
        'seq',
        'same_seg_p',
        'same_page_p',
        'disk_reads',
        'read_ahead',
        'wait',
        ## general features - compiled list
        'comp_msec',
        'comp_reads',
        'comp_read_p',
        'comp_messages',
        'comp_clw',
        ##general_features_from_file
        'ql_id',
        'ql_start_dt',
        'ql_rt_msec',
        'ql_rt_clocks',
        'ql_client_ip',
        'ql_user',
        'ql_sqlstate',
        'ql_error',
        'ql_swap',
        'ql_user_cpu',
        'ql_sys_cpu',
        'ql_params',
        'ql_plan_hash',
        'ql_c_clocks',
        'ql_c_msec',
        # ql_c_disk="SELECT TOP 1 ql_c_disk  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        'ql_c_disk_reads',
        'ql_c_disk_wait',
        'ql_c_cl_wait',
        'ql_cl_messages',
        'ql_c_rnd_rows',
        'ql_rnd_rows',
        'ql_seq_rows',
        'ql_same_seg',
        'ql_same_page',
        'ql_same_parent',
        'ql_thread_clocks',
        'ql_disk_wait_clocks',
        'ql_cl_wait_clocks',
        'ql_pg_wait_clocks',
        'ql_disk_reads',
        'ql_spec_disk_reads',
        'ql_messages',
        'ql_message_bytes',
        'ql_qp_threads',
        # ql_vec_bytes="SELECT TOP 1 ql_vec_bytes  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        # ql_vec_bytes_max="SELECT TOP 1 ql_vec_bytes_max  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        'ql_memory',
        'ql_memory_max',
        'ql_lock_waits',
        'ql_lock_wait_msec',
        'ql_node_stat',
        'ql_c_memory',
        'ql_rows_affected',
        # matrix and binary tree
        'matrix_format',
        'binary_tree'
    ]
    final_df = pd.DataFrame(list_of_features, columns=columns)
    return final_df




