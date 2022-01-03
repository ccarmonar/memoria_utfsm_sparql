import pandas as pd, ast, json
from functions.general_features import GeneralFeaturesFromProfileFile, GeneralFeaturesFromPerformanceTuning, GeneralFeaturesFromScan, \
    GetJsonPredicatesFeatures, GeneralFeaturesFromOperatorsAndSparqlFile
from functions.matrix_format import MatrixFormat, MatrixNumpyFormat, DataFrameFormat, MatrixFormat_subtrees
from functions.tree_format import TreeFormat, TreeFormat_old_format
from functions.aux import HashStringId, OnlyScans, OnlyScansAsList


def AllData(operators, profile, predicates, filename, sparql_file, general_features_pt_file, list_alleq, old_features_json, symbol,new=True):
    matrix_format = MatrixFormat(operators, predicates)
    general_features = GeneralFeaturesFromProfileFile(profile, operators)
    limit = general_features['GENERAL_FEATURES']['LIMIT']
    precompiled_list = list(general_features['GENERAL_FEATURES']['precompiled'].values())
    compiled_list = list(general_features['GENERAL_FEATURES']['compiled'].values())
    #total_time = general_features['GENERAL_FEATURES']['time']
    #general_features_pt = GeneralFeaturesFromPerformanceTuning(general_features_pt_file)
    operators = GeneralFeaturesFromScan(operators, list_alleq)
    if not new:
        old_trees = ast.literal_eval(old_features_json)['trees']
        try:
            operators['GF_FROM_OP']['trees_daniel'] = json.loads(old_trees)
        except:
            print("ERROR EN EL JSON LOAD, SE CARGARA COMO STR")
            operators['GF_FROM_OP']['trees_daniel'] = old_trees
    binary_tree, operators, subtrees, num_bgp_subtree = TreeFormat(operators, sparql_file, symbol)
    binary_tree_old, operators, subtrees_old = TreeFormat_old_format(operators, sparql_file, symbol)
    triples = general_features['GF_FROM_OP']['triples']
    total_bgps = general_features['GF_FROM_OP']['total_bgps']
    treesize = general_features['GF_FROM_OP']['treesize']
    JsonPredicatesFeatures = GetJsonPredicatesFeatures(operators)
    json_time_predicate = operators['GF_FROM_OP']['json_time_predicate']
    json_fanout_predicate = operators['GF_FROM_OP']['json_fanout_predicate']
    json_input_rows_predicate = operators['GF_FROM_OP']['json_input_rows_predicate']
    json_cardinality_fanout = operators['GF_FROM_OP']['json_cardinality_fanout']
    json_cardinality = operators['GF_FROM_OP']['json_cardinality']
    scan_queries = OnlyScansAsList(operators, True)
    bgps = operators['GF_FROM_OP']['bgps_ops']
    unique_id = HashStringId(str(predicates) + str(matrix_format) + str(binary_tree) + str(general_features))
    #all_data = [unique_id, filename, sparql_file, profile, limit] + precompiled_list + compiled_list + general_features_pt + list(ast.literal_eval(old_features_json).values())
    operators = GeneralFeaturesFromOperatorsAndSparqlFile(operators, sparql_file)




    group_by = operators['GF_FROM_OP']['group_by']
    distinct = operators['GF_FROM_OP']['distinct']
    order_by = operators['GF_FROM_OP']['order_by']
    union = operators['GF_FROM_OP']['union']
    #left_join = operators['GF_FROM_OP']['left_join']
    #join = operators['GF_FROM_OP']['join']
    left_join = str(binary_tree).replace('"', ';').replace("'", '"').count('JOIN')
    join = str(binary_tree).replace('"', ';').replace("'", '"').count('JOIN') - left_join
    operators['GF_FROM_OP']['left_join'] = left_join
    operators['GF_FROM_OP']['join'] = join
    iter = operators['GF_FROM_OP']['iter']
    filter = operators['GF_FROM_OP']['filter']
    num_filter = operators['GF_FROM_OP']['num_filter']
    filter_eq = operators['GF_FROM_OP']['filter_eq']
    filter_gt = operators['GF_FROM_OP']['filter_gt']
    filter_ge = operators['GF_FROM_OP']['filter_ge']
    filter_lt = operators['GF_FROM_OP']['filter_lt']
    filter_le = operators['GF_FROM_OP']['filter_le']
    filter_neq = operators['GF_FROM_OP']['filter_neq']
    filter_iri = operators['GF_FROM_OP']['filter_iri']
    filter_neq = operators['GF_FROM_OP']['filter_neq']
    filter_bound = operators['GF_FROM_OP']['filter_bound']
    filter_contains = operators['GF_FROM_OP']['filter_contains']
    filter_exists = operators['GF_FROM_OP']['filter_exists']
    filter_isBlank = operators['GF_FROM_OP']['filter_isBlank']
    filter_isIRI = operators['GF_FROM_OP']['filter_isIRI']
    filter_isLiteral = operators['GF_FROM_OP']['filter_isLiteral']
    filter_lang = operators['GF_FROM_OP']['filter_lang']
    filter_langMatches = operators['GF_FROM_OP']['filter_langMatches']
    filter_not = operators['GF_FROM_OP']['filter_not']
    filter_notexists = operators['GF_FROM_OP']['filter_notexist']
    filter_regex = operators['GF_FROM_OP']['filter_regex']
    filter_sameTerm = operators['GF_FROM_OP']['filter_sameTerm']
    filter_str = operators['GF_FROM_OP']['filter_str']
    filter_strstarts = operators['GF_FROM_OP']['filter_strstarts']
    filter_or = operators['GF_FROM_OP']['filter_or']
    filter_and = operators['GF_FROM_OP']['filter_and']

    matrix_subtrees = MatrixFormat_subtrees(operators, subtrees, operators['GENERAL_FEATURES']['precompiled']['msec'], num_bgp_subtree)
    operators['GF_FROM_OP']['matrix_subtrees'] = matrix_subtrees
    matrix_subtrees_old = MatrixFormat_subtrees(operators, subtrees_old, operators['GENERAL_FEATURES']['precompiled']['msec'], num_bgp_subtree)
    operators['GF_FROM_OP']['matrix_subtrees_old'] = matrix_subtrees_old
    features_list = [unique_id,
                    filename,
                    sparql_file,
                    profile,
                    limit,
                    group_by,
                    distinct,
                    order_by,
                    union,
                    left_join,
                    join,
                    iter,
                    filter,
                    num_filter,
                    filter_eq,
                    filter_gt,
                    filter_ge,
                    filter_lt,
                    filter_le,
                    filter_neq,
                    filter_iri,
                    filter_neq,
                    filter_bound,
                    filter_contains,
                    filter_exists,
                    filter_isBlank,
                    filter_isIRI,
                    filter_isLiteral,
                    filter_lang,
                    filter_langMatches,
                    filter_not,
                    filter_notexists,
                    filter_regex,
                    filter_sameTerm,
                    filter_str,
                    filter_strstarts,
                    filter_or,
                    filter_and]

    if new:
        all_data = features_list + precompiled_list + compiled_list
    else:
        all_data = features_list + precompiled_list + compiled_list + list(ast.literal_eval(old_features_json).values())
    all_data.append(triples)
    all_data.append(total_bgps)
    all_data.append(treesize)
    all_data.append(str(matrix_format))
    all_data.append(str(binary_tree).replace('"', ';').replace("'", '"'))
    all_data.append(str(binary_tree_old).replace('"', ';').replace("'", '"'))
    all_data.append(str(json_time_predicate).replace('"', ';').replace("'", '"'))
    all_data.append(str(json_fanout_predicate).replace('"', ';').replace("'", '"'))
    all_data.append(str(json_input_rows_predicate).replace('"', ';').replace("'", '"'))
    all_data.append(str(json_cardinality_fanout).replace('"', ';').replace("'", '"'))
    all_data.append(str(json_cardinality).replace('"', ';').replace("'", '"'))
    all_data.append(scan_queries)
    all_data.append(bgps)
    all_data.append(str(matrix_subtrees).replace('"', ';').replace("'", '"'))
    all_data.append(str(matrix_subtrees_old).replace('"', ';').replace("'", '"'))
    return all_data


def FullDataframe(list_of_features):
    columns= [
        ## general features - others
        'unique_id',
        'filename',
        'query', #sparql_file
        'profile',
        'limit',
        'group_by',
        'distinct',
        'order_by',
        'union',
        'left_join',
        'join',
        'iter',
        'filter',
        'num_filter',
        'filter_eq',
        'filter_gt',
        'filter_ge',
        'filter_lt',
        'filter_le',
        'filter_neq',
        'filter_iri',
        'filter_neq',
        'filter_bound',
        'filter_contains',
        'filter_exists',
        'filter_isBlank',
        'filter_isIRI',
        'filter_isLiteral',
        'filter_lang',
        'filter_langMatches',
        'filter_not',
        'filter_notexists',
        'filter_regex',
        'filter_sameTerm',
        'filter_str',
        'filter_strstarts',
        'filter_or',
        'filter_and',
        ## general features - precompiled
        'time',
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
        #'ql_id',
        #'ql_start_dt',
        #'ql_rt_msec',
        #'ql_rt_clocks',
        #'ql_client_ip',
        #'ql_user',
        #'ql_sqlstate',
        #'ql_error',
        #'ql_swap',
        #'ql_user_cpu',
        #'ql_sys_cpu',
        #'ql_params',
        #'ql_plan_hash',
        #'ql_c_clocks',
        #'ql_c_msec',
        # ql_c_disk="SELECT TOP 1 ql_c_disk  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        #'ql_c_disk_reads',
        #'ql_c_disk_wait',
        #'ql_c_cl_wait',
        #'ql_cl_messages',
        #'ql_c_rnd_rows',
        #'ql_rnd_rows',
        #'ql_seq_rows',
        #'ql_same_seg',
        #'ql_same_page',
        #'ql_same_parent',
        #'ql_thread_clocks',
        #'ql_disk_wait_clocks',
        #'ql_cl_wait_clocks',
        #'ql_pg_wait_clocks',
        #'ql_disk_reads',
        #'ql_spec_disk_reads',
        #'ql_messages',
        #'ql_message_bytes',
        #'ql_qp_threads',
        # ql_vec_bytes="SELECT TOP 1 ql_vec_bytes  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        # ql_vec_bytes_max="SELECT TOP 1 ql_vec_bytes_max  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        #'ql_memory',
        #'ql_memory_max',
        #'ql_lock_waits',
        #'ql_lock_wait_msec',
        #'ql_node_stat',
        #'ql_c_memory',
        #'ql_rows_affected',
        ### OLD FEATURES
        #'id_old', 'query_old', 'time_old', 'assign_old', 'bgp_old', 'distinct_old', 'extend_old', 'filter_old',
        #'filter_bound_old', 'filter_contains_old', 'filter_eq_old', 'filter_exists_old', 'filter_ge_old',
        #'filter_gt_old', 'filter_isBlank_old', 'filter_isIRI_old', 'filter_isLiteral_old', 'filter_lang_old',
        #'filter_langMatches_old', 'filter_le_old', 'filter_lt_old', 'filter_ne_old', 'filter_not_old',
        #'filter_notexists_old', 'filter_or_old', 'filter_regex_old', 'filter_sameTerm_old', 'filter_str_old',
        #'filter_strends_old', 'filter_strstarts_old', 'filter_subtract_old', 'graph_old', 'group_old', 'has_slice_old',
        #'join_old', 'json_cardinality_old', 'leftjoin_old', 'max_slice_limit_old', 'max_slice_start_old', 'minus_old',
        #'multi_old', 'notoneof_old', 'order_old', 'path*_old', 'path+_old', 'path?_old', 'pathN*_old', 'pathN+_old',
        #'pcs0_old', 'pcs1_old', 'pcs10_old', 'pcs11_old', 'pcs12_old', 'pcs13_old', 'pcs14_old', 'pcs15_old',
        #'pcs16_old', 'pcs17_old', 'pcs18_old', 'pcs19_old', 'pcs2_old', 'pcs20_old', 'pcs21_old', 'pcs22_old',
        #'pcs23_old', 'pcs24_old', 'pcs3_old', 'pcs4_old', 'pcs5_old', 'pcs6_old', 'pcs7_old', 'pcs8_old', 'pcs9_old',
        #'project_old', 'reduced_old', 'sequence_old', 'slice_old', 'tolist_old', 'top_old', 'tree_tdb_old', 'trees_old',
        #'treesize_old', 'triple_old', 'union_old', 'query_name_old',
        # matrix and binary tree
        'triples',
        'total_bgps',
        'treesize',
        'matrix_format',
        'trees',
        'trees_old_format',
        'json_time_predicate',
        'json_fanout_predicate',
        'json_input_rows_predicate',
        'json_cardinality_fanout',
        'json_cardinality',
        'scan_queries',
        'bgps',
        'matrix_subtrees',
        'matrix_subtrees_full'
    ]
    final_df = pd.DataFrame(list_of_features, columns=columns)
    return final_df


def FullDataframe_old(list_of_features):
    columns= [
        ## general features - others
        'unique_id',
        'filename',
        'query', #sparql_file
        'profile',
        'limit',
        'group_by',
        'distinct',
        'order_by',
        'union',
        'left_join',
        'join',
        'iter',
        'filter',
        'num_filter',
        'filter_eq',
        'filter_gt',
        'filter_ge',
        'filter_lt',
        'filter_le',
        'filter_neq',
        'filter_iri',
        'filter_neq',
        'filter_bound',
        'filter_contains',
        'filter_exists',
        'filter_isBlank',
        'filter_isIRI',
        'filter_isLiteral',
        'filter_lang',
        'filter_langMatches',
        'filter_not',
        'filter_notexists',
        'filter_regex',
        'filter_sameTerm',
        'filter_str',
        'filter_strstarts',
        'filter_or',
        'filter_and',
        ## general features - precompiled
        'time',
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
        #'ql_id',
        #'ql_start_dt',
        #'ql_rt_msec',
        #'ql_rt_clocks',
        #'ql_client_ip',
        #'ql_user',
        #'ql_sqlstate',
        #'ql_error',
        #'ql_swap',
        #'ql_user_cpu',
        #'ql_sys_cpu',
        #'ql_params',
        #'ql_plan_hash',
        #'ql_c_clocks',
        #'ql_c_msec',
        # ql_c_disk="SELECT TOP 1 ql_c_disk  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        #'ql_c_disk_reads',
        #'ql_c_disk_wait',
        #'ql_c_cl_wait',
        #'ql_cl_messages',
        #'ql_c_rnd_rows',
        #'ql_rnd_rows',
        #'ql_seq_rows',
        #'ql_same_seg',
        #'ql_same_page',
        #'ql_same_parent',
        #'ql_thread_clocks',
        #'ql_disk_wait_clocks',
        #'ql_cl_wait_clocks',
        #'ql_pg_wait_clocks',
        #'ql_disk_reads',
        #'ql_spec_disk_reads',
        #'ql_messages',
        #'ql_message_bytes',
        #'ql_qp_threads',
        # ql_vec_bytes="SELECT TOP 1 ql_vec_bytes  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        # ql_vec_bytes_max="SELECT TOP 1 ql_vec_bytes_max  FROM sys_query_log WHERE qrl_file = 'virtuoso.qrl';"
        #'ql_memory',
        #'ql_memory_max',
        #'ql_lock_waits',
        #'ql_lock_wait_msec',
        #'ql_node_stat',
        #'ql_c_memory',
        #'ql_rows_affected',
        ### OLD FEATURES
        'id_old', 'query_old', 'time_old', 'assign_old', 'bgp_old', 'distinct_old', 'extend_old', 'filter_old',
        'filter_bound_old', 'filter_contains_old', 'filter_eq_old', 'filter_exists_old', 'filter_ge_old',
        'filter_gt_old', 'filter_isBlank_old', 'filter_isIRI_old', 'filter_isLiteral_old', 'filter_lang_old',
        'filter_langMatches_old', 'filter_le_old', 'filter_lt_old', 'filter_ne_old', 'filter_not_old',
        'filter_notexists_old', 'filter_or_old', 'filter_regex_old', 'filter_sameTerm_old', 'filter_str_old',
        'filter_strends_old', 'filter_strstarts_old', 'filter_subtract_old', 'graph_old', 'group_old', 'has_slice_old',
        'join_old', 'json_cardinality_old', 'leftjoin_old', 'max_slice_limit_old', 'max_slice_start_old', 'minus_old',
        'multi_old', 'notoneof_old', 'order_old', 'path*_old', 'path+_old', 'path?_old', 'pathN*_old', 'pathN+_old',
        'pcs0_old', 'pcs1_old', 'pcs10_old', 'pcs11_old', 'pcs12_old', 'pcs13_old', 'pcs14_old', 'pcs15_old',
        'pcs16_old', 'pcs17_old', 'pcs18_old', 'pcs19_old', 'pcs2_old', 'pcs20_old', 'pcs21_old', 'pcs22_old',
        'pcs23_old', 'pcs24_old', 'pcs3_old', 'pcs4_old', 'pcs5_old', 'pcs6_old', 'pcs7_old', 'pcs8_old', 'pcs9_old',
        'project_old', 'reduced_old', 'sequence_old', 'slice_old', 'tolist_old', 'top_old', 'tree_tdb_old', 'trees_old',
        'treesize_old', 'triple_old', 'union_old', 'query_name_old',
        # matrix and binary tree
        'triples',
        'total_bgps',
        'treesize',
        'matrix_format',
        'trees',
        'trees_old_format',
        'json_time_predicate',
        'json_fanout_predicate',
        'json_input_rows_predicate',
        'json_cardinality_fanout',
        'json_cardinality',
        'scan_queries',
        'bgps',
        'matrix_subtrees',
        'matrix_subtrees_full'
    ]
    final_df = pd.DataFrame(list_of_features, columns=columns)
    return final_df

