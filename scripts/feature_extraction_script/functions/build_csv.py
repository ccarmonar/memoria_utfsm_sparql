import pandas as pd
from functions.general_features import GeneralFeaturesFromProfileFile
from functions.matrix_format import MatrixFormat, MatrixNumpyFormat, DataFrameFormat
from functions.tree_format import IdentifyJoinType, OnlyScans, IterateBuildTree, BinaryTreeFormat
from functions.aux import HashStringId


def AllData(operators, profile, predicates, filename, sparql_file):
    matrix_format = MatrixFormat(operators, predicates)
    binary_tree = BinaryTreeFormat(operators, matrix_format)
    general_features = GeneralFeaturesFromProfileFile(profile, operators)
    unique_id = HashStringId(str(predicates) + str(matrix_format) + str(binary_tree) + str(general_features))
    limit = general_features['GENERAL_FEATURES']['LIMIT']
    precompiled_list = list(general_features['GENERAL_FEATURES']['precompiled'].values())
    compiled_list = list(general_features['GENERAL_FEATURES']['compiled'].values())
    all_data = [unique_id, filename, sparql_file, profile, limit] + precompiled_list + compiled_list
    all_data.append(str(matrix_format))
    all_data.append(str(binary_tree))
    return all_data

def FullDataframe(list_of_features):
    columns= [
        'unique_id',
        'filename',
        'sparql_file',
        'profile',
        'limit',
        'ql_rt_msec',
        'ql_rt_clocks',
        'ql_rnd_rows',
        'ql_seq_rows',
        'ql_same_seg',
        'ql_same_page',
        'ql_disk_reads',
        'ql_spec_disk_reads',
        'ql_cl_wait_clocks',
        'ql_c_msec',
        'ql_c_disk',
        'ql_c_clocks',
        'ql_cl_messages',
        'ql_c_cl_wait',
        'matrix_format',
        'binary_tree'
    ]
    final_df = pd.DataFrame(list_of_features, columns=columns)
    return final_df

def ExportToCSV():
    return 0



