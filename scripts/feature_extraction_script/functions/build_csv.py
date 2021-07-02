import json, os, re, numpy as np, pandas as pd
from general_features import GeneralFeaturesFromProfileFile
from matrix_format import MatrixFormat, MatrixNumpyFormat, DataFrameFormat
from tree_format import IdentifyJoinType, OnlyScans, IterateBuildTree, BinaryTreeFormat
from aux import HashStringId, GetAllPredicatesFromProfile

os.chdir("/home/ccarmona/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script")


def AllData(operators, profile):
    predicates = GetAllPredicatesFromProfile(operators)
    matrix_format = MatrixFormat(operators, predicates)
    binary_tree = BinaryTreeFormat(operators, matrix_format)
    general_features = GeneralFeaturesFromProfileFile(profile, operators)
    unique_id = HashStringId(str(predicates) + str(matrix_format) + str(binary_tree) + str(general_features))
    limit = general_features['GENERAL_FEATURES']['LIMIT']
    precompiled_list = list(general_features['GENERAL_FEATURES']['precompiled'].values())
    compiled_list = list(general_features['GENERAL_FEATURES']['compiled'].values())
    all_data = [unique_id,limit] + precompiled_list + compiled_list
    all_data.append(str(matrix_format))
    all_data.append(str(binary_tree))
    return all_data

def FullDataframe(list_of_features):
    columns= [
        'unique_id',
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



# Opening JSON file
filename1 = 'test_wikidata5'
with open('returns/'+filename1+'.json') as json_file:
    operators1 = json.load(json_file)
profile_normal1 = open('/home/ccarmona/Memoria/memoria_utfsm_sparql/scripts/outputs/outputs_' + filename1 + '/profile_normal_file_' + filename1, 'r', encoding = 'latin-1').read()

filename2 = 'test_wikidata2'
with open('returns/'+filename2+'.json') as json_file:
    operators2 = json.load(json_file)
profile_normal2 = open('/home/ccarmona/Memoria/memoria_utfsm_sparql/scripts/outputs/outputs_' + filename2 + '/profile_normal_file_' + filename2, 'r', encoding = 'latin-1').read()


ex1 = AllData(operators1,profile_normal1)
ex2 = AllData(operators2,profile_normal2)

examples = []
examples.append(ex1)
examples.append(ex2)


df = FullDataframe(examples)
print(df)
print(df.to_csv('test.csv', index=False))

