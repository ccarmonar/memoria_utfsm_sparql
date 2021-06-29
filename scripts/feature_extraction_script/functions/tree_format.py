import json, os, hashlib, numpy as np, pandas as pd
from matrix_format import MatrixFormat
os.chdir("/home/ccarmona/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script")



# Opening JSON file
example = 'ex047'
with open('returns/'+example+'.json') as json_file:
    operators = json.load(json_file)

#operators[k]['time'],
#operators[k]['fanout'],
#operators[k]['input_rows'],
#operators[k]['cardinality_estimate'],
#operators[k]['cardinality_fanout'],
#operators[k]['operator_type'],
#operators[k]['precode_bool'],
#operators[k]['after_code_bool'],
#operators[k]['group_by_read_bool'],
#operators[k]['distinct_bool'],
#operators[k]['TOP_bool'],
#operators[k]['TOP_num'],
#operators[k]['top_order_by_bool'],
#operators[k]['skip_node_bool'],
#operators[k]['skip_node_num'],
#operators[k]['start_optional'],
#operators[k]['end_optional'],
#operators[k]['optional_section?'],
#operators[k]['after_test_1op?'],
#operators[k]['after_test_lvl'],
#operators[k]['target_bracket'],
#operators[k]['transitive_bracket'],
#operators[k]['union_sort_lvl'],
#operators[k]['sort_lvl'],
#operators[k]['union_sub_lvl'],
#operators[k]['subquerie_lvl'],
#operators[k]['subquery_select?'],
#operators[k]['select?'],


def IdentifyJoinType(operator):
    print(operator[''])
    join_type = 0
    return join_type


def OnlyScans(operators):
    only_scans = []
    for k in operators.keys():
        if operators[k]['operator_type'] == 1 or operators[k]['start_optional'] == 1:
            only_scans.append(operators[k])
    return only_scans


def IterateBuildTree(prearmed, binary_tree_format):
    if len(prearmed) == 0:
        return binary_tree_format,prearmed
    if len(prearmed) == 1:
        binary_tree_format = prearmed
        prearmed = []
        return binary_tree_format,prearmed
    else:
        if len(binary_tree_format) == 0:
            aux = [prearmed[2],prearmed[0], prearmed[1]]
            prearmed = prearmed[3::]
            return IterateBuildTree(prearmed, aux)
        else:
            aux = [prearmed[1],binary_tree_format,prearmed[0]]
            prearmed = prearmed[2::]
            return IterateBuildTree(prearmed, aux)


def BinaryTreeFormat(operators, matrix_format):
    list_op = list(operators.keys())

    prearmed = []
    j = 0
    for k in range(0, len(list_op)):
        if operators[list_op[k]]['operator_type'] == 1:
            if j == 0  and operators[list_op[k-1]]['start_optional'] == 0:
                prearmed.append(matrix_format[k])
                j = 1
            elif j == 0 and  operators[list_op[k - 1]]['start_optional'] == 0:
                prearmed.append(matrix_format[k])
                j = 1
            elif j == 1 and operators[list_op[k-1]]['start_optional'] == 1:
                prearmed.append(matrix_format[k])
                prearmed.append("LOJ")
            elif j == 1 and operators[list_op[k-1]]['start_optional'] == 0:
                prearmed.append(matrix_format[k])
                prearmed.append("IJ")

    binary_tree_format = []

    binary_tree_format, prearmed = IterateBuildTree(prearmed, binary_tree_format)


    return binary_tree_format


matrix_format = MatrixFormat(operators)
for k,v in operators.items():
    if operators[k]['operator_type'] == 1:
        print(v)
print("++++")
print(BinaryTreeFormat(operators, matrix_format))

