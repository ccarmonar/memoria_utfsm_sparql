import hashlib, numpy as np, pandas as pd, re
from functions.aux import OnlyScans


def HashStringId(string):
    sha = hashlib.sha256()
    sha.update(string.encode())
    sha_return = sha.hexdigest()
    return sha_return


def GetTriplesSubtree(subtree_as_str):
    code_tpf = ['VAR_VAR_VAR', 'VAR_VAR_URI', 'VAR_URI_VAR', 'VAR_URI_URI', 'VAR_URI_LITERAL', 'VAR_VAR_LITERAL',
                'URI_URI_LITERAL', 'URI_URI_VAR', 'URI_URI_URI', 'URI_VAR_VAR', 'URI_VAR_URI', 'URI_VAR_LITERAL',
                'LITERAL_URI_VAR', 'LITERAL_URI_URI', 'LITERAL_URI_LITERAL']
    total_triples = 0
    for i in code_tpf:
        total_triples += subtree_as_str.count(i)
    #if subtree_as_str in code_tpf:
    #    total_triples += 1
    return total_triples

def GetTreeSize(subtrees, treesize):
    for st in subtrees:
        #print(st)
        if type(st) == str:
            treesize += 1
        else:
            if len(st) == 3:
                return GetTreeSize(st, treesize)
            elif len(st) == 1:
                treesize += 1
                break
    return treesize


def GetAllJoins(subtree_as_str):
    join = subtree_as_str.count('JOIN')
    left_join = subtree_as_str.count('LEFT_JOIN')
    return join-left_join, left_join



def MatrixFormat_subtrees(operators, subtrees, total_time, num_bgp_subtree):
    if not subtrees:
        iter = 0
        if 'iter' in operators['GF_FROM_OP']['tree']:
            iter = 1
        return [[operators['GF_FROM_OP']['tree'],total_time,1,1,1,0,0,iter]]
    only_scans, keys = OnlyScans(operators, True)
    times_op = []
    times_only_scans_sums = []
    time_sum = 0
    for k,v in operators.items():
        if k != 'GENERAL_FEATURES' and k != 'GF_FROM_OP':
            times_op.append([k, np.float64(v['time'])])

    for top in times_op:
        time_sum += top[1]
        if top[0] in keys:
            if total_time != 0:
                times_only_scans_sums.append([top[0], time_sum*float(total_time)/100, operators[top[0]]['P']])
            time_sum = 0
    times_only_scans_sums[-1][1] += time_sum

    #print("total_time", total_time)

    subtrees_as_str = []
    for s in subtrees:
        subtrees_as_str.append(str(s))

    subtrees_with_features = []
    for s_str, s, bgp in zip(subtrees_as_str,subtrees, num_bgp_subtree):
        time_exc = 0
        join, left_join = GetAllJoins(s_str)
        treesize = GetTreeSize(s,0)
        iter = 0
        triples = GetTriplesSubtree(s_str)
        for tons in times_only_scans_sums:
            if tons[2] in s_str:
                time_exc += tons[1]
            if 'iter' in s_str:
                iter = 1
        subtrees_with_features.append([s,float(time_exc),bgp,triples,treesize,join,left_join,iter])
    subtrees_with_features[-1][1] = float(total_time)
    subtrees_with_features = subtrees_with_features
    return subtrees_with_features


def MatrixFormat(operators, predicates):
    matrix_format = []
    for k in operators.keys():
        aux = []
        #time, fanout, input_rows, cardinality_estimate,cardinality_fanout, operator_type, precode_bool, after_code_bool, group_by_read, distinct_bool, TOP_bool,
        # TOP_num, top_order_by_bool, skip_node_bool, skip_node_num, start_optional, end_optional, optional_section, after_test_1op, after_test_lvl, target_bracket,
        # transitive_bracket, union_sort_lvl+union_sub_lvl, sort_lvl, subquerie_lvl, subquery_select?, select?

        aux.extend([
            operators[k]['time'],
            operators[k]['fanout'],
            operators[k]['input_rows'],
            operators[k]['cardinality_estimate'],
            operators[k]['cardinality_fanout'],
            operators[k]['operator_type'],
            operators[k]['precode_bool'],
            operators[k]['after_code_bool'],
            operators[k]['group_by_read_bool'],
            operators[k]['distinct_bool'],
            operators[k]['TOP_bool'],
            operators[k]['TOP_num'],
            operators[k]['top_order_by_bool'],
            operators[k]['skip_node_bool'],
            operators[k]['skip_node_num'],
            operators[k]['start_optional'],
            operators[k]['end_optional'],
            operators[k]['optional_section?'],
            operators[k]['after_test_1op?'],
            operators[k]['after_test_lvl'],
            operators[k]['target_bracket'],
            operators[k]['transitive_bracket'],
            operators[k]['union_sort_lvl'] + operators[k]['union_sub_lvl'],
            operators[k]['sort_lvl'],
            operators[k]['subquerie_lvl'],
            operators[k]['subquery_select?'],
            operators[k]['select?']
        ])
        for p in predicates:
            aux.append(operators[k][p])

        matrix_format.append(aux)


    return matrix_format


def MatrixNumpyFormat(operators,predicates):
    matrix_np_format = np.array(MatrixFormat(operators,predicates))
    return matrix_np_format


def DataFrameFormat(operators,predicates):
    columns = []
    columns_general = [
        'time',
        'fanout',
        'input_rows',
        'cardinality_estimate',
        'cardinality_fanout',
        'operator_type',
        'precode_bool',
        'after_code_bool',
        'group_by_read_bool',
        'distinct_bool',
        'TOP_bool',
        'TOP_num',
        'top_order_by_bool',
        'skip_node_bool',
        'skip_node_num',
        'start_optional',
        'end_optional',
        'optional_section?',
        'after_test_1op?',
        'after_test_lvl',
        'target_bracket',
        'transitive_bracket',
        'union_lvl',
        'sort_lvl',
        'subquerie_lvl',
        'subquery_select?',
        'select?'
    ]
    columns_predicates = []
    for p in predicates:
        columns_predicates.append(p)

    columns.extend(columns_general)
    columns.extend(columns_predicates)

    df_format = pd.DataFrame(MatrixFormat(operators, predicates), index=operators.keys(), columns=columns)
    return df_format
