import hashlib, numpy as np, pandas as pd

def HashStringId(string):
    sha = hashlib.sha256()
    sha.update(string.encode())
    sha_return = sha.hexdigest()
    return sha_return


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
