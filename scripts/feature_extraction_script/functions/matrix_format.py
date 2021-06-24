import json, os, hashlib
print(os.path.abspath(os.curdir))
os.chdir("..")
print(os.path.abspath(os.curdir))
# Opening JSON file
example = 'test_wikidata1'
with open('returns/'+example+'.json') as json_file:
    operators = json.load(json_file)


def HashStringId(string):
    sha = hashlib.sha256()
    sha.update(string.encode())
    sha_return = sha.hexdigest()
    return sha_return


def MatrixFormat(operators):
    matrix_format = []
    for k in operators.keys():
        #time, fanout, input_rows, cardinality_estimate,cardinality_fanout, operator_type, precode_bool, after_code_bool, group_by_read, distinct_bool, TOP_bool,
        # TOP_num, top_order_by_bool, skip_node_bool, skip_node_num, start_optional, end_optional, optional_section, after_test_1op, after_test_lvl, target_bracket,
        # transitive_bracket, union_sort_lvl, sort_lvl, union_sub_lvl, subquerie_lvl, subquery_select?, select?
        matrix_format.append([
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
            operators[k]['union_sort_lvl'],
            operators[k]['sort_lvl'],
            operators[k]['union_sub_lvl'],
            operators[k]['subquerie_lvl'],
            operators[k]['subquery_select?'],
            operators[k]['select?'],
        ])
    return matrix_format


def MatrixNumpyFormat(operators):
    matrix_np_format = operators
    return matrix_np_format


def DataframeFormat(operators):
    df_operators = operators
    return df_operators


x = MatrixFormat(operators)
for i in x:
    print(i)

