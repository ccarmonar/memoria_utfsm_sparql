from anytree import Node, RenderTree
from anytree.exporter import DotExporter


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


def IterateBuildTree2(prearmed, binary_tree_format):
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

def Intermediate_Nodes():
    return 0


def CheckIfOptional(operators, key):
    list_op = list(operators.keys())
    c = 0
    k = list_op.index(key)
    for k in list_op[:k]:
        if operators[k]['start_optional'] == 1:
            c = c + 1
        if operators[k]['end_optional'] == 1:
            c = c + 1
    return c


def ScanQueriesKeys(operators):
    scan_queries = []
    for k in operators.keys():
        if operators[k]['operator_type'] == 1:
            scan_queries.append(k)
    return scan_queries


def BinaryTreeFormat2(operators, matrix_format):
    list_op = list(operators.keys())
    prearmed = []
    j = 0

    for k in range(0, len(list_op)):
        if operators[list_op[k]]['operator_type'] == 1:
            check_opt = CheckIfOptional(operators,k)
            if j == 0:
                prearmed.append([operators[list_op[k]]['triple_type'] + "-" + operators[list_op[k]]['P']])
                j = 1
            elif j == 1 and check_opt:
                #prearmed.append(matrix_format[k])
                prearmed.append([operators[list_op[k]]['triple_type'] + " - " + operators[list_op[k]]['P']])
                prearmed.append("LEFT_JOIN")
            elif j == 1 and not check_opt:
                #prearmed.append(matrix_format[k])
                #prearmed.append(list_op[k])
                prearmed.append([operators[list_op[k]]['triple_type'] + " - " + operators[list_op[k]]['P']])
                prearmed.append("JOIN")

    print(prearmed)

    binary_tree_format = []

    binary_tree_format, prearmed = IterateBuildTree(prearmed, binary_tree_format)

    print(binary_tree_format)
    return binary_tree_format


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
        else:
            aux = [prearmed[1],binary_tree_format,prearmed[0]]
            prearmed = prearmed[2::]
    return IterateBuildTree(prearmed, aux)


def BinaryTreeFormat(operators, matrix_format):
    scan_queries_keys = ScanQueriesKeys(operators)
    prearmed, binary_tree_format = [],[]
    first_node = 1
    for k in range(len(scan_queries_keys)):
        if first_node == 1:
            prearmed.append([operators[scan_queries_keys[k]]['triple_type'] + "-" + operators[scan_queries_keys[k]]['P']])
            first_node = 0
        else:
            if operators[scan_queries_keys[k]]['optional_section?'] == 1:
                check_opt = CheckIfOptional(operators, scan_queries_keys[k])
                #print(scan_queries_keys[k], check_opt)
                if operators[scan_queries_keys[k-1]]['optional_section?'] == 1 and check_opt % 2 == 0:
                    prearmed.append([operators[scan_queries_keys[k]]['triple_type'] + "-" + operators[scan_queries_keys[k]]['P']])
                    prearmed.append(str(scan_queries_keys[k])+'JOIN')
                else:
                    prearmed.append([operators[scan_queries_keys[k]]['triple_type'] + "-" + operators[scan_queries_keys[k]]['P']])
                    prearmed.append(str(scan_queries_keys[k])+'LEFT_JOIN')
            else:
                prearmed.append([operators[scan_queries_keys[k]]['triple_type'] + "-" + operators[scan_queries_keys[k]]['P']])
                prearmed.append('JOIN')
    #print(prearmed)
    binary_tree_format, prearmed = IterateBuildTree(prearmed, binary_tree_format)
    print(binary_tree_format)

    return binary_tree_format


def IdentifyBGPs(operators):
    scan_queries = ScanQueriesKeys(operators)


    for k in range(len(scan_queries)):
        print(operators[scan_queries[k]]['triple_type'])

    return "t"


