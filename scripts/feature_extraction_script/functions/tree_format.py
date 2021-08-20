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

def Intermediate_Nodes():
    return 0


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
