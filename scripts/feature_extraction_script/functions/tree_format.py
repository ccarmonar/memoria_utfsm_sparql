from anytree import Node, RenderTree
from anytree.exporter import DotExporter
from functions.aux import OnlyScans, Flatten


def IterateBuildTree(tree_format, prearmed, symbol):
    if len(prearmed) == 0:
        return tree_format,prearmed
    if len(prearmed) == 1:
        tree_format = prearmed
        prearmed = []
        return tree_format,prearmed
    else:
        if len(tree_format) == 0:
            aux = [prearmed[1],[prearmed[0]], [prearmed[2]]]
            prearmed = prearmed[3::]
        else:
            aux = [prearmed[0],tree_format,prearmed[1]]
            prearmed = prearmed[2::]
    return IterateBuildTree(aux, prearmed, symbol)


def InnerJoinsIntraBGPS(bgp, symbol):
    prearmed = []
    tree_format = []
    predicates = []
    for k in range(len(bgp)):
        if k == 0:
            if 'NONE' not in bgp[k]['P']:
                predicates.append(bgp[k]['P'])
            prearmed.append(bgp[k]['triple_type']+ symbol + bgp[k]['P'])
        else:
            if 'NONE' not in bgp[k]['P']:
                predicates.append(bgp[k]['P'])
            prearmed.append('JOIN' + symbol + symbol.join(predicates[:k+1]))
            prearmed.append(bgp[k]['triple_type'] + symbol + bgp[k]['P'])
    tree_format, prearmed = IterateBuildTree(tree_format, prearmed, symbol)
    return tree_format, predicates


def IterateBuildTreeBetweenBGPS(tree_format, prearmed, symbol):
    if len(prearmed) == 0:
        return tree_format,prearmed
    if len(prearmed) == 1:
        tree_format = prearmed
        prearmed = []
        return tree_format,prearmed
    else:
        if len(tree_format) == 0:
            aux = [prearmed[1],prearmed[0], prearmed[2]]
            prearmed = prearmed[3::]
        else:
            aux = [prearmed[0],tree_format,prearmed[1]]
            prearmed = prearmed[2::]
    return IterateBuildTreeBetweenBGPS(aux, prearmed, symbol)



def TreeFormat(operators, sparql_file, symbol):
    bgp_joins = []
    bgp_type = []
    tree_format = []
    prearmed = []
    list_current_predicates = []
    for k, v in operators['GF_FROM_OP']['bgps_ops'].items():
        bgp, current_predicates = InnerJoinsIntraBGPS(v['bgp_list'], symbol)
        type = v['opt']
        bgp_joins.append(bgp)
        bgp_type.append(type)
        list_current_predicates.append(current_predicates)


    treesize_between = len(list_current_predicates) - 1
    treesize_intra = 0
    for i in list_current_predicates:
        treesize_intra = max(treesize_intra,len(i) - 1)

    for k in range(len(bgp_joins)):
        if k == 0:
            prearmed.append(bgp_joins[k])
        else:
            if bgp_type[k] == 0:
                prearmed.append('JOIN' + symbol + symbol.join(Flatten(list_current_predicates[:k+1])))
                prearmed.append(bgp_joins[k])
            if bgp_type[k] == 1:
                prearmed.append('LEFT JOIN' + symbol + symbol.join(Flatten(list_current_predicates[:k+1])))
                prearmed.append(bgp_joins[k])
    if operators['GF_FROM_OP']['total_bgps'] == 1:
        tree_format = prearmed[0]
    else:
        tree_format, prearmed = IterateBuildTreeBetweenBGPS(tree_format, prearmed, symbol)
    operators['GF_FROM_OP']['tree'] = tree_format

    treesize = treesize_between + treesize_intra

    operators['GF_FROM_OP']['treesize'] = treesize
    return tree_format, operators