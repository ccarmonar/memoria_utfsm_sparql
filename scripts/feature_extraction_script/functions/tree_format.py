from anytree import Node, RenderTree
from anytree.exporter import DotExporter
from functions.aux import OnlyScans


def IterateBuildTree(tree_format, prearmed):
    if len(prearmed) == 0:
        return tree_format,prearmed
    if len(prearmed) == 1:
        tree_format = prearmed
        prearmed = []
        return tree_format,prearmed
    else:
        if len(tree_format) == 0:
            aux = [prearmed[1],[prearmed[0], prearmed[2]]]
            prearmed = prearmed[3::]
        else:
            aux = [prearmed[0],tree_format,prearmed[1]]
            prearmed = prearmed[2::]
    return IterateBuildTree(aux, prearmed)


def InnerJoinsIntraBGPS(bgp):
    prearmed = []
    tree_format = []
    for k in range(len(bgp)):
        if k == 0:
            prearmed.append(bgp[k]['triple_type']+"|"+bgp[k]['P'])
        else:
            prearmed.append('JOIN')
            prearmed.append(bgp[k]['triple_type'] + "|" + bgp[k]['P'])
    tree_format, prearmed = IterateBuildTree(tree_format, prearmed)
    return tree_format


def IterateBuildTreeBetweenBGPS(tree_format, prearmed):
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
    return IterateBuildTreeBetweenBGPS(aux, prearmed)


def TreeFormat(operators, sparql_file):
    bgp_joins = []
    bgp_type = []
    tree_format = []
    prearmed = []
    for k,v in operators['GF_FROM_OP']['bgps_ops'].items():
        bgp = InnerJoinsIntraBGPS(v['bgp_list'])
        type = v['opt']
        bgp_joins.append(bgp)
        bgp_type.append(type)

    for k in range(len(bgp_joins)):
        if k == 0:
            prearmed.append(bgp_joins[k])
        else:
            if bgp_type[k] == 0:
                prearmed.append('JOIN')
                prearmed.append(bgp_joins[k])
            if bgp_type[k] == 1:
                prearmed.append('LEFT JOIN')
                prearmed.append(bgp_joins[k])
    if operators['GF_FROM_OP']['total_bgps'] == 1:
        tree_format = prearmed[0]
    else:
        tree_format, prearmed = IterateBuildTreeBetweenBGPS(tree_format, prearmed)
    print(tree_format)

    return tree_format