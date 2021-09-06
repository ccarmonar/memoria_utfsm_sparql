from anytree import Node, RenderTree
from anytree.exporter import DotExporter
from functions.aux import OnlyScans


def IterateBuildTree(prearmed, binary_tree_format):
    if len(prearmed) == 0:
        return binary_tree_format,prearmed
    if len(prearmed) == 1:
        binary_tree_format = prearmed
        prearmed = []
        return binary_tree_format,prearmed
    else:
        if len(binary_tree_format) == 0:
            aux = [prearmed[1],[prearmed[0], prearmed[2]]]
            prearmed = prearmed[3::]
        else:
            aux = [prearmed[0],binary_tree_format,prearmed[1]]
            prearmed = prearmed[2::]
    return IterateBuildTree(prearmed, aux)



def InnerJoinsIntraBGPS(bgp):
    prearmed = []
    binary_tree_format = []

    for k in range(len(bgp)):
        if k == 0:
            prearmed.append(bgp[k]['OP']+"|"+bgp[k]['P'])
        else:
            prearmed.append('JOIN')
            prearmed.append(bgp[k]['OP'] + "|" + bgp[k]['P'])
    binary_tree_format, prearmed = IterateBuildTree(prearmed, binary_tree_format)
    return binary_tree_format


def TreeFormat(operators, sparql_file):
    bgp_joins = []
    tree_format = []
    for k in operators['GF_FROM_OP']['bgps_ops']:
        x = InnerJoinsIntraBGPS(operators['GF_FROM_OP']['bgps_ops'][k])
        bgp_joins.append(x)

    return 0