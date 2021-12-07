from functions.aux import OnlyScans, Flatten


def IterateBuildTree(tree_format, prearmed, subtrees, symbol):
    if len(prearmed) == 0:
        return tree_format,prearmed,subtrees
    if len(prearmed) == 1:
        tree_format = prearmed
        prearmed = []
        return tree_format,prearmed,subtrees
    else:
        if len(tree_format) == 0:
            aux = [prearmed[1],[prearmed[0]], [prearmed[2]]]
            subtrees.append([prearmed[0]])
            subtrees.append(aux)
            prearmed = prearmed[3::]

        else:
            aux = [prearmed[0],tree_format,prearmed[1]]
            subtrees.append(aux)
            prearmed = prearmed[2::]
    return IterateBuildTree(aux, prearmed, subtrees, symbol)


def InnerJoinsIntraBGPS(bgp, subtrees, symbol):
    prearmed = []
    tree_format = []
    predicates = []
    for k in range(len(bgp)):
        if k == 0:
            if 'NONE' not in bgp[k]['P']:
                predicates.append(bgp[k]['P'])
            prearmed.append(bgp[k]['triple_type'] + symbol + bgp[k]['P'])
        else:
            if 'NONE' not in bgp[k]['P']:
                predicates.append(bgp[k]['P'])
            prearmed.append('JOIN' + symbol + symbol.join(predicates[:k+1]))
            prearmed.append(bgp[k]['triple_type'] + symbol + bgp[k]['P'])
    tree_format, prearmed, subtrees = IterateBuildTree(tree_format, prearmed,subtrees ,symbol)
    return tree_format, predicates, subtrees


def IterateBuildTreeBetweenBGPS(tree_format, prearmed, subtrees, symbol):
    if len(prearmed) == 0:
        return tree_format,prearmed
    if len(prearmed) == 1:
        tree_format = prearmed
        prearmed = []
        return tree_format,prearmed
    else:
        if len(tree_format) == 0:
            aux = [prearmed[1],prearmed[0], prearmed[2]]
            subtrees.append(prearmed[0])
            subtrees.append(aux)
            prearmed = prearmed[3::]
        else:
            aux = [prearmed[0],tree_format,prearmed[1]]
            subtrees.append(aux)
            prearmed = prearmed[2::]
    return IterateBuildTreeBetweenBGPS(aux, prearmed, subtrees, symbol)



def TreeFormat(operators, sparql_file, symbol):
    bgp_joins = []
    bgp_type = []
    tree_format = []
    prearmed = []
    list_current_predicates = []
    subtrees = []
    num_bgp_subtree = []
    for k, v in operators['GF_FROM_OP']['bgps_ops'].items():
        bgp, current_predicates, subtrees = InnerJoinsIntraBGPS(v['bgp_list'], subtrees, symbol)
        type = v['opt']
        bgp_joins.append(bgp)
        bgp_type.append(type)
        list_current_predicates.append(current_predicates)
    for i in subtrees:
        num_bgp_subtree.append(1)
    subtrees_prev = subtrees.copy()

    treesize_between = len(list_current_predicates) - 1
    treesize_intra = 0
    for i in list_current_predicates:
        treesize_intra = max(treesize_intra,len(i) - 1)

    for k in range(len(bgp_joins)):
        if k == 0:
            prearmed.append(bgp_joins[k])
        else:
            if bgp_type[k] == 0:
                prearmed.append('JOIN')
                prearmed.append(bgp_joins[k])
            if bgp_type[k] == 1:
                prearmed.append('LEFT_JOIN')
                prearmed.append(bgp_joins[k])
    if operators['GF_FROM_OP']['total_bgps'] == 1:
        tree_format = prearmed[0]
    else:
        tree_format, prearmed = IterateBuildTreeBetweenBGPS(tree_format, prearmed, subtrees, symbol)
    operators['GF_FROM_OP']['tree'] = tree_format
    c = 1
    for i in range(len(subtrees)):
        if len(subtrees_prev) - 1 < i:
            num_bgp_subtree.append(c)
            c += 1

    treesize = treesize_between + treesize_intra
    operators['GF_FROM_OP']['subtrees'] = subtrees
    operators['GF_FROM_OP']['treesize'] = treesize
    return tree_format, operators, subtrees, num_bgp_subtree


def TreeFormat_old_format(operators, sparql_file, symbol):
    bgp_joins = []
    bgp_type = []
    tree_format = []
    prearmed = []
    list_current_predicates = []
    subtrees = []
    for k, v in operators['GF_FROM_OP']['bgps_ops'].items():
        bgp, current_predicates, subtrees = InnerJoinsIntraBGPS(v['bgp_list'], subtrees, symbol)
        type = v['opt']
        bgp_joins.append(bgp)
        bgp_type.append(type)
        list_current_predicates.append(current_predicates)
    #treesize_between = len(list_current_predicates) - 1
    #treesize_intra = 0
    #for i in list_current_predicates:
    #    treesize_intra = max(treesize_intra,len(i) - 1)

    for k in range(len(bgp_joins)):
        if k == 0:
            prearmed.append(bgp_joins[k])
        else:
            if bgp_type[k] == 0:
                prearmed.append('JOIN' + symbol + symbol.join(Flatten(list_current_predicates[:k+1])))
                prearmed.append(bgp_joins[k])
            if bgp_type[k] == 1:
                prearmed.append('LEFT_JOIN' + symbol + symbol.join(Flatten(list_current_predicates[:k+1])))
                prearmed.append(bgp_joins[k])
    if operators['GF_FROM_OP']['total_bgps'] == 1:
        tree_format = prearmed[0]
    else:
        tree_format, prearmed = IterateBuildTreeBetweenBGPS(tree_format, prearmed, subtrees, symbol)
    operators['GF_FROM_OP']['trees_old_format'] = tree_format
    #treesize = treesize_between + treesize_intra
    #operators['GF_FROM_OP']['treesize'] = treesize
    operators['GF_FROM_OP']['subtrees_old_format'] = subtrees
    return tree_format, operators, subtrees