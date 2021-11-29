from functions.aux import MainCurlyBrackets, OnlyScans, ListDuplicatesOf
import collections

def GeneralFeaturesFromProfileFile(profile_file,operators):
     ## IDENTIFICAR LIMIT
    limit = 0
    for k in operators.keys():
        if operators[k]['skip_node_bool'] == 1 or operators[k]['TOP_bool'] == 1:
            limit = operators[k]['TOP_num'] + operators[k]['skip_node_num']
    # CREAR LLAVE DE GENERAL_FEATURES
    operators['GENERAL_FEATURES'] = {
        'LIMIT' : limit,
        'precompiled' : {
            'msec': '0',
            'cpu_p': '0',
            'rnd': '0',
            'seq': '0',
            'same_seg_p': '0',
            'same_page_p': '0',
            'disk_reads': '0',
            'read_ahead': '0',
            'wait_p': '0'
        },
        'compiled' : {
            'comp_msec': '0',
            'comp_reads': '0',
            'comp_read_p': '0',
            'comp_messages': '0',
            'comp_clw': '0'
        }
    }



    replace_string = profile_file.replace(MainCurlyBrackets(profile_file),'').replace('Warning: You might have a Cartesian product.','').split('{}')
    general_features_string = replace_string[1].strip().split('Compilation:')
    precompilation_list = general_features_string[0].strip().split()
    compilation_list = general_features_string[1].strip().split()
    #print(precompilation_list)
    #print(compilation_list)


    #ALL FEATURE WAS COMPUTED
    # (name - medition)

    ## PRECOMPILATION
    # ql_rt_msec - msec
    # ql_rt_clocks - %cpu
    # ql_rnd_rows - rnd
    # ql_seq_rows - seq
    # ql_same_seg - same seg
    # ql_same_page - same pg
    # ql_disk_reads - disk reads
    # ql_spec_disk_reads - read ahead
    # ql_cl_wait_clocks - wait

    for i in range(0,len(precompilation_list)):
        if precompilation_list[i] == 'msec':
            operators['GENERAL_FEATURES']['precompiled']['msec'] = precompilation_list[i-1]
        if precompilation_list[i] == 'cpu,':
            operators['GENERAL_FEATURES']['precompiled']['cpu_p'] = precompilation_list[i-1]
        if precompilation_list[i] == 'rnd':
            operators['GENERAL_FEATURES']['precompiled']['rnd'] = precompilation_list[i-1]
        if precompilation_list[i] == 'seq':
            operators['GENERAL_FEATURES']['precompiled']['seq'] = precompilation_list[i-1]
        if precompilation_list[i] == 'same' and precompilation_list[i+1] == 'seg':
            operators['GENERAL_FEATURES']['precompiled']['same_seg_p'] = precompilation_list[i-1]
        if precompilation_list[i] == 'same' and precompilation_list[i+1] == 'pg':
            operators['GENERAL_FEATURES']['precompiled']['same_page_p'] = precompilation_list[i-1]
        if precompilation_list[i] == 'disk' and precompilation_list[i+1] == 'reads':
            operators['GENERAL_FEATURES']['precompiled']['disk_reads'] = precompilation_list[i-1]
        if precompilation_list[i] == 'read' and precompilation_list[i+1] == 'ahead':
            operators['GENERAL_FEATURES']['precompiled']['read_ahead'] = precompilation_list[i-1]
        if precompilation_list[i] == 'wait':
            operators['GENERAL_FEATURES']['precompiled']['wait_p'] = precompilation_list[i-1]

    ## COMPILATION
    # ql_c_msec - msec
    # ql_c_disk - reads
    # ql_c_clocks - %read
    # ql_cl_messages - messages
    # ql_c_cl_wait - clw
    for i in range(0,len(compilation_list)):
        if compilation_list[i] == 'msec':
            operators['GENERAL_FEATURES']['compiled']['comp_msec'] = compilation_list[i-1]
        if compilation_list[i] == 'reads':
            operators['GENERAL_FEATURES']['compiled']['comp_reads'] = compilation_list[i-1]
        if compilation_list[i] == 'read' and compilation_list[i] != 'reads':
            operators['GENERAL_FEATURES']['compiled']['comp_read_p'] = compilation_list[i-1]
        if compilation_list[i] == 'messages':
            operators['GENERAL_FEATURES']['compiled']['comp_messages'] = compilation_list[i-1]
        if compilation_list[i] == 'clw':
            operators['GENERAL_FEATURES']['compiled']['comp_clw'] = compilation_list[i - 1]

    return operators


def IdentifyFilter(operators, sparql_file):

    split_1 = sparql_file.split('FILTER')[1:]
    num_filter = sparql_file.count('FILTER')
    filter_eq = 0
    filter_gt = 0
    filter_ge = 0
    filter_lt = 0
    filter_le = 0
    filter_neq = 0
    filter_iri = 0
    filter_bound = 0
    filter_contains = 0
    filter_exists = 0
    filter_isBlank = 0
    filter_isIRI = 0
    filter_isLiteral = 0
    filter_lang = 0
    filter_langMatches = 0
    filter_not = 0
    filter_notexists = 0
    filter_or = 0
    filter_regex = 0
    filter_sameTerm = 0
    filter_str = 0
    filter_strstarts = 0
    filter_and = 0

    for i in split_1:
        find_filter = i[i.find("(") + 1:i.rfind(")")]
        if " = " in find_filter and all(e not in find_filter for e in ["<=,>=,!="]):
            filter_eq += 1
        if " > " in find_filter and all(e not in find_filter for e in ["<=,>=,!="]):
            filter_gt += 1
        if " >= " in find_filter:
            filter_ge += 1
        if " < " in find_filter and "<=" not in find_filter:
            filter_lt += 1
        if " <= " in find_filter:
            filter_le += 1
        if " !" in find_filter:
            filter_not += 1
        if " != " in find_filter:
            filter_neq += 1
        if " bound(" in find_filter and "!" not in find_filter:
            filter_bound += 1
        if " contains(" in find_filter and "!" not in find_filter:
            filter_bound += 1
        if " EXISTS " in find_filter and " NOT " not in find_filter:
            filter_exists += 1
        if " NOT EXISTS " in find_filter :
            filter_notexists += 1
        if " isBlank(" in find_filter and "!" not in find_filter:
            filter_isBlank += 1
        if " isIRI(" in find_filter and "!" not in find_filter:
            filter_isIRI += 1
        if " isLiteral(" in find_filter and "!" not in find_filter:
            filter_isLiteral += 1
        if " lang(" in find_filter and "!" not in find_filter:
            filter_lang += 1
        if " langMatches(" in find_filter and "!" not in find_filter:
            filter_langMatches += 1
        if " regex(" in find_filter and "!" not in find_filter:
            filter_regex += 1
        if " sameTerm(" in find_filter and "!" not in find_filter:
            filter_sameTerm += 1
        if " str(" in find_filter and "!" not in find_filter:
            filter_str += 1
        if " strstarts(" in find_filter and "!" not in find_filter:
            filter_strstarts += 1
        if " && " in find_filter:
            filter_and += 1
        if " || " in find_filter:
            filter_or += 1
    operators['GF_FROM_OP']['num_filter'] = num_filter
    operators['GF_FROM_OP']['filter_eq'] = filter_eq
    operators['GF_FROM_OP']['filter_gt'] = filter_gt
    operators['GF_FROM_OP']['filter_ge'] = filter_ge
    operators['GF_FROM_OP']['filter_lt'] = filter_lt
    operators['GF_FROM_OP']['filter_le'] = filter_le
    operators['GF_FROM_OP']['filter_neq'] = filter_neq
    operators['GF_FROM_OP']['filter_iri'] = filter_iri
    operators['GF_FROM_OP']['filter_neq'] = filter_neq
    operators['GF_FROM_OP']['filter_bound'] = filter_bound
    operators['GF_FROM_OP']['filter_contains'] = filter_contains
    operators['GF_FROM_OP']['filter_exists'] = filter_exists
    operators['GF_FROM_OP']['filter_isBlank'] = filter_isBlank
    operators['GF_FROM_OP']['filter_isIRI'] = filter_isIRI
    operators['GF_FROM_OP']['filter_isLiteral'] = filter_isLiteral
    operators['GF_FROM_OP']['filter_lang'] = filter_lang
    operators['GF_FROM_OP']['filter_langMatches'] = filter_langMatches
    operators['GF_FROM_OP']['filter_not'] = filter_not
    operators['GF_FROM_OP']['filter_notexist'] = filter_notexists
    operators['GF_FROM_OP']['filter_regex'] = filter_regex
    operators['GF_FROM_OP']['filter_sameTerm'] = filter_sameTerm
    operators['GF_FROM_OP']['filter_str'] = filter_str
    operators['GF_FROM_OP']['filter_strstarts'] = filter_strstarts
    operators['GF_FROM_OP']['filter_or'] = filter_or
    operators['GF_FROM_OP']['filter_and'] = filter_and


    return operators


def GeneralFeaturesFromOperatorsAndSparqlFile(operators, sparql_file):
    operators['GF_FROM_OP']['group_by'] = 0
    operators['GF_FROM_OP']['distinct'] = 0
    operators['GF_FROM_OP']['order_by'] = 0
    operators['GF_FROM_OP']['union'] = 0
    operators['GF_FROM_OP']['left_join'] = 0
    operators['GF_FROM_OP']['join'] = 0
    operators['GF_FROM_OP']['iter'] = 0
    operators['GF_FROM_OP']['filter'] = 0
    for k, v in operators.items():
        if k != 'GENERAL_FEATURES' and k != 'GF_FROM_OP':
            if v['group_by_read_bool'] == 1:
                operators['GF_FROM_OP']['group_by'] = 1
            if v['distinct_bool'] == 1:
                operators['GF_FROM_OP']['distinct'] = 1
            if v['top_order_by_bool'] == 1 or v['skip_node_bool'] == 1:
                operators['GF_FROM_OP']['order_by'] = 1
            if v['start_optional'] == 1:
                operators['GF_FROM_OP']['left_join'] = 1
            if v['iter'] == 1:
                operators['GF_FROM_OP']['iter'] = 1
    if operators['GF_FROM_OP']['triples'] > 1:
        operators['GF_FROM_OP']['join'] = 1
    if 'UNION' in sparql_file:
        operators['GF_FROM_OP']['union'] = 1
    if 'FILTER' in sparql_file:
        operators['GF_FROM_OP']['filter'] = 1
    operators = IdentifyFilter(operators, sparql_file)
    return operators

def GeneralFeaturesFromScan(operators, list_alleq):
    triples = 0
    bgps = 0
    only_scans, os_keys = OnlyScans(operators, True)
    ## NUMEROS TOTALES
    for k in operators.keys():
        if k != 'GENERAL_FEATURES':
            if operators[k]['operator_type'] == 1:
                triples += 1
            if operators[k]['num_bgp'] != 'None':
                bgps = max(bgps, int(operators[k]['num_bgp']))

    ## GET BGPS
    keys_to_extract = ['S','P','O','time','fanout','input_rows','cardinality_estimate','cardinality_fanout','triple_type','optional_section?','num_bgp']
    subset_onlyscans = []
    for k in range(len(only_scans)):
        os_subset = {key: only_scans[k][key] for key in keys_to_extract}
        os_subset['OP'] = os_keys[k]
        subset_onlyscans.append(os_subset)

    result = collections.defaultdict(list)
    for d in subset_onlyscans:
        result[d['num_bgp']].append(d)


    result_list = list(result.values())
    bgps_ops_prearmed = dict(zip(range(1,len(result_list)+1),result_list))
    bgps_ops = {}


    for k,v in bgps_ops_prearmed.items():
        bgps_ops["bgp_" + str(k)] = {
            "bgp_list": bgps_ops_prearmed[k],
            "opt": 0
        }
        if len(v) != 1:
            test_aux1 = []
            for d in v:
                test_aux1.append(d['optional_section?'])
            if len(set(test_aux1)) == 1:
                ## Prueba pasada
                bgps_ops["bgp_" + str(k)]['opt'] = v[0]['optional_section?']
            else:
                print("error en un bgp")
                bgps_ops["bgp_" + str(k)]['opt'] = 'ERROR'
        else:
            bgps_ops["bgp_" + str(k)]['opt'] = v[0]['optional_section?']
    operators['GF_FROM_OP'] = {'triples' : triples, 'total_bgps' : bgps, 'list_alleq' : list_alleq, 'bgps_ops': bgps_ops}
    return operators


def GeneralFeaturesFromPerformanceTuning(general_features_pt_file):
    general_features_pt_aux = [x.strip() for x in general_features_pt_file.split('\n')]
    general_features_pt_list = []
    for i in range(len(general_features_pt_aux)):
        if i % 2 == 1:
            general_features_pt_list.append(general_features_pt_aux[i])
    return general_features_pt_list


def GetJsonPredicatesFeatures(operators):
    operators['GF_FROM_OP']['json_time_predicate'] = {}
    operators['GF_FROM_OP']['json_fanout_predicate'] = {}
    operators['GF_FROM_OP']['json_input_rows_predicate'] = {}
    operators['GF_FROM_OP']['json_cardinality_fanout'] = {}
    operators['GF_FROM_OP']['json_cardinality'] = {}

    #wrs = with repeat suma, wrm = with repeat mean
    operators['GF_FROM_OP']['json_time_predicate_wr'] = {}
    operators['GF_FROM_OP']['json_fanout_predicate_wr'] = {}
    operators['GF_FROM_OP']['json_input_rows_predicate_wr'] = {}
    operators['GF_FROM_OP']['json_cardinality_fanout_wr'] = {}
    operators['GF_FROM_OP']['json_cardinality_wr'] = {}
    ##############################
    operators['GF_FROM_OP']['json_time_predicate_wrs'] = {}
    operators['GF_FROM_OP']['json_fanout_predicate_wrs'] = {}
    operators['GF_FROM_OP']['json_input_rows_predicate_wrs'] = {}
    operators['GF_FROM_OP']['json_cardinality_fanout_wrs'] = {}
    operators['GF_FROM_OP']['json_cardinality_wrs'] = {}
    ##############################
    operators['GF_FROM_OP']['json_time_predicate_wrm'] = {}
    operators['GF_FROM_OP']['json_fanout_predicate_wrm'] = {}
    operators['GF_FROM_OP']['json_input_rows_predicate_wrm'] = {}
    operators['GF_FROM_OP']['json_cardinality_fanout_wrm'] = {}
    operators['GF_FROM_OP']['json_cardinality_wrm'] = {}

    var_count = 0
    repeat_list_op = []
    repeat_list_time = []
    repeat_list_fanout = []
    repeat_list_input_rows = []
    repeat_list_cardinality_estimate = []
    repeat_list_cardinality_fanout = []

    for k in operators['GF_FROM_OP']['bgps_ops'].keys():
        bgp_list = operators['GF_FROM_OP']['bgps_ops'][k]['bgp_list']
        for bgp in bgp_list:
            if 'NONE' not in bgp['P']:
                operators['GF_FROM_OP']['json_time_predicate'][bgp['P']] = str(bgp['time'])
                operators['GF_FROM_OP']['json_fanout_predicate'][bgp['P']] = str(bgp['fanout'])
                operators['GF_FROM_OP']['json_input_rows_predicate'][bgp['P']] = str(bgp['input_rows'])
                operators['GF_FROM_OP']['json_cardinality'][bgp['P']] = str(bgp['cardinality_estimate'])
                operators['GF_FROM_OP']['json_cardinality_fanout'][bgp['P']] = str(bgp['cardinality_fanout'])

            else:
                operators['GF_FROM_OP']['json_time_predicate'][str(k)+'var_'+str(var_count)] = str(bgp['time'])
                operators['GF_FROM_OP']['json_fanout_predicate'][str(k)+'var_'+str(var_count)] = str(bgp['fanout'])
                operators['GF_FROM_OP']['json_input_rows_predicate'][str(k)+'var_'+str(var_count)] = str(bgp['input_rows'])
                operators['GF_FROM_OP']['json_cardinality'][str(k)+'var_'+str(var_count)] = str(bgp['cardinality_estimate'])
                operators['GF_FROM_OP']['json_cardinality_fanout'][str(k)+'var_'+str(var_count)] = str(bgp['cardinality_fanout'])
                var_count += 1
            repeat_list_op.append(bgp['P'])
            repeat_list_time.append(bgp['time'])
            repeat_list_fanout.append(bgp['fanout'])
            repeat_list_input_rows.append(bgp['input_rows'])
            repeat_list_cardinality_estimate.append(bgp['cardinality_estimate'])
            repeat_list_cardinality_fanout.append(bgp['cardinality_fanout'])

    if len(repeat_list_op) != len(set(repeat_list_op)):
        c = 0
        for i in range(len(repeat_list_op)):
            if repeat_list_op[i] in repeat_list_op[i+1:]:
                operators['GF_FROM_OP']['json_time_predicate_wr'][repeat_list_op[i] + "_" + str(c)] = str(repeat_list_time[i])
                operators['GF_FROM_OP']['json_fanout_predicate_wr'][repeat_list_op[i] + "_" + str(c)] = str(repeat_list_fanout[i])
                operators['GF_FROM_OP']['json_input_rows_predicate_wr'][repeat_list_op[i] + "_" + str(c)] = str(repeat_list_input_rows[i])
                operators['GF_FROM_OP']['json_cardinality_fanout_wr'][repeat_list_op[i] + "_" + str(c)] = str(repeat_list_cardinality_estimate[i])
                operators['GF_FROM_OP']['json_cardinality_wr'][repeat_list_op[i] + "_" + str(c)] = str(repeat_list_cardinality_fanout[i])
                c += 1
            else:
                operators['GF_FROM_OP']['json_time_predicate_wr'][repeat_list_op[i]] = str(repeat_list_time[i])
                operators['GF_FROM_OP']['json_fanout_predicate_wr'][repeat_list_op[i]] = str(repeat_list_fanout[i])
                operators['GF_FROM_OP']['json_input_rows_predicate_wr'][repeat_list_op[i]] = str(repeat_list_input_rows[i])
                operators['GF_FROM_OP']['json_cardinality_fanout_wr'][repeat_list_op[i]] = str(repeat_list_cardinality_fanout[i])
                operators['GF_FROM_OP']['json_cardinality_wr'][repeat_list_op[i]] = str(repeat_list_cardinality_estimate[i])




        for i in set(repeat_list_op):
            #print(ListDuplicatesOf(repeat_list_op, i))
            duplicates_index = ListDuplicatesOf(repeat_list_op, i)
            #if len(duplicates_index) > 1:
            suma_time = 0
            suma_fanout = 0
            suma_input_rows = 0
            suma_cardinality_estimate = 0
            suma_cardinality_fanout = 0
            for j in duplicates_index:
                suma_time += repeat_list_time[j]
                suma_fanout += repeat_list_fanout[j]
                suma_input_rows += repeat_list_input_rows[j]
                suma_cardinality_estimate += repeat_list_cardinality_estimate[j]
                suma_cardinality_fanout += repeat_list_cardinality_fanout[j]
            operators['GF_FROM_OP']['json_time_predicate_wrs'][i] = str(suma_time)
            operators['GF_FROM_OP']['json_fanout_predicate_wrs'][i] = str(suma_fanout)
            operators['GF_FROM_OP']['json_input_rows_predicate_wrs'][i] = str(suma_input_rows)
            operators['GF_FROM_OP']['json_cardinality_fanout_wrs'][i] = str(suma_cardinality_fanout)
            operators['GF_FROM_OP']['json_cardinality_wrs'][i] = str(suma_cardinality_estimate)
            ########################################################
            operators['GF_FROM_OP']['json_time_predicate_wrm'][i] = str(suma_time/len(duplicates_index))
            operators['GF_FROM_OP']['json_fanout_predicate_wrm'][i] = str(suma_fanout/len(duplicates_index))
            operators['GF_FROM_OP']['json_input_rows_predicate_wrm'][i] = str(suma_input_rows/len(duplicates_index))
            operators['GF_FROM_OP']['json_cardinality_fanout_wrm'][i] = str(suma_cardinality_fanout/len(duplicates_index))
            operators['GF_FROM_OP']['json_cardinality_wrm'][i] = str(suma_cardinality_estimate/len(duplicates_index))
    else:
        operators['GF_FROM_OP']['json_time_predicate_wrs'] = operators['GF_FROM_OP']['json_time_predicate']
        operators['GF_FROM_OP']['json_fanout_predicate_wrs'] = operators['GF_FROM_OP']['json_fanout_predicate']
        operators['GF_FROM_OP']['json_input_rows_predicate_wrs'] = operators['GF_FROM_OP']['json_input_rows_predicate']
        operators['GF_FROM_OP']['json_cardinality_fanout_wrs'] = operators['GF_FROM_OP']['json_cardinality_fanout']
        operators['GF_FROM_OP']['json_cardinality_wrs'] = operators['GF_FROM_OP']['json_cardinality']
        #############################################################################################
        operators['GF_FROM_OP']['json_time_predicate_wrm'] = operators['GF_FROM_OP']['json_time_predicate']
        operators['GF_FROM_OP']['json_fanout_predicate_wrm'] = operators['GF_FROM_OP']['json_fanout_predicate']
        operators['GF_FROM_OP']['json_input_rows_predicate_wrm'] = operators['GF_FROM_OP']['json_input_rows_predicate']
        operators['GF_FROM_OP']['json_cardinality_fanout_wrm'] = operators['GF_FROM_OP']['json_cardinality_fanout']
        operators['GF_FROM_OP']['json_cardinality_wrm'] = operators['GF_FROM_OP']['json_cardinality']

    return operators