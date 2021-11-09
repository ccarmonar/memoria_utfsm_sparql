from functions.aux import MainCurlyBrackets, OnlyScans
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


def GeneralFeaturesFromOperators(operators, list_alleq):
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
    for k in operators['GF_FROM_OP']['bgps_ops'].keys():
        bgp_list = operators['GF_FROM_OP']['bgps_ops'][k]['bgp_list']
        for bgp in bgp_list:
            if 'NONE' not in bgp['P']:
                operators['GF_FROM_OP']['json_time_predicate'][bgp['P']] = bgp['time']
                operators['GF_FROM_OP']['json_fanout_predicate'][bgp['P']] = bgp['fanout']
                operators['GF_FROM_OP']['json_input_rows_predicate'][bgp['P']] = bgp['input_rows']
                operators['GF_FROM_OP']['json_cardinality'][bgp['P']] = bgp['cardinality_estimate']
                operators['GF_FROM_OP']['json_cardinality_fanout'][bgp['P']] = bgp['cardinality_fanout']
    return operators