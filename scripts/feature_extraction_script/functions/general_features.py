from functions.aux import MainCurlyBrackets

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
            'ql_rt_msec': '0',
            'ql_rt_clocks': '0',
            'ql_rnd_rows': '0',
            'ql_seq_rows': '0',
            'ql_same_seg': '0',
            'ql_same_page': '0',
            'ql_disk_reads': '0',
            'ql_spec_disk_reads': '0',
            'ql_cl_wait_clocks': '0'
        },
        'compiled' : {
            'ql_c_msec': '0',
            'ql_c_disk': '0',
            'ql_c_clocks': '0',
            'ql_cl_messages': '0',
            'ql_c_cl_wait': '0'
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
            operators['GENERAL_FEATURES']['precompiled']['ql_rt_msec'] = precompilation_list[i-1]
        if precompilation_list[i] == 'cpu,':
            operators['GENERAL_FEATURES']['precompiled']['ql_rt_clocks'] = precompilation_list[i-1]
        if precompilation_list[i] == 'rnd':
            operators['GENERAL_FEATURES']['precompiled']['ql_rnd_rows'] = precompilation_list[i-1]
        if precompilation_list[i] == 'seq':
            operators['GENERAL_FEATURES']['precompiled']['ql_seq_rows'] = precompilation_list[i-1]
        if precompilation_list[i] == 'same' and precompilation_list[i+1] == 'seg':
            operators['GENERAL_FEATURES']['precompiled']['ql_same_seg'] = precompilation_list[i-1]
        if precompilation_list[i] == 'same' and precompilation_list[i+1] == 'pg':
            operators['GENERAL_FEATURES']['precompiled']['ql_same_page'] = precompilation_list[i-1]
        if precompilation_list[i] == 'disk' and precompilation_list[i+1] == 'reads':
            operators['GENERAL_FEATURES']['precompiled']['ql_disk_reads'] = precompilation_list[i-1]
        if precompilation_list[i] == 'read' and precompilation_list[i+1] == 'ahead':
            operators['GENERAL_FEATURES']['precompiled']['ql_spec_disk_reads'] = precompilation_list[i-1]
        if precompilation_list[i] == 'wait':
            operators['GENERAL_FEATURES']['precompiled']['ql_cl_wait_clocks'] = precompilation_list[i-1]

    ## COMPILATION
    # ql_c_msec - msec
    # ql_c_disk - reads
    # ql_c_clocks - %read
    # ql_cl_messages - messages
    # ql_c_cl_wait - clw
    for i in range(0,len(compilation_list)):
        if compilation_list[i] == 'msec':
            operators['GENERAL_FEATURES']['compiled']['ql_c_msec'] = compilation_list[i-1]
        if compilation_list[i] == 'reads':
            operators['GENERAL_FEATURES']['compiled']['ql_c_disk'] = compilation_list[i-1]
        if compilation_list[i] == 'read' and compilation_list[i] != 'reads':
            operators['GENERAL_FEATURES']['compiled']['ql_c_clocks'] = compilation_list[i-1]
        if compilation_list[i] == 'messages':
            operators['GENERAL_FEATURES']['compiled']['ql_cl_messages'] = compilation_list[i-1]
        if compilation_list[i] == 'clw':
            operators['GENERAL_FEATURES']['compiled']['ql_c_cl_wait'] = compilation_list[i - 1]

    return operators