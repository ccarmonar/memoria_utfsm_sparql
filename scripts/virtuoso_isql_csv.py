import os, time, shutil, pandas as pd
from subprocess import call
start_time = time.time()

os.chdir('/home/c161905/Memoria/memoria_utfsm_sparql')

cwd = os.getcwd()
print(cwd)
path_dataset = cwd+"/dataset/queries/"





if not os.path.exists(cwd+'/scripts/temp/'):
    os.makedirs(cwd+'/scripts/temp/')

for file in os.listdir(path_dataset):
    if file.endswith(".csv"):
        df = pd.read_csv(path_dataset+file, engine='python', encoding='utf-8')
        values_list = df.values.tolist()
        columns = list(df.columns)

        for values in values_list:
            id = values[0]
            query = values[1]
            time = values[2]
            assign = values[3]
            bgp = values[4]
            distinct = values[5]
            extend = values[6]
            filter = values[7]
            filter_bound = values[8]
            filter_contains = values[9]
            filter_eq = values[10]
            filter_exists = values[11]
            filter_ge = values[12]
            filter_gt = values[13]
            filter_isBlank = values[14]
            filter_isIRI = values[15]
            filter_isLiteral = values[16]
            filter_lang = values[17]
            filter_langMatches = values[18]
            filter_le = values[19]
            filter_lt = values[20]
            filter_ne = values[21]
            filter_not = values[22]
            filter_notexists = values[23]
            filter_or = values[24]
            filter_regex = values[25]
            filter_sameTerm = values[26]
            filter_str = values[27]
            filter_strends = values[28]
            filter_strstarts = values[29]
            filter_subtract = values[30]
            graph = values[31]
            group = values[32]
            has_slice = values[33]
            join = values[34]
            json_cardinality = values[35]
            leftjoin = values[36]
            max_slice_limit = values[37]
            max_slice_start = values[38]
            minus = values[39]
            multi = values[40]
            notoneof = values[41]
            order = values[42]
            path_start = values[43]
            path_plus = values[44]
            path_question = values[45]
            pathN_start = values[46]
            pathN_plus = values[47]
            pcs0 = values[48]
            pcs1 = values[49]
            pcs10 = values[50]
            pcs11 = values[51]
            pcs12 = values[52]
            pcs13 = values[53]
            pcs14 = values[54]
            pcs15 = values[55]
            pcs16 = values[56]
            pcs17 = values[57]
            pcs18 = values[58]
            pcs19 = values[59]
            pcs2 = values[60]
            pcs20 = values[61]
            pcs21 = values[62]
            pcs22 = values[63]
            pcs23 = values[64]
            pcs24 = values[65]
            pcs3 = values[66]
            pcs4 = values[67]
            pcs5 = values[68]
            pcs6 = values[69]
            pcs7 = values[70]
            pcs8 = values[71]
            pcs9 = values[72]
            project = values[73]
            reduced = values[74]
            sequence = values[75]
            slice = values[76]
            tolist = values[77]
            top = values[78]
            tree_tdb = values[79]
            trees = values[80]
            treesize = values[81]
            triple = values[82]
            union = values[83]
            query_name = values[84]

            tempfile = open(cwd + '/scripts/temp/' + query_name + '.rq', "w+")
            tempfile.write(query)
            tempfile.close()

            dict_aux = {}
            for c, v in zip(columns,values):
                dict_aux[c] = v

            tempfile_json = open(cwd + '/scripts/temp/' + query_name + ".json", "w")
            tempfile_json.write(str(dict_aux))
            tempfile_json.close()

            #command = cwd + "/scripts/./virtuoso_isql_exec_csv.sh " + query_name+".rq"
            #rc = call(command, shell=True)

print("Se creo el folder temp")
os.chdir('/home/c161905/Memoria/memoria_utfsm_sparql/scripts')
command = "./virtuoso_isql_exec_varius_csv.sh"
rc = call(command, shell=True)
print("Se crearon ouputs con los profiles y otros archivos necesarios")


shutil.rmtree(cwd+'/scripts/temp/')
print("Se elimino carpeta temporal")

end_time = time.time()
print("--- %s seconds ---" % (time.time() - start_time))
