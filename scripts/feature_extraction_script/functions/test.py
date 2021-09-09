import os, json
path = "/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script/returns/"
path_profiles = os.listdir(path)

limit = 0
list = []
count_ls = []
count = 0
for i in path_profiles:
    if "queries2" in i:
        f = open(path+i)
        data = json.load(f)
        for k, v in data['GF_FROM_OP'].items():
            if k == "bgps_ops":
                for bgp_list in v.values():
                    for bgp in bgp_list['bgp_list']:
                        if 'all_eq' in bgp['S'] or 'all_eq' in bgp['P'] or 'all_eq' in bgp['O']:
                            count += bgp['S'].count('all_eq') + bgp['P'].count('all_eq') + bgp['O'].count('all_eq')


                            #print(bgp['S'],bgp['P'],bgp['O'])


                            list.append(int(i.split("_")[1].split(".")[0]))
        print(i)
        print(count)
        count = 0
h = sorted(set(list))
print(h)
print(len(h))