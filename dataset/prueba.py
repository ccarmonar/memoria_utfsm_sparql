import os
import pandas as pd
cwd = os.getcwd()
path_db = cwd+'/dataset/'
path_sparql_queries = cwd+'/scripts/sparql_files/'
path_sparql_queries_wikidata = cwd+'/scripts/sparql_files/wikidata_queries/'
wikidata_prefixes = open(cwd+'/scripts/sparql_files/wikidata_queries/wikidata_prefixes', 'r', encoding = 'latin-1').read()

if not os.path.exists(os.getcwd()+'/dataset/queries/'):
    os.makedirs(os.getcwd()+'/dataset/queries/')

c = 0
df1 = pd.read_csv(path_db+'ds_test_pred_filtered.csv', sep='ᶶ', engine='python', encoding='utf-8')
df2 = pd.read_csv(path_db+'ds_trainval_pred_filtered.csv', sep='ᶶ', engine='python', encoding='utf-8')

queries1_aux = wikidata_prefixes + df1['query'].astype(str)
queries1 = queries1_aux.to_list()
list_queries1 = {'name' : [], 'query' : []}
for i in queries1:
    list_queries1['name'].append('queries1_' + str(c))
    list_queries1['query'].append(i)
    c += 1
queries1 = pd.DataFrame(list_queries1, columns=['name','query'])
queries1.to_csv(path_db+'queries/queries1.csv', index=False)


list_queries2 = {'name' : [], 'query' : []}
for file in os.listdir(path_sparql_queries):
    if file.endswith(".rq"):
        list_queries2['name'].append("queries2_" + file)
        sparql_file = open(path_sparql_queries+file, 'r', encoding = 'latin-1').read()
        list_queries2['query'].append(sparql_file)
queries2 = pd.DataFrame(list_queries2, columns=['name','query'])
queries2.to_csv(path_db+'queries/queries2.csv', index=False)


list_queries3 = {'name' : [], 'query' : []}
for file in os.listdir(path_sparql_queries_wikidata):
    if file.endswith(".rq"):
        list_queries3['name'].append("queries3_" + file)
        sparql_file = open(path_sparql_queries_wikidata+file, 'r', encoding = 'latin-1').read()
        list_queries3['query'].append(wikidata_prefixes+'\n'+sparql_file)
queries3 = pd.DataFrame(list_queries3, columns=['name','query'])
queries3.to_csv(path_db+'queries/queries3.csv', index=False)
