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
        query_list = df.values.tolist()
        for query in query_list:
            name = query[0]
            #if 'test_wikidata14' not in name:
            if True:
                query = query[1]
                if name.endswith(".rq"):
                    name = name.replace(".rq","")

                tempfile = open(cwd+'/scripts/temp/'+name+'.rq',"w+")
                tempfile.write(query)
                tempfile.close()
            #command = cwd + "/scripts/./virtuoso_isql_exec_csv.sh " + name+".rq"
            #rc = call(command, shell=True)

print("Se creo el folder temp")
os.chdir('/home/c161905/Memoria/memoria_utfsm_sparql/scripts')
command = "./virtuoso_isql_exec_varius_csv.sh"
rc = call(command, shell=True)
print("Se crearon ouputs con los profiles y otros archivos necesarios")


shutil.rmtree(cwd+'/scripts/temp/')
print("Se elimino carpeta temporal")

print("--- %s seconds ---" % (time.time() - start_time))
