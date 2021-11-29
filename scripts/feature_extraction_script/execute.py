import os, json, csv, subprocess, shutil
from functions.main import GetFinalResults, GroupOperators, GetOperatorExecutionFeatures, IdentifyOperatorType, \
	IdentifyPrecode, IdentifyAfterCode, IdentifyGroupBy, IdentifyDistinct, IdentifyTOP, IdentifyTopOrderByRead, \
	IdentifySkipNode, IdentifySelect, GetGSPO, GetIRI_ID, GetAllPredicatesFromProfile, SetBooleanPredicates, \
	GetStartAndEndOptionalSection, SetBooleanOptionalSection, SetTargetAndTransitive, SetSorts, SetSubqueries, \
	SetAfterTest, SetTripleType, SetGSPODefault, IdentifyBGPS, IdentifyUnionFeatures, IdentifyEndNode, IdentifyAllEq, \
	IdentifyIter
from functions.aux import GetSubstring, ParseNestedBracket, CleanOperators, GetPrefixes, VectorString, MainCurlyBrackets, \
	CountCurlyBrackets, CleanSalts, SubstractStrings, GetAllSubstring
from functions.build_csv import AllData, FullDataframe, FullDataframe_old

execute_new = True
execute_old = True

name_new = 'new_dataset_5.5_full'
name_old = 'old_dataset_2.5_full'

#error_list_new = []
error_list_new = ['query_5673','query_6072','query_17945']
error_data_deleted_new = len(error_list_new)

#current working directory
csv_path = '/home/c161905/Memoria/memoria_utfsm_sparql/scripts/csv_files/'
cwd = os.getcwd()
path_profiles = os.listdir(os.getcwd()+"/scripts/outputs")
path_profiles_str = os.getcwd()+"/scripts/outputs"

path_profiles_old = os.listdir(os.getcwd()+"/scripts/outputs_old")
path_profiles_str_old = os.getcwd()+"/scripts/outputs_old"

path_dataset = os.listdir(os.getcwd()+"/dataset")
c = 0
symbol = "ᶲ"

if os.path.exists(os.getcwd()+'/scripts/feature_extraction_script/returns/'):
	shutil.rmtree(os.getcwd()+'/scripts/feature_extraction_script/returns/')
if os.path.exists(os.getcwd()+'/scripts/feature_extraction_script/returns_old/'):
	shutil.rmtree(os.getcwd()+'/scripts/feature_extraction_script/returns_old/')
os.makedirs(os.getcwd()+'/scripts/feature_extraction_script/returns/')
os.makedirs(os.getcwd()+'/scripts/feature_extraction_script/returns_old/')


def execute(profile_sparql, profile_low_explain, sparql_file):
	operators = MainCurlyBrackets(profile_sparql)
	operators_low_explain = MainCurlyBrackets(profile_low_explain)
	operators = GroupOperators(operators, operators_low_explain)
	operators = CleanOperators(operators)
	still_not_optional_possibility = 0
	for i in operators.keys():
		operators[i] = GetOperatorExecutionFeatures(operators[i])
		operators[i] = IdentifyOperatorType(operators[i])
		operators[i] = IdentifyPrecode(operators[i])
		operators[i] = IdentifyAfterCode(operators[i])
		operators[i] = IdentifyGroupBy(operators[i])
		operators[i] = IdentifyDistinct(operators[i])
		operators[i] = GetGSPO(operators[i])
		operators[i] = SetGSPODefault(operators[i])
		operators[i] = IdentifyTOP(operators[i])
		operators[i] = IdentifyTopOrderByRead(operators[i])
		operators[i] = IdentifySkipNode(operators[i])
		operators[i] = CountCurlyBrackets(operators[i])
		operators[i] = IdentifySelect(operators[i])
		operators[i] = IdentifyEndNode(operators[i])
		operators[i], still_not_optional_possibility = GetStartAndEndOptionalSection(operators[i], sparql_file, still_not_optional_possibility)
	predicates_list = GetAllPredicatesFromProfile(operators)
	operators = SetBooleanPredicates(operators, predicates_list)
	operators = SetBooleanOptionalSection(operators)
	operators = SetAfterTest(operators)
	operators = SetTargetAndTransitive(operators)
	operators = SetSorts(operators)
	operators = SetSubqueries(operators)
	operators = IdentifyUnionFeatures(operators, sparql_file)
	operators = IdentifyBGPS(operators)
	operators = IdentifyIter(operators)
	operators, list_alleq = IdentifyAllEq(operators)
	operators = SetTripleType(operators, sparql_file, list_alleq)
	return operators, predicates_list, list_alleq


def test_print():
	for k,v in operators.items():
		print(k, v)


dataframe_test = []
dataframe_train = []
full_dataframe = []

lst = [
	0,
	18,
	1586,
	3937,
	10382,
	12616,
	22,
	30,
	87,
	275,
	444,
	457,
	459,
	486,
	556,
	666,
	766,
	800,
	926,
	961,
	998,
	1248,
	1335,
	1956,
	2106,
	2119,
	2303,
	2710,
	3222,
	3570,
	3599,
	4732,
	4878,
	5095,
	5429,
	6985,
	7428,
	7788,
	8358,
	8811,
	8830,
	8930,
	8932,
	9568,
	9691,
	10553,
	10757,
	10874,
	10969,
	11000,
	11725,
	12008,
	12015,
	12036,
	12245,
	12345,
	12463,
	12664,
	13248,
	13456,
	13963,
	14233,
	16640,
	16790,
	17865,
	18985,
	21291,
	22004,
	22088,
	23391,
	24200,
	25068,
	25390,
	25515,
	25797,
]

#lst = [8930,10969,24200]


if execute_new:
	for i in path_profiles:
		if os.path.isdir(path_profiles_str+"/"+i):
			total_data_proccesed = os.path.isdir(path_profiles_str+"/"+i)
			filename = "_".join(i.split("_")[1:])
			if filename not in error_list_new:
				print("filename: ", filename)
				sparql_file = open(path_profiles_str + "/outputs_" + filename + "/" + filename + ".rq", 'r', encoding='latin-1').read()
				profile_normal = open(path_profiles_str + "/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin-1').read()
				profile_explain_bajo = open(path_profiles_str + "/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin').read()
				#general_features_pt_file = open(path_profiles_str + "/outputs_" + filename + "/gfeatures_" + filename, 'r', encoding='latin-1').read()
				general_features_pt_file = 0
				#old_features_json = open(path_profiles_str + "/outputs_" + filename + "/" + filename + ".json", 'r', encoding='latin-1').read()
				old_features_json = 0
				if profile_normal == '':
					error_data_deleted_new += 1
					print("profile error")
					continue
				operators, predicates_list, list_alleq = execute(profile_normal, profile_explain_bajo, sparql_file)
				all_data = AllData(operators, profile_normal, predicates_list, filename, sparql_file, general_features_pt_file, list_alleq, old_features_json, symbol, True)
				full_dataframe.append(all_data)
				with open(os.getcwd()+'/scripts/feature_extraction_script/returns/'+filename+'.json', 'w') as json_file:
					json.dump(operators, json_file)

if execute_old:
	operators, predicates_list, list_alleq = {}, [], []
	for i in path_profiles_old:
		if os.path.isdir(path_profiles_str_old+"/"+i):
			filename = "_".join(i.split("_")[1:])
			#if all(e != filename for e in ['queries1_696', 'queries1_57']): #and "queries1" in filename:
			#if "queries2" in filename:
			#if any(('queries2_'+str(e)) == filename for e in lst):
			#if "queries2_25390" in filename:
			if True:
				print("filename: ", filename)
				sparql_file = open(path_profiles_str_old + "/outputs_" + filename + "/" + filename + ".rq", 'r', encoding='latin-1').read()
				profile_normal = open(path_profiles_str_old + "/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin-1').read()
				profile_explain_bajo = open(path_profiles_str_old + "/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin').read()
				#general_features_pt_file = open(path_profiles_str + "/outputs_" + filename + "/gfeatures_" + filename, 'r', encoding='latin-1').read()
				general_features_pt_file = 0
				old_features_json = open(path_profiles_str_old + "/outputs_" + filename + "/" + filename + ".json", 'r', encoding='latin-1').read()
				if profile_normal == '':
					print("profile error")
					continue
				operators, predicates_list, list_alleq = execute(profile_normal, profile_explain_bajo, sparql_file)
				all_data = AllData(operators, profile_normal, predicates_list, filename, sparql_file, general_features_pt_file, list_alleq, old_features_json, symbol, False)
				if "queries1" in filename:
					dataframe_test.append(all_data)
				if "queries2" in filename:
					dataframe_train.append(all_data)
				with open(os.getcwd()+'/scripts/feature_extraction_script/returns_old/'+filename+'.json', 'w') as json_file:
					json.dump(operators, json_file)




if execute_old:
	df_test = FullDataframe_old(dataframe_test)
	df_train = FullDataframe_old(dataframe_train)
	df_train.to_csv(csv_path + 'train_' + name_old + '.csv', index=False)
	df_test.to_csv(csv_path + 'test_' + name_old + '.csv', index=False)

if execute_new:
	df_full = FullDataframe(full_dataframe)
	df_full.to_csv(csv_path + name_new + '.csv', index=False)
	print("----------------------------")
	print("Numero de archivos revisados:", total_data_proccesed)
	print("Número de errores eliminados: ", error_data_deleted_new)
	print("----------------------------")

if execute_new:
	subprocess.call("/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script/pretty.sh")

if execute_old:
	subprocess.call("/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script/pretty_old.sh")