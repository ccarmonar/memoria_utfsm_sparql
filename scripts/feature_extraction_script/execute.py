import os, json, csv
from functions.main import GetFinalResults, GroupOperators, GetOperatorExecutionFeatures, IdentifyOperatorType, \
	IdentifyPrecode, IdentifyAfterCode, IdentifyGroupBy, IdentifyDistinct, IdentifyTOP, IdentifyTopOrderByRead, \
	IdentifySkipNode, IdentifySelect, GetGSPO, GetIRI_ID, GetAllPredicatesFromProfile, SetBooleanPredicates, \
	GetStartAndEndOptionalSection, SetBooleanOptionalSection, SetTargetAndTransitive, SetSorts, SetSubqueries, \
	SetAfterTest, SetTripleType, SetGSPODefault, IdentifyBGPS, IdentifyUnionFeatures, IdentifyEndNode, IdentifyAllEq, \
	IdentifyIter
from functions.aux import GetSubstring, ParseNestedBracket, CleanOperators, GetPrefixes, VectorString, MainCurlyBrackets, \
	CountCurlyBrackets, CleanSalts, SubstractStrings, GetAllSubstring
from functions.build_csv import AllData, FullDataframe
#current working directorya
cwd = os.getcwd()
path_profiles = os.listdir(os.getcwd()+"/scripts/outputs")
path_dataset = os.listdir(os.getcwd()+"/dataset")
c = 0

if not os.path.exists(os.getcwd()+'/scripts/feature_extraction_script/returns/'):
	os.makedirs(os.getcwd()+'/scripts/feature_extraction_script/returns/')


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


dataframe = []

lst = [
	0,
	22,
	275,
	457,
	556,
	666,
	766,
	800,
	998,
	1248,
	2303,
	3222,
	3599,
	4732,
	5095,
	5429,
	6985,
	7788,
	8358,
	8830,
	8930,
	8932,
	9568,
	10757,
	10969,
	11000,
	11725,
	12008,
	12345,
	12664,
	13456,
	13963,
	14233,
	16790,
	17865,
	21291,
	22004,
	22088,
	23391,
	24200
]
#lst = [30, 87, 444, 459, 961, 1335, 1956, 2119, 2710, 2106, 10553, 12015, 18985, 25068, 25797]
#lst = [30,25068,1335,2710]
#lst = [486, 929, 3570, 4878, 7428, 8811, 9691, 10874, 12036, 12245, 12463, 13248, 16640, 25390, 25515]

lst = [
	0,
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

for i in path_profiles:
	if os.path.isdir(os.getcwd()+"/scripts/outputs/"+i):
		filename = "_".join(i.split("_")[1:])
		#if all(e != filename for e in ['queries1_696', 'queries1_57']): #and "queries1" in filename:
		#if "queries2" in filename:
		if any(('queries2_'+str(e)) == filename for e in lst):
			print("filename: ", filename)
			sparql_file = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/" + filename + ".rq", 'r', encoding='latin-1').read()
			profile_normal = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin-1').read()
			profile_explain_bajo = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin').read()
			general_features_pt_file = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/gfeatures_" + filename, 'r', encoding='latin-1').read()
			old_features_json = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/" + filename + ".json", 'r', encoding='latin-1').read()
			if profile_normal == '':
				print("profile error")
				continue
			operators, predicates_list, list_alleq = execute(profile_normal, profile_explain_bajo, sparql_file)
			all_data = AllData(operators, profile_normal, predicates_list, filename, sparql_file, general_features_pt_file, list_alleq, old_features_json)
			dataframe.append(all_data)
			with open(os.getcwd()+'/scripts/feature_extraction_script/returns/'+filename+'.json', 'w') as json_file:
				json.dump(operators, json_file)


df = FullDataframe(dataframe)
df.to_csv('/home/c161905/Memoria/memoria_utfsm_sparql/scripts/csv_files/test_example.csv', index=False)

#print(df.to_csv('/home/c161905/Memoria/memoria_utfsm_sparql/scripts/csv_files/test_example.csv', index=False))


import subprocess
subprocess.call("/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script/pretty.sh")
