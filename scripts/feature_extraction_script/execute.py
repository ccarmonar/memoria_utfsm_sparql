import os, json, csv
from functions.main import GetFinalResults, GroupOperators, GetOperatorExecutionFeatures, IdentifyOperatorType, IdentifyPrecode, IdentifyAfterCode, IdentifyGroupBy, IdentifyDistinct, IdentifyTOP, IdentifyTopOrderByRead, IdentifySkipNode, IdentifySelect, GetGSPO, GetIRI_ID, GetAllPredicatesFromProfile, SetBooleanPredicates, GetStartAndEndOptionalSection, SetBooleanOptionalSection, SetTargetAndTransitive, SetSorts, SetSubqueries, SetAfterTest, SetTripleType, SetGSPODefault, IdentifyBGPS
from functions.aux import GetSubstring, ParseNestedBracket, CleanOperators, GetPrefixes, VectorString, MainCurlyBrackets, CountCurlyBrackets, CleanSalts, SubstractStrings
from functions.build_csv import AllData, FullDataframe
#current working directorya
cwd = os.getcwd()
path_profiles = os.listdir(os.getcwd()+"/scripts/outputs")
path_dataset = os.listdir(os.getcwd()+"/dataset")
c = 0

if not os.path.exists(os.getcwd()+'/scripts/feature_extraction_script/returns/'):
	os.makedirs(os.getcwd()+'/scripts/feature_extraction_script/returns/')


def execute(profile_sparql, profile_low_explain):
	operators = MainCurlyBrackets(profile_sparql)
	operators_low_explain = MainCurlyBrackets(profile_low_explain)
	operators = GroupOperators(operators, operators_low_explain)
	operators = CleanOperators(operators)
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
		operators[i] = GetStartAndEndOptionalSection(operators[i], i)
	predicates_list = GetAllPredicatesFromProfile(operators)
	operators = SetBooleanPredicates(operators, predicates_list)
	operators = SetBooleanOptionalSection(operators)
	operators = SetAfterTest(operators)
	operators = SetTargetAndTransitive(operators)
	operators = SetSorts(operators)
	operators = SetSubqueries(operators)
	operators = IdentifyBGPS(operators)
	operators = SetTripleType(operators)
	return operators, predicates_list


def test_print():
	for k,v in operators.items():
		print(k, v)


dataframe = []


for i in path_profiles:
	if os.path.isdir(os.getcwd()+"/scripts/outputs/"+i):
		filename = "_".join(i.split("_")[1:])


		#if all(e != filename for e in ['queries1_696', 'queries1_57']) and "queries1" in filename:
		'''
		if filename == "queries4_test_wikidata22" \
				or "test_wikidata" in filename \
				or filename == "queries3_ex052" \
				or filename == "queries3_ex021" \
				or filename == "queries2_4" \
				or filename == "queries2_5" \
				or filename == "queries2_36" \
				or filename == "queries2_78" \
				or filename == "queries2_196" \
				or filename == "queries2_30" \
				or filename == "queries1_16" \
				or filename == "queries2_84" \
				or filename == "queries2_87" \
				or filename == "queries2_187" \
				or filename == "queries2_3000" \
				or filename == "queries2_23000" \
				or filename == "queries2_7800"   \
				or filename == "queries3_ex010" \
				or filename == "queries3_ex015" \
				or filename == "queries3_ex021" \
				or filename == "queries3_q1" \
				or filename == "queries3_q2" \
				or filename == "queries3_q3" \
				or filename == "queries3_q4" \
				or filename == "queries3_q5" \
				or filename == "queries3_q6":
		'''
		if	filename == "queries2_24200" \
			or filename == "queries2_0" \
			or filename == "queries2_22"\
			or filename == "queries2_22004" \
			or filename == "queries2_2303" \
			or filename == "queries2_10969" \
			or filename == "queries2_13963"\
			or filename == "queries2_14351":
			#	or filename == "queries2_0" \
			#	or filename == "queries2_8830"\
			#	or filename == "queries2_0" \
			#	or filename == "queries2_22" \
			#	or filename == "queries2_8830"\
			#	or filename == "queries2_5429"\
			#	or filename == "queries2_3222"\
			#	or filename == "queries2_8358":
			print("filename: ", filename)
			sparql_file = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/" + filename + ".rq", 'r', encoding='latin-1').read()
			profile_normal = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin-1').read()
			profile_explain_bajo = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/profile_normal_file_" + filename, 'r', encoding='latin').read()
			general_features_pt_file = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/gfeatures_" + filename, 'r', encoding='latin-1').read()
			'''
				if 'wikidata' in filename:
					sparql_file = open("/home/c161905/Memoria/memoria_utfsm_sparql/scripts/sparql_files/wikidata_queries/"+filename+".rq", 'r', encoding = 'latin-1').read()
				else:
					sparql_file = open("/home/c161905/Memoria/memoria_utfsm_sparql/scripts/sparql_files/" + filename + ".rq", 'r', encoding = 'latin-1').read()
			'''
			if profile_normal == '':
				print("profile error")
				continue
			operators, predicates_list = execute(profile_normal, profile_explain_bajo)
			all_data = AllData(operators, profile_normal, predicates_list, filename, sparql_file, general_features_pt_file)
			#dataframe.append(all_data)
			with open(os.getcwd()+'/scripts/feature_extraction_script/returns/'+filename+'.json', 'w') as json_file:
				json.dump(operators, json_file)


df = FullDataframe(dataframe)
df.to_csv('/home/c161905/Memoria/memoria_utfsm_sparql/scripts/csv_files/test_example.csv', index=False)

#print(df.to_csv('/home/c161905/Memoria/memoria_utfsm_sparql/scripts/csv_files/test_example.csv', index=False))

