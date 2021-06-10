import re, time, os, json
from functions.main import GetFinalResults, GroupOperators, GetOperatorExecutionFeatures, IdentifyOperatorType, IdentifyPrecode, IdentifyAfterCode, IdentifyGroupBy, IdentifyDistinct, GetGSPO, GetIRI_ID, GetAllPredicatesFromProfile, SetBooleanPredicates, GetStartAndEndOptionalSection, SetBooleanOptionalSection, SetTargetAndTransitive, SetSorts, SetSubqueries, SetAfterTest
from functions.aux import GetSubstring, ParseNestedBracket, CleanOperators, GetPrefixes, VectorString, MainCurlyBrackets, CountCurlyBrackets, CleanSalts

#current working directory
cwd = os.getcwd()
path_profiles = os.listdir(os.getcwd()+"/scripts/outputs")

if not os.path.exists(os.getcwd()+'/scripts/feature_extraction_script/returns/'):
	os.makedirs(os.getcwd()+'/scripts/feature_extraction_script/returns/')


def execute(profile_sparql,profile_low_explain):
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
		operators[i] = CountCurlyBrackets(operators[i])
		operators[i] = GetStartAndEndOptionalSection(operators[i], i)
	predicates_list = GetAllPredicatesFromProfile(operators)
	operators = SetBooleanPredicates(operators, predicates_list)
	operators = SetBooleanOptionalSection(operators)
	operators = SetAfterTest(operators)
	operators = SetTargetAndTransitive(operators)
	operators = SetSorts(operators)
	operators = SetSubqueries(operators)

	#operators = AddMissingFeatures(operators)
	return operators, predicates_list


def test_print():
	for k,v in operators.items():
		print(k, v)

#f = open("demofile.txt", "w")
for i in path_profiles:
	if os.path.isdir(os.getcwd()+"/scripts/outputs/"+i):
		filename = "_".join(i.split("_")[1:])

		profile_normal = open(os.getcwd()+"/scripts/outputs/outputs_"+filename+"/profile_normal_file_"+filename, 'r', encoding='latin-1').read()
		profile_explain_bajo = open(os.getcwd() + "/scripts/outputs/outputs_" + filename + "/profile_normal_explain_bajo_" + filename, 'r', encoding='latin-1').read()

		operators, predicates_list = execute(profile_normal,profile_explain_bajo)
		#operators_bajo, predicates_list_bajo = execute(profile_explain_bajo)
		'''
		if (predicates_list_bajo != predicates_list):
			f.write("DESIGUAL: " + filename)
			f.write("\n")
			f.write("normal: \n" + str(predicates_list))
			f.write("\n")
			f.write("bajo: \n" + str(predicates_list_bajo))
			f.write("\n")
			f.write("\n")
			f.write("\n")
		else:
			f.write("IGUAL: " + filename)
			f.write("\n")
			f.write("normal: \n" + str(predicates_list))
			f.write("\n")
			f.write("bajo: \n" + str(predicates_list_bajo))
			f.write("\n")
			f.write("\n")
			f.write("\n")
		'''
		with open(os.getcwd()+'/scripts/feature_extraction_script/returns/'+filename+'.json', 'w') as json_file:
			json.dump(operators, json_file)

#f.close()