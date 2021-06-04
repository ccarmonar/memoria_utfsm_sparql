import re, time, os, json
from functions.main import GetFinalResults,GroupOperators,GetOperatorExecutionFeatures,IdentifyOperatorType,IdentifyPrecode,IdentifyAfterCode,GetGSPO,GetIRI_ID,GetAllPredicatesFromProfile,SetBooleanPredicates
from functions.aux import GetSubstring,ParseNestedBracket,CleanOperators,GetPrefixes,VectorString,MainCurlyBrackets

#current working directory
cwd = os.getcwd()
path_profiles = os.listdir(os.getcwd()+"/scripts/sparql_profiles")

if not os.path.exists(os.getcwd()+'/scripts/feature_extraction_script/returns/'):
	os.makedirs(os.getcwd()+'/scripts/feature_extraction_script/returns/')


def execute(profile_sparql):
	operators = MainCurlyBrackets(profile_sparql)
	operators = GroupOperators(operators)
	operators = CleanOperators(operators)
	for i in operators.keys():
		operators[i] = GetOperatorExecutionFeatures(operators[i])
		operators[i] = IdentifyOperatorType(operators[i])
		operators[i] = IdentifyPrecode(operators[i])
		operators[i] = IdentifyAfterCode(operators[i])
		operators[i] = GetGSPO(operators[i])
	predicates_list = GetAllPredicatesFromProfile(operators)
	operators = SetBooleanPredicates(operators, predicates_list)
	return operators, predicates_list


def test_print():
	for k,v in operators.items():
		print(k, v)

#operators, predicates_list = execute(profile_sparql)
#print("+++++++++++++")
#print(sparql_file)
#print("+++++++++++++")
#print("+++++++++++++")
#print(predicates_list)
#print("+++++++++++++")
#test_print()

#with open('operators.json', 'w') as json_file:
	#json.dump(operators, json_file)





for i in path_profiles:
	profile_sparql = open(os.getcwd()+"/scripts/sparql_profiles/"+i, 'r', encoding='latin-1').read()
	operators, predicates_list = execute(profile_sparql)
	with open(os.getcwd()+'/scripts/feature_extraction_script/returns/'+i+'.json', 'w') as json_file:
		json.dump(operators, json_file)








