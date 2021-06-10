import re, time, os, json
from functions.main import GetFinalResults, GroupOperators, GetOperatorExecutionFeatures, IdentifyOperatorType, IdentifyPrecode, IdentifyAfterCode, IdentifyGroupBy, IdentifyDistinct, GetGSPO, GetIRI_ID, GetAllPredicatesFromProfile, SetBooleanPredicates, GetStartAndEndOptionalSection, SetBooleanOptionalSection, SetTargetAndTransitive, SetSorts, SetSubqueries, SetAfterTest
from functions.aux import GetSubstring, ParseNestedBracket, CleanOperators, GetPrefixes, VectorString, MainCurlyBrackets, CountCurlyBrackets

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


for i in path_profiles:
	profile_sparql = open(os.getcwd()+"/scripts/sparql_profiles/"+i, 'r', encoding='latin-1').read()
	operators, predicates_list = execute(profile_sparql)
	with open(os.getcwd()+'/scripts/feature_extraction_script/returns/'+i+'.json', 'w') as json_file:
		json.dump(operators, json_file)

