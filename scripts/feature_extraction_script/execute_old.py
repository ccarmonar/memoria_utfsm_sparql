import re, time, os, json
from functions.main import GetFinalResults,GroupOperators,GetOperatorExecutionFeatures,IdentifyOperatorType,IdentifyPrecode,IdentifyAfterCode,GetGSPO,GetIRI_ID,GetAllPredicatesFromProfile,SetBooleanPredicates
from functions.aux import GetSubstring,ParseNestedBracket,CleanOperators,GetPrefixes,VectorString,MainCurlyBrackets

#current working directory
cwd = os.getcwd()

#z es el indice de el archivo de la lista n que se quiere abrir
z = 0
n = [
	'ex008',#0
	'test_wikidata1',  # 1
	'test_wikidata2',  # 2
	'test_wikidata3',  # 3
	'test_wikidata4',  # 4
	'test_wikidata5',  # 5
	'test_wikidata6',  # 6
	'test_wikidata7',  # 7
	'test_wikidata8',  # 8
	'test_wikidata9',  # 9
]

n1 = [
	'ex003',#0
	'ex006',#1
	'ex010',#2
	'ex015',#3
	'ex023',#4
	'ex057',#5
	'ex061',#6
	'ex088',#7
	'ex098',#8
	'ex101',#9
	'ex103',#10
	'ex105',#11
	'ex137',#12
	'ex143',#13
	'ex269',#14	 -- ERROR
	'ex332',#15
	'ex459',#16
	'q1',#17
	'q2',#18
	'q3',#19
	'q4',#20
	'q5',#21
	'q6',#22
	'q7',#23 --ERROR
	'q8',#24
	'q9',#25
	'q10',#26
	'q11',#27
	'test1',#28
	'test2',#29
	'test3',#30
	'test4',#31
	'test5',#32
	'test_wikidata1',#33
	'test_wikidata2',#34
	'test_wikidata3',#35
	'test_wikidata4',#36
	'test_wikidata5',#37
	]


#PATHS
output_path = "../outputs/outputs_" + n[z]

path_sparql_file = output_path + "/" + n[z] + ".rq"

path_profile_sparql = output_path + "/profile_normal_file_" + n[z]
path_profile_explain_bajo_sparql = output_path + "/profile_normal_explain_bajo_" + n[z]
path_profile_loop_sparql = output_path + "/profile_loop_file_" + n[z]
path_profile_order_loop_sparql = output_path + "/profile_order_loop_file_" + n[z]

path_explain_sparql = output_path + "/explain_normal_file_" + n[z]
path_explain_order_loop = output_path + "/explain_order_loop_file_" + n[z]

path_translate_sparql = output_path + "/sparql_translate_file_" + n[z]

#FILES
sparql_file = open(path_sparql_file, 'r', encoding='latin-1').read()
profile_sparql = open(path_profile_sparql, 'r', encoding='latin-1').read()
profile_explain_bajo_sparql = open(path_profile_explain_bajo_sparql, 'r', encoding='latin-1').read()
translate_sparql = open(path_translate_sparql,'r', encoding='latin-1').read()


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

operators, predicates_list = execute(profile_sparql)
print("+++++++++++++")
print(sparql_file)
print("+++++++++++++")
print("+++++++++++++")
print(predicates_list)
print("+++++++++++++")
test_print()

with open('operators.json', 'w') as json_file:
	json.dump(operators, json_file)










