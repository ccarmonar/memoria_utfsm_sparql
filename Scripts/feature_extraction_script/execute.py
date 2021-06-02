import re, time, os
from functions.main import GetFinalResults,GroupOperators,GetOperatorExecutionFeatures,IdentifyOperatorType,IdentifyPrecode,IdentifyAfterCode,GetGSPO,GetIRI_ID,GetAllPredicatesFromProfile,SetBooleanPredicates
from functions.aux import GetSubstring,ParseNestedBracket,CleanOperators,GetPrefixes,VectorString

#current working directory
cwd = os.getcwd()

#z es el indice de el archivo de la lista n que se quiere abrir
z = 10
n = [
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
sparql_file = open(path_sparql_file, 'r').read()
profile_sparql = open(path_profile_sparql, 'r').read()
profile_explain_bajo_sparql = open(path_profile_explain_bajo_sparql, 'r').read()
translate_sparql = open(path_translate_sparql,'r').read()


def execute(profile_sparql):
	operators = GroupOperators(profile_sparql)
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
	#print(operators)
	for k,v in operators.items():
		if 'P' in operators[k].keys():
			print(k,v)
		#print("   ")
		#try:
		#	print(k)
		#	print("+++++")
		#	print(operators[k]['profile_text'])
		#	print(" ")
		#	print("S : "+ operators[k]['S'])
		#except KeyError:
		#	print(" ")
		#	print(k)
		#	print("xd")
	#print("|+++++++++++++++++++++++++++++++++++++++++++++++++|")


operators, predicates_list = execute(profile_sparql)
print("+++++++++++++")
print(sparql_file)
print("+++++++++++++")
print("+++++++++++++")
print(predicates_list)
print("+++++++++++++")
test_print()

#print(x)










