import re, time
z =37
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
	'ex269',#14	
	'ex332',#15	
	'ex459',#16	
	'q1',#17
	'q2',#18	
	'q3',#19	
	'q4',#20	
	'q5',#21	
	'q6',#22	
	'q7',#23 ERROR	
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


output_path = "outputs/outputs_" + n[z]
#print(output_path)	

path_sparql_file = output_path + "/" + n[z] + ".rq"

path_profile_sparql = output_path + "/profile_normal_file_" + n[z]
path_profile_explain_bajo_sparql = output_path + "/profile_normal_explain_bajo_" + n[z]
path_profile_loop_sparql = output_path +	"/profile_loop_file_" + n[z]
path_profile_order_loop_sparql = output_path + "/profile_order_loop_file_" + n[z]

path_explain_sparql = output_path + "/explain_normal_file_" + n[z]
path_explain_order_loop = output_path + "/explain_order_loop_file_" + n[z]

path_translate_sparql = output_path + "/sparql_translate_file_" + n[z]

#print(path_sparql_file)

#print(path_profile_sparql)
#print(path_profile_explain_bajo_sparql)
#print(path_profile_loop_sparql)
#print(path_profile_order_loop_sparql)

#print(path_explain_sparql)
#print(path_explain_order_loop)

#print(path_translate_sparql)
#print("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
sparql_file = open(path_sparql_file, 'r').read()
profile_sparql = open(path_profile_sparql, 'r').read()
profile_explain_bajo_sparql = open(path_profile_explain_bajo_sparql, 'r').read()
translate_sparql = open(path_translate_sparql,'r').read()
print(sparql_file)
print("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
print(profile_sparql)
print("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
print(profile_explain_bajo_sparql)
print("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")

def ParseNestedBracket(string, level):
    """
    Return string contained in nested {}, indexing i = level
    """
    CountLeft = len(re.findall("\{", string))
    CountRight = len(re.findall("\}", string))
    if CountLeft == CountRight:
        LeftRightIndex = [x for x in zip(
        [Left.start()+1 for Left in re.finditer('\{', string)], 
        reversed([Right.start() for Right in re.finditer('\}', string)]))]

    elif CountLeft > CountRight:
        return ParseNestedParen(string + '}', level)

    elif CountLeft < CountRight:
        return ParseNestedParen('{' + string, level)

    return string[LeftRightIndex[level][0]:LeftRightIndex[level][1]]

#Funcion que agrupa cada operador en un diccionario de diccionario. Esto último se hace porque servira mas adelante
#INPUT: profile_sparql
def GroupOperators(profile_sparql):
	extract_sparql_profile = ParseNestedBracket(profile_sparql, 1)
	extract_sparql_profile = extract_sparql_profile.split("\n")
	c = 0
	operators = {}
	for i in range(0,len(extract_sparql_profile)):
		x = extract_sparql_profile[i].strip()
		if x != "" and (x.split(" ")[0] == "time"):
			c = c + 1	
		OP = "OP"+str(c)
		if OP not in operators:
			operators[OP]={'profile_text' : x}
		else:
			operators[OP]['profile_text']=operators[OP]['profile_text']+'\n'+x
		
		#print(x)
		#print(OP)
	return operators
	
operators = GroupOperators(profile_sparql)

#print("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
#for k,v in operator.items():
#	print(k)
#	print(v)
print("||||||||+++++|||||||")
print(operators["OP0"])
print(operators["OP2"])
print(operators["OP1"])
print("||||||||+++++|||||||")
print("||||||||+++++|||||||")
print(operators["OP0"]['profile_text'])
print(operators["OP2"]['profile_text'])
print(operators["OP1"]['profile_text'])
print("||||||||+++++|||||||")

def IdentifyOperatorType(operator):
	return 0

#Esta función obtiene el time, fanout, input rows y estimacion de cardinalidad (si lo dispone Virtuoso, si compete y/o simplemente de existir) de un operador
#Crea un nuevo diccionario con estos valores
def GetOperatorExecutionFeatures(operator):
	lines_split_text = operator['profile_text'].split("\n")
	return_dicto = operator
	for l in lines_split_text:
		l_filt = list(filter(None,l.strip().split(" ")))
		if 'time' and 'fanout' and 'input' in l_filt:
			for nch in range(0,len(l_filt)):
				if l_filt[nch] == 'time' and nch == 0:
					return_dicto['time'] = float(l_filt[nch+1][:-1])
				if l_filt[nch] == 'fanout' and nch == 2:
					return_dicto['fanout'] = float(l_filt[nch+1])
				if l_filt[nch] == 'input' and nch == 4 and l_filt[nch+2] == 'rows':
					return_dicto['input_rows'] = float(l_filt[nch+1])
		if "Cardinality" and "estimate" in l_filt:
			short_l_filt=l_filt[14:]
			for nch in range(0,len(short_l_filt)):
				if short_l_filt[nch] == 'Cardinality' and nch == 0 and short_l_filt[nch+1] == "estimate:":
					return_dicto['cardinality_estimate'] = short_l_filt[nch+2]
				if short_l_filt[nch] == 'Fanout' and nch == 3:
					return_dicto['cardinality_fanout'] = short_l_filt[nch+1]
	return return_dicto	

print(GetOperatorExecutionFeatures(operators["OP2"]))
print("||||||||+++++|||||||")
print(GetOperatorExecutionFeatures(operators["OP0"]))
#Obtiene un diccionario con los PREFIX de la consulta Sparql
def GetPrefixes(sparql_file):
	dicto = {}
	lines_split_text = sparql_file.split("\n")
	for l in lines_split_text:
		if "PREFIX " in l:
			ls = l.split(" ")
			if ls[0] == "PREFIX":
				if ls[1][:-1] == '':
					dicto[':'] = ls[2]
				else:
					dicto[ls[1][:-1]] = ls[2]
	return dicto

#Obtiene las traducciones de SPARQL->SQL de Virtuoso para IRI/URIs, Literales y  Variables
def GetTranslates(sparql_file, translate_sparql):
	lines_split_translate_text = translate_sparql.split("\n")
	lines_split_sparql_file_text = sparql_file.split("\n")
	
	for lt in lines_split_translate_text:
		print(lt)
	
	
	return 0
	
#print(GetPrefixes(sparql_file))
#print(GetTranslates(sparql_file,translate_sparql))

#print(GetTranslates(sparql_file,translate_sparql))

