import re, time, os
from functions.aux import GetSubstring,ParseNestedBracket,CleanOperators,GetPrefixes,VectorString


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
		
	return operators



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
				if short_l_filt[nch] == 'Fanout:' and nch == 3:
					return_dicto['cardinality_fanout'] = short_l_filt[nch+1]
	return return_dicto	




#Función que identifica si el operador es SCAN, SUBQUERY, u otro.
def IdentifyOperatorType(operator):
	if "P =  " and "Key RDF_QUAD" and "from DB.DBA.RDF_QUAD by RDF_QUAD" in operator['profile_text']:
		operator['operator_type'] = 'scan'
	else:
		operator['operator_type'] = 'no_scan'
	return operator

#Funcion que guarda el precode en una llave, además, marca con un booleano para identificar si existe precode aqui o no.
def IdentifyPrecode(operator):
	if "Precode:" in operator['profile_text']:
		operator['precode_bool'] = 1
		operator['precode_text'] = GetSubstring(operator['profile_text'],'Precode:','Return 0',True)
	else:
		operator['precode_bool'] = 0
	return operator
	
#Funcion que guarda el after code en una llave, además, marca con un booleano para identificar si existe after code aqui o no.
def IdentifyAfterCode(operator):
	if "After code:" in operator['profile_text']:
		operator['after_code_bool'] = 1
		operator['after_code_text'] = GetSubstring(operator['profile_text'],'After code:','Return 0',True)
	else:
		operator['after_code_bool'] = 0
	return operator
	

def GetGSPO(operator):
	lines = operator['profile_text'].split('\n')
	for ls in lines:
		#print(ls)
		if " P " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0,len(split_P)):
				if split_P[s] == 'P':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['P'] = VectorString(split_P[s+2:])
					else:
						operator['P'] = split_P[s+2]
					#print(operator['P'])
		if " O " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0,len(split_P)):
				if split_P[s] == 'O':
					#print(split_P[s+2])
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['O'] = VectorString(split_P[s+2:])
					else:
						operator['O'] = split_P[s+2]
					#print(operator['O'])

		if " S " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0,len(split_P)):
				if split_P[s] == 'S':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['S'] = VectorString(split_P[s+2:])
					else:
						operator['S'] = split_P[s+2]
					
					#print(operator['S'])

		if " G " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0,len(split_P)):
				if split_P[s] == 'G':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['G'] = VectorString(split_P[s+2:])
					else:
						operator['G'] = split_P[s+2]
					#print(operator['G'])

		
		
	#print("---------")
	return operator
			
def GetIRI_ID(sparql_query,triple_component):
	if triple_component == 'P':
		main_selection = ParseNestedBracket(sparql_query,0)
		prefixes = GetPrefixes(sparql_query)
	
	print(sparql_query)
	print("++++++++++++")
	print(prefixes)
	print("++++++++++++")
	return main_selection

