import re, time, os
from functions.aux import GetSubstring,ParseNestedBracket,CleanOperators,GetPrefixes,VectorString


#Funcion que guarda los resultados finales:
def GetFinalResults(profile_sparql,operators):
	extract_final_results = ParseNestedBracket(profile_sparql, 0).strip().split('\n')
	final_results_aux = (extract_final_results[-2] + ' ' + extract_final_results[-1]).split(' ')
	final_results = list(filter(None, final_results_aux))
	for i in range(0,len(final_results)):
		if final_results[i] == 'msec':
			operators['ql_rt_msec'] = final_results[i-1]
		if final_results[i] == 'cpu,':
			operators['ql_rt_clocks'] = final_results[i-1]
		if final_results[i] == 'rnd':
			operators['ql_c_rnd_rows'] = final_results[i-1]

	return operators


#Funcion que agrupa cada operador en un diccionario de diccionario. Esto último se hace porque servira mas adelante
#INPUT: profile_sparql
def GroupOperators(profile_sparql):
	extract_sparql_profile = ParseNestedBracket(profile_sparql, 1)
	extract_sparql_profile = extract_sparql_profile.split("\n")
	c = 0
	operators = {}
	for i in range(0,len(extract_sparql_profile)):
		x = extract_sparql_profile[i]
		if x != "" and all(element in x for element in ["time", "fanout", "input", "rows"]):
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
		if set(["time","fanout","input","rows"]).issubset(l_filt):
			for nch in range(0,len(l_filt)):
				if l_filt[nch] == 'time' and nch == 0:
					return_dicto['time'] = float(l_filt[nch+1][:-1])
				if l_filt[nch] == 'fanout' and nch == 2:
					return_dicto['fanout'] = float(l_filt[nch+1])
				if l_filt[nch] == 'input' and nch == 4 and l_filt[nch+2] == 'rows':
					return_dicto['input_rows'] = float(l_filt[nch+1])
		if set(["Cardinality","estimate"]).issubset(l_filt):
			short_l_filt=l_filt[14:]
			for nch in range(0,len(short_l_filt)):
				if short_l_filt[nch] == 'Cardinality' and nch == 0 and short_l_filt[nch+1] == "estimate:":
					return_dicto['cardinality_estimate'] = float(short_l_filt[nch+2])
				if short_l_filt[nch] == 'Fanout:' and nch == 3:
					return_dicto['cardinality_fanout'] = float(short_l_filt[nch+1])
	return return_dicto


#Función que identifica si el operador es SCAN, SUBQUERY, u otro.
def IdentifyOperatorType(operator):
	if all(element in operator['profile_text'] for element in ["P =  ", "fanout", "Key RDF_QUAD", "from DB.DBA.RDF_QUAD by RDF_QUAD"]):
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
		if " P " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0, len(split_P)):
				if split_P[s] == 'P':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['P'] = VectorString(split_P[s+2:])
					else:
						operator['P'] = split_P[s+2]
					#print(operator['P'])
		if " O " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0, len(split_P)):
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


#Función que retorna todos los predicados leidos en un query profile
def GetAllPredicatesFromProfile(operators):
	set_predicates = set(())
	for k in operators.keys():
		if 'P' in operators[k].keys():
			set_predicates.add(operators[k]['P'])
	return list(set_predicates)


#Setea un booleano como una llave segun los predicados existentes en la consulta
def SetBooleanPredicates(operators, predicates_list):
	for k in operators.keys():
		if 'P' in operators[k].keys():
			for p in predicates_list:
				if operators[k]['P'] == p:
					operators[k][p] = 1
				else:
					operators[k][p] = 0
	return operators


# EN CONSTRUCCION TAL VEZ SE HAGA
def GetIRI_ID(sparql_query, triple_component):
	if triple_component == 'P':
		main_selection = ParseNestedBracket(sparql_query,0)
		prefixes = GetPrefixes(sparql_query)
	return main_selection

