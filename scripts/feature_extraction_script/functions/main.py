import re
from functions.aux import GetSubstring, ParseNestedBracket, CleanOperators, GetPrefixes, VectorString, CleanSalts, SubstractStrings, OnlyScans


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
def GroupOperators(profile_sparql, profile_low_explain):
	extract_sparql_profile = profile_sparql
	extract_sparql_profile = extract_sparql_profile.split("\n")
	c = 0
	operators = {}
	for i in range(0,len(extract_sparql_profile)):
		x = extract_sparql_profile[i]
		if x != "" and all(element in x for element in ["time", "fanout", "input", "rows"]):
			c = c + 1
		OP = "OP"+str(c)
		if OP not in operators:
			operators[OP] = {'profile_text': x,'profile_text_low_explain': ''}
		else:
			operators[OP]['profile_text'] = operators[OP]['profile_text'] + '\n' + x

	extract_low_explain = profile_low_explain
	extract_low_explain = extract_low_explain.split("\n")
	c = 0
	for i in range(0, len(extract_low_explain)):
		x = extract_low_explain[i]
		if x != "" and all(element in x for element in ["time", "fanout", "input", "rows"]):
			c = c + 1
		OP = "OP" + str(c)
		operators[OP]['profile_text_low_explain'] = operators[OP]['profile_text_low_explain'] + '\n' + x
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
		if set(["Cardinality", "estimate"]).issubset(l_filt):
			short_l_filt=l_filt[14:]
			for nch in range(0,len(short_l_filt)):
				if short_l_filt[nch] == 'Cardinality' and nch == 0 and short_l_filt[nch+1] == "estimate:":
					return_dicto['cardinality_estimate'] = float(short_l_filt[nch+2])
				if short_l_filt[nch] == 'Fanout:' and nch == 3:
					return_dicto['cardinality_fanout'] = float(short_l_filt[nch+1])
		if "Cardinality estimate:" not in operator['profile_text']:
			return_dicto['cardinality_estimate'] = 0
			return_dicto['cardinality_fanout'] = 0

	return return_dicto


#Función que identifica si el operador es SCAN
def IdentifyOperatorType(operator):
	if all(e in operator['profile_text'] for e in ["DB.DBA.RDF","Key"]) \
			and all(e not in operator['profile_text'] for e in ["RDF_QUAD_SP","RDF_QUAD_OP"]):
		operator['operator_type'] = 1
	else:
		operator['operator_type'] = 0
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
		operator['after_code_text'] = GetSubstring(operator['profile_text'], 'After code:', 'Return 0',True)
	else:
		operator['after_code_bool'] = 0
	return operator


#Funcion que guarda el after code en una llave, además, marca con un booleano para identificar si existe after code aqui o no.
def IdentifyAfterTest(operator):
	if "After test:" in operator['profile_text']:
		operator['after_test_bool'] = 1
		operator['after_test_text'] = GetSubstring(operator['profile_text'], 'After test:', 'Return 0',True)
	else:
		operator['after_test_bool'] = 0
	return operator


def IdentifyGroupBy(operator):
	if "group by read" in operator['profile_text']:
		operator['group_by_read_bool'] = 1
	else:
		operator['group_by_read_bool'] = 0
	return operator


def IdentifyDistinct(operator):
	if any(e in operator['profile_text'] for e in ["Distinct ", "Distinct (HASH)", "distinct"]):
		operator['distinct_bool'] = 1
	else:
		operator['distinct_bool'] = 0
	return operator


def IdentifyTopOrderByRead(operator):
	if 'top order by read' in operator['profile_text'] or 'top order by node' in operator['profile_text']:
		operator['top_order_by_bool'] = 1
	else:
		operator['top_order_by_bool'] = 0
	return operator


def IdentifySelect(operator):
	if 'Select' in operator['profile_text'] and 'Subquery' not in operator['profile_text']:
		operator['select?'] = 1
	else:
		operator['select?'] = 0
	return operator


def IdentifyTOP(operator):
	if "(TOP" in operator['profile_text']:
		operator['TOP_bool'] = 1
		lines = operator['profile_text'].split('\n')
		for ls in lines:
			if '(TOP' in ls:
				splitting = list(filter(None, ls.strip().split(' ')))
				for sp in range(0, len(splitting)):
					if '(TOP' in splitting[sp]:
						operator['TOP_num'] = int(splitting[sp+1])

	else:
		operator['TOP_bool'] = 0
		operator['TOP_num'] = 0
	return operator


def IdentifySkipNode(operator):
	if " skip node " in operator['profile_text']:
		operator['skip_node_bool'] = 1
		lines = operator['profile_text'].split('\n')
		for ls in lines:
			if ' skip node ' in ls:
				splitting = list(filter(None, ls.strip().split(' ')))
				for sp in range(0, len(splitting)):
					if 'skip' in splitting[sp] and 'node' in splitting[sp+1]:
						operator['skip_node_num'] = int(splitting[sp+2])

	else:
		operator['skip_node_bool'] = 0
		operator['skip_node_num'] = 0
	return operator


def GetGSPO_normal_profile(operator):
	if operator['operator_type'] == 1:
		lines = operator['profile_text'].split('\n')
		for ls in lines:
			#PREDICADOS no rowspecs
			if all(e in ls for e in [" P "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'P':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['P'] = VectorString(split_P[s+2:])
							operator['P_math_op'] = split_P[s + 1]
						elif "$" in split_P[s+2]:
							operator['P'] = CleanSalts(split_P[s+2], False)
							operator['P_math_op'] = split_P[s + 1]
						else:
							operator['P'] = split_P[s+2]
							operator['P_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" O "]) and any(e in ls for e in [" = ", " > ", " < "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'O':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['O'] = VectorString(split_P[s+2:])
							operator['O_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['O'] = CleanSalts(split_P[s + 2], False)
							operator['O_math_op'] = split_P[s + 1]
						else:
							operator['O'] = split_P[s+2]
							operator['O_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" S "]) and any(e in ls for e in [" = ", " > ", " < "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'S':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['S'] = VectorString(split_P[s+2:])
							operator['S_math_op'] = split_P[s+1]
						elif "$" in split_P[s + 2]:
							operator['S'] = CleanSalts(split_P[s + 2], False)
							operator['S_math_op'] = split_P[s + 1]
						else:
							operator['S'] = split_P[s+2]
							operator['S_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" G "]) and any(e in ls for e in [" = ", " > ", " < "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0,len(split_P)):
					if split_P[s] == 'G':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['G'] = VectorString(split_P[s+2:])
							operator['G_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['G'] = CleanSalts(split_P[s + 2], False)
							operator['G_math_op'] = split_P[s + 1]
						else:
							operator['G'] = split_P[s+2]
							operator['G_math_op'] = split_P[s + 1]
			#------------------------ROW SPECS---------------------------------#
			if all(e in ls for e in [" P ", "row specs"]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'P':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['P_rs'] = VectorString(split_P[s+2:])
							operator['P_rs_math_op'] = split_P[s + 1]
						elif "$" in split_P[s+2]:
							operator['P_rs'] = CleanSalts(split_P[s+2], False)
							operator['P_rs_math_op'] = split_P[s + 1]
						else:
							operator['P_rs'] = split_P[s+2]
							operator['P_rs_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" O ", "row specs"]) and any(e in ls for e in [" = ", " > ", " < "]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'O':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['O_rs'] = VectorString(split_P[s+2:])
							operator['O_rs_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['O_rs'] = CleanSalts(split_P[s + 2], False)
							operator['O_rs_math_op'] = split_P[s + 1]
						else:
							operator['O_rs'] = split_P[s+2]
							operator['O_rs_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" S ", "row specs"]) and any(e in ls for e in [" = ", " > ", " < "]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'S':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['S_rs'] = VectorString(split_P[s+2:])
							operator['S_rs_math_op'] = split_P[s+1]
						elif "$" in split_P[s + 2]:
							operator['S_rs'] = CleanSalts(split_P[s + 2], False)
							operator['S_rs_math_op'] = split_P[s + 1]
						else:
							operator['S_rs'] = split_P[s+2]
							operator['S_rs_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" G ", "row specs"]) and any(e in ls for e in [" = ", " > ", " < "]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0,len(split_P)):
					if split_P[s] == 'G':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['G_rs'] = VectorString(split_P[s+2:])
							operator['G_rs_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['G_rs'] = CleanSalts(split_P[s + 2], False)
							operator['G_rs_math_op'] = split_P[s + 1]
						else:
							operator['G_rs'] = split_P[s+2]
							operator['G_rs_math_op'] = split_P[s + 1]
	else:
		operator['G'] = 'None'
		operator['G_math_op'] = 'None'
		operator['G_rs'] = 'None'
		operator['G_rs_math_op'] = 'None'
		operator['S'] = 'None'
		operator['S_math_op'] = 'None'
		operator['S_rs'] = 'None'
		operator['S_rs_math_op'] = 'None'
		operator['P'] = 'None'
		operator['P_math_op'] = 'None'
		operator['P_rs'] = 'None'
		operator['P_rs_math_op'] = 'None'
		operator['O'] = 'None'
		operator['O_math_op'] = 'None'
		operator['O_rs'] = 'None'
		operator['O_rs_math_op'] = 'None'
	return operator


def GetGSPO(operator):
	if operator['operator_type'] == 1:
		lines = operator['profile_text_low_explain'].split('\n')
		for ls in lines:
			#PREDICADOS no rowspecs
			if all(e in ls for e in [" P "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'P':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['P'] = VectorString(split_P[s+2:])
							operator['P_math_op'] = split_P[s + 1]
						elif "$" in split_P[s+2]:
							operator['P'] = CleanSalts(split_P[s+2], False)
							operator['P_math_op'] = split_P[s + 1]
						else:
							operator['P'] = split_P[s+2]
							operator['P_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" O "]) and any(e in ls for e in [" = ", " > ", " < "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'O':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['O'] = VectorString(split_P[s+2:])
							operator['O_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['O'] = CleanSalts(split_P[s + 2], False)
							operator['O_math_op'] = split_P[s + 1]
						else:
							operator['O'] = split_P[s+2]
							operator['O_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" S "]) and any(e in ls for e in [" = ", " > ", " < "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'S':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['S'] = VectorString(split_P[s+2:])
							operator['S_math_op'] = split_P[s+1]
						elif "$" in split_P[s + 2]:
							operator['S'] = CleanSalts(split_P[s + 2], False)
							operator['S_math_op'] = split_P[s + 1]
						else:
							operator['S'] = split_P[s+2]
							operator['S_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" G "]) and any(e in ls for e in [" = ", " > ", " < "]) and 'row specs' not in ls:
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0,len(split_P)):
					if split_P[s] == 'G':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['G'] = VectorString(split_P[s+2:])
							operator['G_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['G'] = CleanSalts(split_P[s + 2], False)
							operator['G_math_op'] = split_P[s + 1]
						else:
							operator['G'] = split_P[s+2]
							operator['G_math_op'] = split_P[s + 1]
			#------------------------ROW SPECS---------------------------------#
			if all(e in ls for e in [" P ", "row specs"]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'P':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['P_rs'] = VectorString(split_P[s+2:])
							operator['P_rs_math_op'] = split_P[s + 1]
						elif "$" in split_P[s+2]:
							operator['P_rs'] = CleanSalts(split_P[s+2], False)
							operator['P_rs_math_op'] = split_P[s + 1]
						else:
							operator['P_rs'] = split_P[s+2]
							operator['P_rs_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" O ", "row specs"]) and any(e in ls for e in [" = ", " > ", " < "]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'O':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['O_rs'] = VectorString(split_P[s+2:])
							operator['O_rs_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['O_rs'] = CleanSalts(split_P[s + 2], False)
							operator['O_rs_math_op'] = split_P[s + 1]
						else:
							operator['O_rs'] = split_P[s+2]
							operator['O_rs_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" S ", "row specs"]) and any(e in ls for e in [" = ", " > ", " < "]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0, len(split_P)):
					if split_P[s] == 'S':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['S_rs'] = VectorString(split_P[s+2:])
							operator['S_rs_math_op'] = split_P[s+1]
						elif "$" in split_P[s + 2]:
							operator['S_rs'] = CleanSalts(split_P[s + 2], False)
							operator['S_rs_math_op'] = split_P[s + 1]
						else:
							operator['S_rs'] = split_P[s+2]
							operator['S_rs_math_op'] = split_P[s + 1]
			if all(e in ls for e in [" G ", "row specs"]) and any(e in ls for e in [" = ", " > ", " < "]):
				split_P = list(filter(None,ls.strip().split(' ')))
				for s in range(0,len(split_P)):
					if split_P[s] == 'G':
						if split_P[s+2][:2].lower() == "<v" \
								or split_P[s+2][:2].lower() == "<r" \
								or split_P[s+2][:3].lower() == "<$r" \
								or split_P[s+2][:3].lower() == "<$v" \
								or split_P[s+2][:3].lower() == "<$c" \
								or split_P[s+2][:3].lower() == "<c" \
								or split_P[s + 2][:3].lower() == "<tag":
							operator['G_rs'] = VectorString(split_P[s+2:])
							operator['G_rs_math_op'] = split_P[s + 1]
						elif "$" in split_P[s + 2]:
							operator['G_rs'] = CleanSalts(split_P[s + 2], False)
							operator['G_rs_math_op'] = split_P[s + 1]
						else:
							operator['G_rs'] = split_P[s+2]
							operator['G_rs_math_op'] = split_P[s + 1]
	else:
		operator['G'] = 'None'
		operator['G_math_op'] = 'None'
		operator['G_rs'] = 'None'
		operator['G_rs_math_op'] = 'None'
		operator['S'] = 'None'
		operator['S_math_op'] = 'None'
		operator['S_rs'] = 'None'
		operator['S_rs_math_op'] = 'None'
		operator['P'] = 'None'
		operator['P_math_op'] = 'None'
		operator['P_rs'] = 'None'
		operator['P_rs_math_op'] = 'None'
		operator['O'] = 'None'
		operator['O_math_op'] = 'None'
		operator['O_rs'] = 'None'
		operator['O_rs_math_op'] = 'None'
	return operator



def SetGSPODefault(operator):
	if 'G' not in list(operator.keys()):
		operator['G'] = 'None'
		operator['G_math_op'] = 'None'
	if 'G_rs' not in list(operator.keys()):
		operator['G_rs'] = 'None'
		operator['G_rs_math_op'] = 'None'
	if 'S' not in list(operator.keys()):
		operator['S'] = 'None'
		operator['S_math_op'] = 'None'
	if 'S_rs' not in list(operator.keys()):
		operator['S_rs'] = 'None'
		operator['S_rs_math_op'] = 'None'
	if 'P' not in list(operator.keys()):
		operator['P'] = 'None'
		operator['P_math_op'] = 'None'
	if 'P_rs' not in list(operator.keys()):
		operator['P_rs'] = 'None'
		operator['P_rs_math_op'] = 'None'
	if 'O' not in list(operator.keys()):
		operator['O'] = 'None'
		operator['O_math_op'] = 'None'
	if 'O_rs' not in list(operator.keys()):
		operator['O_rs'] = 'None'
		operator['O_rs_math_op'] = 'None'

	return operator


#Función que retorna todos los predicados leidos en un query profile
def GetAllPredicatesFromProfile(operators):
	set_predicates = set(())
	for k in operators.keys():
		if operators[k]['P'] != 'None':
			set_predicates.add(operators[k]['P'])
	return list(set_predicates)


def SetBooleanPredicates(operators, predicates_list):
	for k in operators.keys():
		if 'P' in operators[k].keys():
			for p in predicates_list:
				if operators[k]['P'] == p:
					operators[k][p] = 1
				else:
					operators[k][p] = 0
		else:
			for p in predicates_list:
				operators[k][p] = 0

	return operators


def GetStartAndEndOptionalSection(operator, sparql_file, still_not_optional_possibility=0):
	if 'OPTIONAL' not in sparql_file:
		operator['start_optional'] = 0
		operator['end_optional'] = 0
		return operator, 0
	if any(element in operator['profile_text_low_explain'] for element in ["cluster outer seq start"]) and still_not_optional_possibility == 0:
		still_not_optional_possibility = 1
		operator['start_optional'] = 0
		operator['end_optional'] = 0
		return operator, still_not_optional_possibility

	if still_not_optional_possibility == 0:
		operator['start_optional'] = 0
		operator['end_optional'] = 0
		return operator, still_not_optional_possibility
	else:
		if any(element in operator['profile_text_low_explain'] for element in ["cluster outer seq start", "outer {"]):
			operator['start_optional'] = 1
			operator['end_optional'] = 0
		elif any(element in operator['profile_text_low_explain'] for element in [" end of outer seq", "end of outer seq", "} /* end of outer */"]):
			operator['start_optional'] = 0
			operator['end_optional'] = 1
		else:
			operator['start_optional'] = 0
			operator['end_optional'] = 0
		return operator, still_not_optional_possibility


def SetBooleanOptionalSection(operators):
	optional_boolean = 0
	num_opt = 0
	static_keys = list(operators.keys())
	for k in static_keys:
		if operators[k]['start_optional'] == 1:
			optional_boolean = 1
			operators[k]['optional_section?'] = optional_boolean
		if operators[k]['end_optional'] == 1:
			operators[k]['optional_section?'] = optional_boolean
			optional_boolean = 0
		else:
			operators[k]['optional_section?'] = optional_boolean

	for k in static_keys:
		if operators[k]['optional_section?'] == 0:
			operators[k]['num_opt'] = 0
		else:
			if operators[k]['start_optional'] == 1:
				num_opt += 1
			operators[k]['num_opt'] = num_opt

	return operators



# EN CONSTRUCCION TAL VEZ SE HAGA
def GetIRI_ID(sparql_query, triple_component):
	if triple_component == 'P':
		main_selection = ParseNestedBracket(sparql_query,0)
		prefixes = GetPrefixes(sparql_query)
	return main_selection



def SetSorts(operators):
	sort_lvl = 0
	union_sort_lvl = 0
	for k in operators.keys():
		if any(element in operators[k]['profile_text'] for element in ["fork {", " Fork "]):
			sort_lvl = sort_lvl + 1
			operators[k]['sort_lvl'] = sort_lvl
			operators[k]['union_sort_lvl'] = union_sort_lvl
			# detectar union UNION
		if any(element in operators[k]['profile_text'] for element in ["union", "Union"]) and sort_lvl > 0:
			union_sort_lvl = union_sort_lvl + 1
			operators[k]['union_sort_lvl'] = union_sort_lvl
			operators[k]['sort_lvl'] = sort_lvl
		if any(element in operators[k]['profile_text'] for element in [" Sort "," Sort (HASH) ", "Sort "]) and operators[k]['}'] >= 2 and operators[k]['{'] == 0:
			operators[k]['union_sort_lvl'] = union_sort_lvl
			operators[k]['sort_lvl'] = sort_lvl
			sort_lvl = sort_lvl - 1
			union_sort_lvl = union_sort_lvl - 1
		elif any(element in operators[k]['profile_text'] for element in [" Sort "," Sort (HASH) ", "Sort "]) and operators[k]['{'] == 0:
			operators[k]['sort_lvl'] = sort_lvl
			operators[k]['union_sort_lvl'] = union_sort_lvl
			sort_lvl = sort_lvl - 1
		else:
			operators[k]['union_sort_lvl'] = union_sort_lvl
			operators[k]['sort_lvl'] = sort_lvl
	return operators


def SetTargetAndTransitive(operators):
	target_bracket = 0
	transitive_bracket = 0
	for k in operators.keys():
		if any(element in operators[k]['profile_text'] for element in ["Target:"]) and operators[k]['{'] >= 1:
			target_bracket = target_bracket + 1
			operators[k]['target_bracket'] = target_bracket
		elif "Subquery Select" in operators[k]['profile_text'] and operators[k]['}'] == 1 and target_bracket > 0:
			operators[k]['target_bracket'] = target_bracket
			target_bracket = target_bracket - 1
		else:
			operators[k]['target_bracket'] = target_bracket

	for k in operators.keys():
		if any(element in operators[k]['profile_text'] for element in ["Transitive dt"]) and operators[k]['{'] >= 1:
			transitive_bracket = transitive_bracket + 1
			operators[k]['transitive_bracket'] = transitive_bracket
		elif "Subquery Select" in operators[k]['profile_text'] and operators[k]['}'] == 1 and transitive_bracket > 0:
			operators[k]['transitive_bracket'] = transitive_bracket
			transitive_bracket = transitive_bracket - 1
		else:
			operators[k]['transitive_bracket'] = transitive_bracket

	return operators


def SetSubqueries(operators):
	subquerie_lvl = 0
	union_sub_lvl = 0
	# SUBQUERY BRACKETS
	for k in operators.keys():
		operators[k]['subquerie_lvl'] = subquerie_lvl
		operators[k]['union_sub_lvl'] = union_sub_lvl
		if 'Subquery' in operators[k]['profile_text']:
			aux_split = operators[k]['profile_text'].strip().split('Subquery')[1::]
			for i in aux_split:
				if 'Select' not in i:
					subquerie_lvl = subquerie_lvl + 1
					operators[k]['subquerie_lvl'] = subquerie_lvl
					operators[k]['union_sub_lvl'] = union_sub_lvl
				else:
					if union_sub_lvl == 0:
						if all(e == 0 for e in [operators[k]['transitive_bracket'], operators[k]['target_bracket'],operators[k]['after_test_lvl']]) and operators[k]['}'] > 0:
							subquerie_lvl = subquerie_lvl - 1
					else:
						if all(e == 0 for e in [operators[k]['transitive_bracket'], operators[k]['target_bracket'], operators[k]['after_test_lvl']]) and operators[k]['{'] == 0 and operators[k]['}'] > 0:
							subquerie_lvl = subquerie_lvl - 1
		#UNION WITHOUT FORK
		if any(element in operators[k]['profile_text'] for element in ['union', 'Union']) and operators[k]['union_sort_lvl'] < 1:
			union_sub_lvl = union_sub_lvl + 1
			operators[k]['union_sub_lvl'] = union_sub_lvl

		# END OF SORT
		if any(e in operators[k]['profile_text'] for e in [" Sort "," Sort (HASH) ", "Sort "]) \
				and operators[k]['}'] >= 2 and operators[k]['{'] == 0 and operators[k]["sort_lvl"] > 0:
			subquerie_lvl = subquerie_lvl - operators[k]['}'] + operators[k]['union_sort_lvl'] + operators[k]["sort_lvl"]

		# RESTA UNION union_sub_lvl > 0 target_bracket=0 after_test_lvl=0 '}'>=2 '{'==0
		if "Subquery Select" in operators[k]['profile_text'] and operators[k]['target_bracket'] == 0 \
				and operators[k]['after_test_lvl'] == 0 and operators[k]['transitive_bracket'] == 0 \
				and operators[k]['}'] >= 2 and operators[k]['{'] == 0 and union_sub_lvl > 0:
			union_sub_lvl = union_sub_lvl - 1

	# SUBQUERY SELECT
	for k in operators.keys():
		if "Subquery Select" in operators[k]['profile_text']:
			operators[k]['subquery_select?'] = 1
		else:
			operators[k]['subquery_select?'] = 0

	return operators


def IdentifyEndNode(operator):
	if 'END Node' in operator['profile_text']:
		operator['END_NODE?'] = 1
	else:
		operator['END_NODE?'] = 0
	return operator


def SetAfterTest(operators):
	after_test_lvl = 0
	for k in operators.keys():
		if "After test:" in operators[k]['profile_text'] and "Return 0" in operators[k]['profile_text'] and operators[k]['precode_bool'] == 0 and operators[k]['after_code_bool'] == 0: #and operators[k]['precode_bool'] == 0 and operators[k]['after_code_bool'] == 0:
			operators[k]['after_test_lvl'] = 1
			operators[k]['after_test_1op?'] = 1

		elif "After test:" in operators[k]['profile_text'] and "Return 0" in operators[k]['profile_text'] and operators[k]['precode_bool'] == 1 and operators[k]['after_code_bool'] == 0:
			aux_text = SubstractStrings(operators[k]['profile_text'], operators[k]['precode_text'])
			if "Return 0" in aux_text:
				operators[k]['after_test_lvl'] = 1
				operators[k]['after_test_1op?'] = 1

		elif "After test:" in operators[k]['profile_text'] and "Return 0" in operators[k]['profile_text'] and operators[k]['precode_bool'] == 0 and operators[k]['after_code_bool'] == 1:
			aux_text = SubstractStrings(operators[k]['profile_text'], operators[k]['after_code_text'])
			if "Return 0" in aux_text:
				operators[k]['after_test_lvl'] = 1
				operators[k]['after_test_1op?'] = 1

		elif "After test:" in operators[k]['profile_text'] and "Return 0" in operators[k]['profile_text'] and operators[k]['precode_bool'] == 1 and operators[k]['after_code_bool'] == 1:
			aux_text = SubstractStrings(operators[k]['profile_text'], operators[k]['precode_text'])
			aux_text = SubstractStrings(aux_text, operators[k]['after_code_text'])
			if "Return 0" in aux_text:
				operators[k]['after_test_lvl'] = 1
				operators[k]['after_test_1op?'] = 1

		else:
			operators[k]['after_test_1op?'] = 0
			operators[k]['after_test_lvl'] = after_test_lvl
			if "After test:" in operators[k]['profile_text']:
				after_test_lvl = after_test_lvl + 1
				operators[k]['after_test_lvl'] = after_test_lvl

			if "Return 0" in operators[k]['profile_text'] and operators[k]['precode_bool'] == 0 and operators[k]['after_code_bool'] == 0:
				operators[k]['after_test_lvl'] = after_test_lvl
				after_test_lvl = after_test_lvl - 1

			elif operators[k]['precode_bool'] == 1 and after_test_lvl > 0:
				aux_text = SubstractStrings(operators[k]['profile_text'], operators[k]['precode_text'])
				if "Return 0" in aux_text:
					operators[k]['after_test_lvl'] = after_test_lvl
					after_test_lvl = after_test_lvl - 1

			elif operators[k]['after_code_bool'] == 1 and after_test_lvl > 0:
				aux_text = SubstractStrings(operators[k]['profile_text'], operators[k]['after_code_text'])
				if "Return 0" in aux_text:
					operators[k]['after_test_lvl'] = after_test_lvl
					after_test_lvl = after_test_lvl - 1
			else:
				operators[k]['after_test_lvl'] = after_test_lvl
	return operators

#FALTA CORREGIR ALGUNAS COSAS AQUI
def IdentifyBGPS(operators):
	only_scans, os_keys = OnlyScans(operators)
	list_keys = list(operators.keys())
	num_bgp = 1

	for k in operators:
		operators[k]["num_bgp"] = 'None'

	for k in range(len(only_scans)):
		if k != 0:
			if operators[os_keys[k]]["union_count"] > operators[os_keys[k-1]]["union_count"]:
				num_bgp += 1
			if operators[os_keys[k]]["num_opt"] != operators[os_keys[k-1]]["num_opt"]:
				num_bgp += 1

		operators[os_keys[k]]["num_bgp"] = num_bgp

	return operators


def IdentifyUnionFeatures(operators, sparql):
	if 'UNION' in sparql:
		#union_lvl = sparql.count('UNION')
		union_lvl = 0
		for k in operators.keys():
			if union_lvl == 0:
				union_lvl = max(operators[k]['profile_text_low_explain'].count('Union'), union_lvl)

		union_i = 0
		union_count = 0
		for k in operators.keys():
			operators[k]['union_separate'] = 0
			operators[k]['union_end'] = 0
			if operators[k]['{'] >= 1 and operators[k]['}'] == 1 and (operators[k]['subquery_select?'] == 1 or operators[k]['END_NODE?'] == 1) and union_i == 0:
				operators[k]['union_separate'] = 1
				union_count += 1
				union_i = 1
			if operators[k]['}'] >= 2 and (operators[k]['subquery_select?'] == 1 or operators[k]['END_NODE?'] == 1) and union_i == 1:
				operators[k]['union_end'] = 1
				union_count += 1
				union_i = 0
				union_lvl = union_lvl - 1
			operators[k]['union_count'] = union_count
			operators[k]['union_lvl'] = union_lvl
	else:
		for k in operators.keys():
			operators[k]['union_lvl'] = 0
			operators[k]['union_separate'] = 0
			operators[k]['union_end'] = 0
			operators[k]['union_count'] = 0
	return operators


## CORREGIR
def SetTripleType(operators, sparql_file, list_alleq):
	sparql_file_as_list = GetSparqlAsList(sparql_file)
	list_alleq_aux = list_alleq[:]
	for k in operators.keys():
		if operators[k]['operator_type'] == 1:
			s, p, o = 'None', 'None', 'None'
			# SUJETO
			if 'IRI' in operators[k]['S']:
				s = 'URI'
			## OUTPUT ESPECIALES
			elif all(e in operators[k]['S'] for e in ["k_"]) and all(e not in operators[k]['S'] for e in [".S",".O",".P","all_eq","cast"]):
				q = ''
				k_ = operators[k]['S'].split('_')[1].lower()
				for i in set(sparql_file_as_list):
					if k_ in i:
						q = i
				if 'http://' in q:
					s = 'URI'
				else:
					s = 'VAR'
			elif 'all_eq' in operators[k]['S']:
				if 'IRI' in list_alleq_aux[0]:
					s = 'URI'
					del list_alleq_aux[0]
				else:
					s = 'VAR'
					del list_alleq_aux[0]

			else:
				s = 'VAR'


			# PREDICADOS
			if 'IRI' in operators[k]['P']:
				p = 'URI'
			## OUTPUT ESPECIALES
			elif all(e in operators[k]['P'] for e in ["k_"]) and all(e not in operators[k]['P'] for e in [".S",".O",".P","all_eq","cast"]):
				q = ''
				k_ = operators[k]['P'].split('_')[1].lower()
				for i in set(sparql_file_as_list):
					if k_ in i:
						q = i
				if 'http://' in q:
					p = 'URI'
				else:
					p = 'VAR'
			else:
				p = 'VAR'


			#OBJETO
			if 'IRI' in operators[k]['O']:
				o = 'URI'
			elif any(e == operators[k]['O'] for e in set(sparql_file_as_list)):
				o = 'LITERAL'
			elif any(e in operators[k]['O'] for e in ["rdflit", "DB.DBA.RDF_OBJ", "DB.DBA.RDF_MAKE_OBJ"]): #tengo dudas con all_eq
				o = 'LITERAL'
			## OUTPUT ESPECIALES
			elif all(e in operators[k]['O'] for e in ["k_"]) and all(e not in operators[k]['O'] for e in [".S",".O",".P","all_eq","cast"]):
				q = ''
				k_ = operators[k]['O'].split('_')[1].lower()
				for i in set(sparql_file_as_list):
					if k_ in i:
						q = i
				if 'http://' in q:
					o = 'URI'
				else:
					o = 'VAR'
			elif 'all_eq' in operators[k]['O']:
				if 'IRI' in list_alleq_aux[0]:
					o = 'URI'
					del list_alleq_aux[0]
				else:
					o = 'VAR'
					del list_alleq_aux[0]
			else:
				o = 'VAR'

			operators[k]['triple_type'] = s + '_' + p + '_' + o
		else:
			operators[k]['triple_type'] = 'None'
	return operators


def GetSparqlAsList(sparql_file):
	remove_str = ['where','select','and','union','ask','delete','{','}','optional','filter','oder by','group by','limit','prefix']
	aux1 = sparql_file.lower().split('\n')
	aux2 = []
	for i in aux1:
		if 'prefix' not in i and i != '':
			aux2.append(i)
	sparql_list = []
	for i in '\n'.join(aux2).split(' '):
		if '' != i and all(e != i for e in remove_str):
			sparql_list.append(i)
	return sparql_list


def IdentifyAllEq(operators):
	list_alleq = []
	for k in operators.keys():
		if 'all_eq' in operators[k]['profile_text']:
			operators[k]['all_eq_bool'] = 1
			if operators[k]['precode_bool'] == 1:
				operators[k]['get_value_all_eq'] = GetAllEqFromPrecodeText(operators[k])
				list_alleq = list_alleq + operators[k]['get_value_all_eq']
			else:
				operators[k]['get_value_all_eq'] = 'None'
		else:
			operators[k]['all_eq_bool'] = 0
	return operators, list_alleq



def GetAllEqFromPrecodeText(OP):
	aux = OP['precode_text'].split('\n')
	aux_lst = []
	for string in aux:
		if 'Call __all_eq (' in string:
			substring = re.search('\((.*)\)', string)
			aux_lst.append(substring.group(1))
	return aux_lst