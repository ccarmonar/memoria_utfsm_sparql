import re, time, os
from functions.aux import GetSubstring,ParseNestedBracket,CleanOperators,GetPrefixes,VectorString, CleanSalts, SubstractStrings


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
		if set(["Cardinality","estimate"]).issubset(l_filt):
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




#Función que identifica si el operador es SCAN, SUBQUERY, u otro.
def IdentifyOperatorType(operator):
	if all(element in operator['profile_text'] for element in ["P =  ", "RDF_QUAD"]):
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
		operator['group_by_read'] = 0
	return operator


def IdentifyDistinct(operator):
	if any(element in operator['profile_text'] for element in ["Distinct ", "Distinct (HASH)", "distinct"]):
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


def GetGSPO(operator):
	lines = operator['profile_text'].split('\n')
	for ls in lines:
		#PREDICADOS
		if " P " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0, len(split_P)):
				if split_P[s] == 'P':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['P'] = VectorString(split_P[s+2:])
					elif "$" in split_P[s+2]:
						operator['P'] = CleanSalts(split_P[s+2], False)
					else:
						operator['P'] = split_P[s+2]
		if " O " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0, len(split_P)):
				if split_P[s] == 'O':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['O'] = VectorString(split_P[s+2:])
					elif "$" in split_P[s + 2]:
						operator['O'] = CleanSalts(split_P[s + 2], False)
					else:
						operator['O'] = split_P[s+2]
		if " S " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0,len(split_P)):
				if split_P[s] == 'S':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['S'] = VectorString(split_P[s+2:])
					elif "$" in split_P[s + 2]:
						operator['S'] = CleanSalts(split_P[s + 2], False)
					else:
						operator['S'] = split_P[s+2]
		if " G " in ls:
			split_P = list(filter(None,ls.strip().split(' ')))
			for s in range(0,len(split_P)):
				if split_P[s] == 'G':
					if split_P[s+2][:2].lower() == "<v" or split_P[s+2][:2].lower() == "<r" or split_P[s+2][:3].lower() == "<$r" or split_P[s+2][:3].lower() == "<$v":
						operator['G'] = VectorString(split_P[s+2:])
					elif "$" in split_P[s + 2]:
						operator['G'] = CleanSalts(split_P[s + 2], False)
					else:
						operator['G'] = split_P[s+2]
	return operator


#Función que retorna todos los predicados leidos en un query profile
def GetAllPredicatesFromProfile(operators):
	set_predicates = set(())
	for k in operators.keys():
		if 'P' in operators[k].keys():
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


def GetStartAndEndOptionalSection(operator, key):
	if key == 'OP1' or key == 'OP0':
		operator['start_optional'] = 0
		operator['end_optional'] = 0
		return operator
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
		return operator


def SetBooleanOptionalSection(operators):
	optional_boolean = 0
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

	return operators


# EN CONSTRUCCION TAL VEZ SE HAGA
def GetIRI_ID(sparql_query, triple_component):
	if triple_component == 'P':
		main_selection = ParseNestedBracket(sparql_query,0)
		prefixes = GetPrefixes(sparql_query)
	return main_selection


def AddJoinType(operators):
	return 0


def GetLimit():
	return 0


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



