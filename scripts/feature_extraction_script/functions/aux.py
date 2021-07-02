import re, time, os, hashlib


def GetSubstring(string_text,pattern1,pattern2,dotall = True):
	if dotall==True:
		substring = re.search(pattern1 + '(.+?)' + pattern2, string_text, flags=re.DOTALL)
		if substring:
			substring_return = substring.group(0)
		else:
			return None
	else:
		substring = re.search(pattern1 + '(.+?)' + pattern2, string_text)
		if substring:
			substring_return = substring.group(0)
		else:
			return None
	return substring_return
	
	
#Funcion que sirve para entrar dentro de los {} según nivel en un string. Esta enfocado a JSON pero sirve para al comienzo.
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


#Funcion que hace una limpieza y saca los operadores que tengan texto vacio
def CleanOperators(operators):
	remove_list = []
	for k in operators:
		if operators[k]['profile_text'] == '' or all(element not in operators[k]['profile_text'] for element in ["time", "fanout", "input", "rows"]):
			remove_list.append(k)
	for k in remove_list:
		operators.pop(k)
	return operators


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


#Rearma el string en caso de que el objeto este en formato vector. Ocurre cuando se expande el explain
def VectorString(auxlist):
	vector_str = ""
	for i in auxlist:
		if i[-1] == ">":
			vector_str = vector_str + i
			break
		vector_str = vector_str + i + " "
	vector_str = CleanSalts(vector_str,True)
	return vector_str


def CleanSalts(string, vector=True):
	if vector:
		return_cleaning = string.split(" ")[1]
	else:
		return_cleaning = string
	return_cleaning = return_cleaning.split("$")[0]
	return return_cleaning


def MainCurlyBrackets(sparql_file):
	st = sparql_file
	return st[st.find("{")+1:st.rfind("}")]


def CountCurlyBrackets(operator):
	profile_text = operator['profile_text']
	operator['{'] = profile_text.count('{')
	operator['}'] = profile_text.count('}')
	return operator


def SubstractStrings(a, b):
	return "".join(a.rsplit(b))


#Función que retorna todos los predicados leidos en un query profile
def GetAllPredicatesFromProfile(operators):
	set_predicates = set(())
	for k in operators.keys():
		if 'P' in operators[k].keys():
			set_predicates.add(operators[k]['P'])
	return list(set_predicates)


def HashStringId(string):
	sha = hashlib.sha256()
	sha.update(string.encode())
	sha_return = sha.hexdigest()
	return sha_return