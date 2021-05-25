import re

n = [
	'profile_loop_file_test_wikidata5',#0
	'profile_normal_explain_bajo_test_wikidata5',#1
	'profile_normal_file_test_wikidata5',#2
	'profile_order_loop_file_test_wikidata5',#3
	'profile_loop_file_ex023',#4
	'profile_normal_explain_bajo_ex023',#5
	'profile_normal_file_ex023',#6
	'profile_order_loop_file_ex023'#7
	]
profile_sparql = open('profile_test/'+n[1], 'r').read()
#profile_sparql = profile_sparql.replace(' ','')

print(profile_sparql)
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

extract_sparql_profile = ParseNestedBracket(profile_sparql, 1)
extract_sparql_profile = extract_sparql_profile.split("\n")
c = 0
operators = {}
for i in range(0,len(extract_sparql_profile)):
	x = extract_sparql_profile[i].strip()
	
	if x != "":
		if (x.split(" ")[0] == "time"):
			print("OPERATOR" + str(c))
			
			c = c + 1
	print(x)
	
#operators = extract_sparql_profile

#print("|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||")
#print(operators)
