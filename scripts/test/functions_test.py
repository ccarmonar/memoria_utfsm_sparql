import json, os.path
#from pathlib import Path

def compare_json(filename):
    returns_path = '/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script/returns/'
    correct_results = '/home/c161905/Memoria/memoria_utfsm_sparql/scripts/test/correct_results/'
    filename_complete = filename+".json"
    if os.path.isfile(returns_path+filename_complete) and os.path.isfile(correct_results+filename_complete):
        with open(returns_path+filename_complete) as jsonFile_ts:
            jsonObject_test = json.load(jsonFile_ts)
            jsonFile_ts.close()

        with open(correct_results+filename_complete) as jsonFile_tr:
            jsonObject_target = json.load(jsonFile_tr)
            jsonFile_tr.close()

        if jsonObject_test.keys() == jsonObject_target.keys():
            for k in jsonObject_test.keys():
                for subk in jsonObject_test[k].keys():
                    if all(element != subk for element in ["time","fanout", "input_rows", "cardinality_estimate", "cardinality_fanout", "profile_text", "profile_text_low_explain", "precode_text", "after_code_text"]):
                        if jsonObject_test[k][subk] == jsonObject_target[k][subk]:
                            continue
                        else:
                            print("ERROR: "+filename)
                            print("TEST: "+filename+"["+k+"]"+"["+subk+"] = "+str(jsonObject_test[k][subk]))
                            print("TARGET: " + filename + "[" + k + "]" + "[" + subk + "] = " + str(jsonObject_target[k][subk]))
                            return False
            return True

        else:
            print("ERROR: n° operations not equal")
            return False
    else:
        print("Some file not exist in returns or correct_results. Filename: "+filename)
        return False

#print(compare_json("test_wikidata1"))

