#!/bin/sh
cd "/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script"

mkdir -p returns_old/pretty_json

for file in returns_old/$sparql_folder_wikidata*'.json'; do
	echo "Prettifying: "${file##*/}
	jq . returns_old/${file##*/} > returns_old/pretty_json/pretty_${file##*/}
done
