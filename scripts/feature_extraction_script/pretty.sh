#!/bin/sh
cd "/home/c161905/Memoria/memoria_utfsm_sparql/scripts/feature_extraction_script"

mkdir -p returns/pretty_json

for file in returns/$sparql_folder_wikidata*'.json'; do
	echo "Prettifying: "${file##*/}
	jq . returns/${file##*/} > returns/pretty_json/pretty_${file##*/}
done

