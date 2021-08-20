#!/bin/sh
mkdir -p returns/pretty_json
for file in returns/$sparql_folder_wikidata*'.json'; do
	echo "Prettifying: "${file##*/}
	jq . returns/${file##*/} > returns/pretty_json/pretty_${file##*/}
done
