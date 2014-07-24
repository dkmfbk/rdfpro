#!/bin/sh

# Step 1 Filtering
rdfpro { @read -w metadata.trig , \
         @read -w vocab/* @filter -r "cu '<graph:vocab>'" , \
         @read -w freebase/* @filter "spou -|http://rdf\.freebase\.com/key/.*|
             -|http://rdf\.freebase\.com/ns/common\.(notable_for\|document).*|
             -fb:common.topic -fb:common.topic.article -fb:common.topic.notable_for
             -fb:common.topic.notable_types -fb:common.topic.topic_equivalent_webpage
             -|http://rdf\.freebase\.com/ns/(type\|user\|base\|freebase\|dataworld\|pipeline\|atom\|community)\..*|" \
             -r "cu '<graph:freebase>'" , \
         @read -w geonames/*.rdf geonames:geonames/all-geonames-rdf.zip \
             @filter "pu -gn:childrenFeatures -gn:locationMap
                 -gn:nearbyFeatures -gn:neighbouringFeatures -gn:countryCode
                 -gn:parentFeature -gn:wikipediaArticle -rdfs:isDefinedBy
                 -rdf:type" -r "cu '<graph:geonames>'" , \
         { @read -w dbp_en/* @filter -r "cu '<graph:dbp_en>'" , \
           @read -w dbp_es/* @filter -r "cu '<graph:dbp_es>'" , \
           @read -w dbp_it/* @filter -r "cu '<graph:dbp_it>'" , \
           @read -w dbp_nl/* @filter -r "cu '<graph:dbp_nl>'" } \
             @filter "ou -bibo pu -dc:rights -dc:language -foaf:primaryTopic" } \
       @filter "ol -'' o@ +'en' +'es' +'it' +'nl' -* o^ +xsd -*" \
       @write filtered.tql.gz

# Step 2 TBox extraction
rdfpro @read filtered.tql.gz \
       @tbox \
       @filter "spou -bibo -con -owl:Thing -schema:Thing -foaf:Document -dc:subject -foaf:page -dct:relation" \
       @write tbox.tql.gz

# Step 3 Smushing
rdfpro @read filtered.tql.gz \
       @smush -S 2048M http://dbpedia http://it.dbpedia http://es.dbpedia \
           http://nl.dbpedia http://rdf.freebase.com http://sws.geonames.org \
       @write smushed.tql.gz

# Step 4 Inference
rdfpro @read smushed.tql.gz \
       @infer -c '<graph:vocab>' -r rdfs1,rdfs2,rdfs3,rdfs5,rdfs6,rdfs7,rdfs9,rdfs10,rdfs11,rdfs12,rdfs13 -d tbox.tql.gz \
       @filter "tu -bibo -con -owl:Thing -schema:Thing -foaf:Document pu -bibo -con -dc:subject -foaf:page -dct:relation" \
       @write inferred.tql.gz


# Step 5 Merging
rdfpro @read inferred.tql.gz \
       @unique -m \
       @write dataset.tql.gz

# Step 6 Statistics extraction
rdfpro { @read tbox.tql.gz , @read dataset.tql.gz @stats } @prefix @write statistics.tql.gz
