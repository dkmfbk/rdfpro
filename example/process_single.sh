#!/bin/sh

# Step 1 Filtering
rdfpro { @read metadata.trig , \
         @read vocab/* @transform '=c <graph:vocab>' , \
         @read freebase/* @transform '-spo fb:common.topic fb:common.topic.article fb:common.topic.notable_for
             fb:common.topic.notable_types fb:common.topic.topic_equivalent_webpage <http://rdf.freebase.com/key/*>
             <http://rdf.freebase.com/ns/common.notable_for*> <http://rdf.freebase.com/ns/common.document*>
             <http://rdf.freebase.com/ns/type.*> <http://rdf.freebase.com/ns/user.*> <http://rdf.freebase.com/ns/base.*>
             <http://rdf.freebase.com/ns/freebase.*> <http://rdf.freebase.com/ns/dataworld.*>
             <http://rdf.freebase.com/ns/pipeline.*> <http://rdf.freebase.com/ns/atom.*>  <http://rdf.freebase.com/ns/community.*>
             =c <graph:freebase>' , \
         @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip \
             @transform '-p gn:childrenFeatures gn:locationMap
                 gn:nearbyFeatures gn:neighbouringFeatures gn:countryCode
                 gn:parentFeature gn:wikipediaArticle rdfs:isDefinedBy
                 rdf:type =c <graph:geonames>' , \
         { @read dbp_en/* @transform '=c <graph:dbp_en>' , \
           @read dbp_es/* @transform '=c <graph:dbp_es>' , \
           @read dbp_it/* @transform '=c <graph:dbp_it>' , \
           @read dbp_nl/* @transform '=c <graph:dbp_nl>' } \
               @transform '-o bibo:* -p dc:rights dc:language foaf:primaryTopic' } \
       @transform '+o <*> _:* * *^^xsd:* *@en *@es *@it *@nl' \
       @transform '-o "" ""@en ""@es ""@it ""@nl' \
       @write filtered.tql.gz

# Step 2 TBox extraction
rdfpro @read filtered.tql.gz \
       @tbox \
       @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
       @write tbox.tql.gz

# Step 3 Smushing
rdfpro @read filtered.tql.gz \
       @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
              '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
       @write smushed.tql.gz

# Step 4 Inference
rdfpro @read smushed.tql.gz \
       @rdfs -c '<graph:vocab>' -e rdfs4a,rdfs4b,rdfs8 -d tbox.tql.gz \
       @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
       @write inferred.tql.gz

# Step 5 Merging
rdfpro @read inferred.tql.gz \
       @unique -m \
       @write dataset.tql.gz

# Step 6 Statistics extraction
rdfpro { @read tbox.tql.gz , @read dataset.tql.gz @stats } @write statistics.tql.gz

