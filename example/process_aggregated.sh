#!/bin/sh

# Steps 1-2 Filtering and TBox extraction
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
       @write filtered.tql.gz \
       @tbox \
       @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
       @write tbox.tql.gz

# Steps 3-6 Smushing, inference, merging and statistics extraction
rdfpro @read filtered.tql.gz \
       @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
              '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
       @rdfs -c '<graph:vocab>' -e rdfs4a,rdfs4b,rdfs8 -d tbox.tql.gz \
       @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
       @unique -m \
       @write dataset.tql.gz \
       @stats \
       @read tbox.tql.gz \
       @write statistics.tql.gz

