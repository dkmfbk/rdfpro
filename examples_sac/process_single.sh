#!/bin/sh

# Step 1 Transform
rdfpro { @read metadata.trig , \
         @read vocab/* @transform '=c <graph:vocab>' , \
         @read freebase/* @transform '=c <graph:freebase>' , \
         @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip @transform '=c <graph:geonames>' , \
         @read dbp_en/* @transform '=c <graph:dbp_en>' , \
         @read dbp_es/* @transform '=c <graph:dbp_es>' , \
         @read dbp_it/* @transform '=c <graph:dbp_it>' , \
         @read dbp_nl/* @transform '=c <graph:dbp_nl>'  } \
       @write filtered.tql.gz
       
# Step 2 TBox extraction
rdfpro @read filtered.tql.gz \
       @tbox \
       @write tbox.tql.gz

# Step 3 Smushing
rdfpro @read filtered.tql.gz \
       @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
              '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
       @write smushed.tql.gz

# Step 4 Inference
rdfpro @read smushed.tql.gz \
       @rdfs -c '<graph:vocab>' -d tbox.tql.gz \
       @write inferred.tql.gz
       
# Step 5 Deduplication
rdfpro @read inferred.tql.gz \
       @unique -m \
       @write dataset.tql.gz
