#!/bin/sh

# Steps 1-2 Filtering and TBox extraction
rdfpro { @read metadata.trig , \
         @read vocab/* @transform '=c <graph:vocab>' , \
         @read freebase/* @transform '=c <graph:freebase>' , \
         @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip @transform '=c <graph:geonames>' , \
         @read dbp_en/* @transform '=c <graph:dbp_en>' , \
         @read dbp_es/* @transform '=c <graph:dbp_es>' , \
         @read dbp_it/* @transform '=c <graph:dbp_it>' , \
         @read dbp_nl/* @transform '=c <graph:dbp_nl>'  } \
       @write filtered.tql.gz \
       @tbox \
       @write tbox.tql.gz

# Steps 3-5 Smushing, inference, merging
rdfpro @read filtered.tql.gz \
       @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
              '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
       @rdfs -c '<graph:vocab>' -d tbox.tql.gz \
       @unique -m \
       @write dataset.tql.gz
