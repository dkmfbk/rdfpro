#!/bin/sh

# Scenario 1

rdfpro @read freebase.nt.gz @tbox @write tbox.tql.gz

rdfpro @read freebase.nt.gz @stats -t 100 @write stats.tql.gz

rdfpro @read freebase.nt.gz { @tbox @write tbox.tql.gz , @stats -t 100 @write stats.tql.gz }

rdfpro { @read freebase.nt.gz , @read freebase_old.nt.gz }d @write new-triples.tql.gz

# Scenario 2

rdfpro @read freebase.nt.gz \
       { @groovy -p 'emitIf(t == fb:music.musical_group)' , \
         @groovy -p 'if(p == fb:music.artist.active_end) emit(s, rdf:type, fb:music.musical_group, null)' }d \
       @write entities.tql.gz

rdfpro @read freebase.nt.gz \
       @groovy 'def init(args) { instances = loadSet("./entities.tql.gz", "s"); };
                emitIf((p == rdfs:label || p == fb:music.artist.genre || p == fb:music.artist.origin)
                       && instances.match(s) );' \
       @write output.tql.gz
