<div class="well sidebar" id="well-home">
    <h1>
        <img src='images/rdfpro-150.png' />
        RDF<sub>pro</sub>
    </h1>
    
    <p class='title2'>
        The Swiss-Army tool for RDF and Named Graph manipulation
    </p>
                
    <p class='title2'>
        <a class="btn btn-primary btn-large" href="install.html">Download</a>
        <a class="btn btn-primary btn-large" href="https://knowledgestore2.fbk.eu/rdfpro-demo/">Online demo</a>
        <a class="btn btn-primary btn-large" href="install.html">Video tour</a>
    </p>
</div>

---------------------------------------

### About

**RDFpro** (RDF Processor) is a public domain, Java command line tool and library for **RDF processing**.
RDFpro offers a suite of stream-oriented, highly optimized **RDF processors** for common tasks that can be assembled in complex **pipelines** to efficiently process RDF data in one or more passes.
RDFpro originated from the need of a tool supporting typical **Linked Data integration tasks**, involving dataset sizes up to few **billions triples**.

[learn more...](model.html)

### Features

- RDF quad (triple + graph) filtering and replacement (with [Groovy](http://groovy.codehaus.org/) scripting support)
- RDFS inference with selectable rules
- owl:sameAs [smushing](http://patterns.dataincubator.org/book/smushing.html)
- TBox and VOID statistics extraction
- RDF deduplication and set/multiset operations
- data upload/download via SPARQL endpoints
- data read/write in multiple (compressed) formats (rdf, rj, jsonld, nt, nq, trix, trig, tql, ttl, n3, brf)
- command line [tool](usage.html) plus [core](rdfprolib.html), [tql](tql.html), [jsonld](jsonld.html) libraries
- based on [Java 8](http://www.oracle.com/technetwork/java/javase/overview/java8-2100321.html) and [Sesame](http://www.openrdf.org/)
- public domain software ([Creative Commons CC0](license.html))


### News

- 2015-02-11 Version 0.3 has been released, RDFpro migrates to GitHub
- 2014-12-01 [Paper](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2015sac.pdf) accepted at [SAC 2015](http://www.acm.org/conferences/sac/sac2015/) conference
- 2014-09-01 [Paper](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2014iswcSemDev01.pdf) accepted at [ISWC 2014 SemDev](http://iswc2014.semdev.org/) workshop
- 2014-08-04 Version 0.2 has been released
- 2014-07-24 Version 0.1 has been released
