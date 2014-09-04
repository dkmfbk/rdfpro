
<div class="row">
<br/>
<br/>
<div class="span12">
<div class="well sidebar" style="text-align: center">
<h1 style="font-size:400%">RDF<sub>pro</sub></h1><br/>
<p style="font-size:200%">An Extensible Tool for Building Stream-Oriented RDF Processing Pipelines</p><br/>
<form method="GET" action="install.html"><button class="btn btn-primary btn-large" type="submit" style="font-size:150%">Download</button></form>
</div>
</div>
</div>

---------------------------------------

### About

**RDFpro** (RDF Processor) is a public domain, Java command line tool and library for **RDF processing**.
RDFpro offers a suite of stream-oriented, highly optimized **RDF processors** for common tasks that can be assembled in complex **pipelines** to efficiently process RDF data in one or more passes.
RDFpro originated from the need of a tool supporting typical **Linked Data integration tasks**, involving dataset sizes up to few **billions triples**.

[learn more...](model.html)

### Features

- RDF quad (triple + graph) filtering and replacement
- RDFS inference with selectable rules
- owl:sameAs [smushing](http://patterns.dataincubator.org/book/smushing.html)
- TBox and VOID statistics extraction
- RDF deduplication, intersection and difference
- data upload/download via SPARQL endpoints
- data read/write in multiple (compressed) formats (rdf, rj, jsonld, nt, nq, trix, trig, tql, ttl, n3, brf)
- command line [tool](usage.html) + [core](rdfprolib.html), [tql](tql.html), [jsonld](jsonld.html) libraries
- based on [Sesame](http://www.openrdf.org/).
- public domain software ([Creative Commons CC0](license.html))


### News

- 2014-08-04 Version 0.2 has been released
- 2014-07-24 Version 0.1 has been released
