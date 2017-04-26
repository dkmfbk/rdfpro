RDFpro: an extensible tool for building stream-oriented RDF processing pipelines
================================================================================

RDFpro (RDF Processor) is a public domain (Creative Commons CC0) Java command line tool and embeddable library that offers a suite of stream-oriented, highly optimized processors for common tasks such as data filtering, RDFS inference, smushing and statistics extraction.
RDFpro processors are extensible by users and can be freely composed to form complex pipelines to efficiently process RDF data in one or more passes.
RDFpro model and multi-threaded design allow processing billions of triples in few hours in typical Linked Open Data integration scenarios.

[RDFpro Web site](http://rdfpro.fbk.eu/)

Building RDFpro
---------------
To build RDFpro, you need to have [`Maven`](https://maven.apache.org/) installed on your machine. 
In order to build RDFpro, you can run the following commands:

    $ git clone https://github.com/dkmfbk/rdfpro.git  (1)
    $ cd rdfpro                                       (2)
    $ git checkout BRANCH_NAME                        (3)
    $ mvn package -DskipTests -Prelease               (4)

Step (3) is optional, if you want to build a specific branch, otherwise the version on top of the `master` branch will be built.  

The `-DskipTests` flag in step (4) disable unit testing to speed up the building process: if you want to run the tests, just omit the flag. The `-Prelease` flag activates a Maven profile called "release" that enables the generation of the same `tar.gz` archive including everything that we distribute as RDFpro binaries on the website. This `tar.gz` is located under:

    rdfpro-dist/target/rdfpro-dist-VERSION-bin.tar.gz  

You may copy it wherever you want, extract it and run rdfpro via the included `rdfpro` script.