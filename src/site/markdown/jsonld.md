
JSON-LD Library
===============

[JSON-LD](http://www.w3.org/TR/json-ld/) is a JSON-based format for serializing data in (a superset of) RDF as JSON and interpreting JSON contents as RDF; similarly to RDF/XML, JSON-LD provides for several ways to encode the same triples (or quads).

The `rdfpro-jsonld` module extends Sesame RIO with support for the JSON-LD format. JSON-LD writing is directly implemented by the module, while parsing reuses the lightweight [Semargl](http://semarglproject.org/) JSONLD parser, which is adapted to the Sesame API.

In order to use the `rdfpro-jsonld` library you have to include the following dependency in your `pom.xml`:

    <dependency>
      <groupId>eu.fbk.rdfpro</groupId>
      <artifactId>rdfpro-jsonld</artifactId>
      <version>1.0</version>
    </dependency>

Then, make sure to include `rdfpro` artifact repository:

    <repository>
      <id>rdfpro</id>
      <url>https://api.bitbucket.org/1.0/repositories/fracor/mvnrepo/raw/master</url>
    </repository>

If you don't use Maven, you have to download and include the following JARs in the classpath:

  * [rdfpro-jsonld-1.0.jar](http://api.bitbucket.org/1.0/repositories/fracor/mvnrepo/raw/master/eu/fbk/rdfpro/rdfpro-jsonld/1.0/rdfpro-jsonld-1.0.jar)
  * [slf4j-api-1.7.7.jar](http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.7/slf4j-api-1.7.7.jar) (or higher version)
  * [sesame-rio-api-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-rio-api/2.7.12/sesame-rio-api-2.7.12.jar) (or higher version)
  * [sesame-model-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-model/2.7.12/sesame-model-2.7.12.jar) (or higher version)
  * [sesame-util-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-util/2.7.12/sesame-util-2.7.12.jar) (or higher version)
  * [semargl-core-0.6.1.jar](http://central.maven.org/maven2/org/semarglproject/semargl-core/0.6.1/semargl-core-0.6.1.jar)
  * [semargl-jsonld-0.6.1.jar](http://central.maven.org/maven2/org/semarglproject/semargl-jsonld/0.6.1/semargl-jsonld-0.6.1.jar)
  * [semargl-rdf-0.6.1.jar](http://central.maven.org/maven2/org/semarglproject/semargl-rdf/0.6.1/semargl-rdf-0.6.1.jar)

Note that the three Sesame JARs are not necessary in case the `openrdf-sesame-2.7.X-onejar.jar` file is already included. The SLF4J API is used by Sesame for logging. You need to include also an implementation (e.g., [Logback](http://logback.qos.ch/)) for logging to work.

No additional action is required to use the JSON-LD parser: just specify `RDFFormat.JSONLD` when creating the parser with Sesame RIO.
The JSON-LD writer, instead, requires the specification of RioSetting `JSONLD.ROOT_TYPES`.
Its value is the set of class URIs associated to 'top-level' RDF resources in the produced JSON-LD. More in details, the JSON produced by the writer contains an array of JSON structures, one for each RDF resource of one of the configured root types. The JSON of RDF resources that are not root types is instead nested in the JSON of root type resources.
