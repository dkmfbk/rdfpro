
TQL Library
===========

Turtle Quads (TQL) is a line-oriented RDF format similar to NQuads but using the more permissive (and efficient!) Turtle encoding for characters.
It has been introduced by [DBpedia](http://wiki.dbpedia.org/Internationalization/Guide) for its dump files and it is supported in input by the Virtuoso triple store. TQL is the preferred format for manipulating RDF data in RDFpro.

The `rdfpro-tql` module extends Sesame RIO with support for the TQL format. In order to use it you have to include the following dependency in your `pom.xml`:

    <dependency>
      <groupId>eu.fbk.rdfpro</groupId>
      <artifactId>rdfpro-tql</artifactId>
      <version>0.1</version>
    </dependency>

Then, make sure to include `rdfpro` artifact repository:

    <repository>
      <id>rdfpro</id>
      <url>http://fracor.bitbucket.org/rdfpro/mvnrepo</url>
    </repository>

If you don't use Maven, you have to download and include the following JARs in the classpath:

  * [rdfpro-tql-1.0.jar](http://fracor.bitbucket.org/rdfpro/mvnrepo/eu/fbk/rdfpro/rdfpro-jsonld/0.1/rdfpro-tql-0.1.jar)
  * [slf4j-api-1.7.7.jar](http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.7/slf4j-api-1.7.7.jar) (or higher version)
  * [sesame-rio-api-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-rio-api/2.7.12/sesame-rio-api-2.7.12.jar) (or higher version)
  * [sesame-model-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-model/2.7.12/sesame-model-2.7.12.jar) (or higher version)
  * [sesame-util-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-util/2.7.12/sesame-util-2.7.12.jar) (or higher version)

Note that the three Sesame JARs are not necessary in case the `openrdf-sesame-2.7.X-onejar.jar` file is already included. The SLF4J API is used by Sesame for logging. You need to include also an implementation (e.g., [Logback](http://logback.qos.ch/)) for logging to work.

As TQL is not part of the predefined set of Sesame [`RDFFormat`](http://openrdf.callimachus.net/sesame/2.7/apidocs/org/openrdf/rio/RDFFormat.html)s, it is necessary to register it.
This can be done either via `RDFFormat#register()`, passing the `TQL.FORMAT` constant, or by simply calling method `TQL.register()` on the `TQL` class, which ensures that multiple calls will result in a single registration.

A note on performances. The TQL parser and writer operate by reading / writing a character at a time, relying on the underlying stream for proper buffering. For optimal performances, make sure to use them with a [`BufferedReader`](http://docs.oracle.com/javase/7/docs/api/java/io/BufferedReader.html) or [`BufferedWriter`](http://docs.oracle.com/javase/7/docs/api/java/io/BufferedWriter.html).
