
RDFpro Core Library
===================

The `rdfpro-core` library provides the `RDFProcessor` abstraction, the implementation of processors for common tasks and the logic required for composing processors in a pipeline.
This library represent the basis of the `rdfpro` command line tool and can be embedded in applications to perform `rdfpro` tasks via Java code.

In order to use the `rdfpro-core` library you have to include the following dependency in your `pom.xml`:

    <dependency>
      <groupId>eu.fbk.rdfpro</groupId>
      <artifactId>rdfpro-core</artifactId>
      <version>1.0</version>
    </dependency>

Then, make sure to include `rdfpro` artifact repository:

    <repository>
      <id>rdfpro</id>
      <url>https://api.bitbucket.org/1.0/repositories/fracor/mvnrepo/raw/master</url>
    </repository>

If you don't use Maven, you have to download and include the following JARs in the classpath:

  * [rdfpro-core-1.0.jar](http://api.bitbucket.org/1.0/repositories/fracor/mvnrepo/raw/master/eu/fbk/rdfpro/rdfpro-core/1.0/rdfpro-core-1.0.jar)
  * [slf4j-api-1.7.7.jar](http://central.maven.org/maven2/org/slf4j/slf4j-api/1.7.7/slf4j-api-1.7.7.jar) (or higher version)
  * [sesame-rio-api-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-rio-api/2.7.12/sesame-rio-api-2.7.12.jar) (or higher version)
  * [sesame-model-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-model/2.7.12/sesame-model-2.7.12.jar) (or higher version)
  * [sesame-util-2.7.12.jar](http://central.maven.org/maven2/org/openrdf/sesame/sesame-util/2.7.12/sesame-util-2.7.12.jar) (or higher version)

Note that the three Sesame JARs are not necessary in case the `openrdf-sesame-2.7.X-onejar.jar` file is already included. The SLF4J API is used by Sesame for logging. You need to include also an implementation (e.g., [Logback](http://logback.qos.ch/)) for logging to work.

The library entry point is the abstract class `RDFProcessor`. You can use its static method to instantiate basic processors and aggregate them in composite processors via sequence and parallel composition. You can also extend the class realizing your own processor, which can then be used and composed in pipelines as done with builtin processors. See the Javadoc for further details.
