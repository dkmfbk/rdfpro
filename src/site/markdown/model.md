
RDFpro processing model
=======================

RDFpro processing model is centred around the concept of **RDF processor**, a reusable Java component that consumes an input stream of RDF quads (i.e., an RDF triple with a fourth named graph component) in one or more passes, produces an output stream of quads and may have side effect like writing RDF data.

Technically, a processor extends the `RDFProcessor` class as shown below:

    abstract class RDFProcessor {
        int getExtraPasses();
        RDFHandler getHandler(RDFHandler sink);
    }

    interface RDFHandler {
        void startRDF();
        void handleComment(String comment);
        void handleNamespace(String p, String n);
        void handleStatement(Statement quad);
        void endRDF();
    }

An `RDFProcessor` declares how many extra passes it needs on its input and providing, upon request, an `RDFHandler` (Sesame interface) where quads, prefix/namespace bindings and comments in RDF data can be fed and handled, with the result sent to a sink RDFHandler.
Differently from Sesame, that does not specify whether an `RDFHandler` can be simultaneously accessed by multiple threads, here we expect methods `handleComment`, `handleNamespace` and `handleStatement` to be called concurrently by multiple threads to achieve higher throughputs.
Thread management is done by RDFpro with the goal of using as many threads as reasonable (~ the number of available cores) to concurrently invoke methods of `RDFProcessor`s.

RDFpro offers a number of processors for common tasks that can be easily extended by users (see the [tool usage page](usage.html) for their detailed description).
It is worth noting that some processors make use of the `sort` utility to perform tasks that cannot be done one quad at a time, enabling their execution on arbitrarily large inputs (disk space permitting).
In addition to the `sort` utility, RDFpro also exploits the native `gzip`, `bzip2`, `xz` and `7za` utilities (and the multi-threaded version of the first two: `pigz` and `pbzip2`) to efficiently deal with compressed data.

Importantly, RDFpro allows to derive new processors by (recursively) applying sequential and parallel compositions, as shown below.

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="model.png" alt="Processor model, sequence and parallel compositions "/>
</div>

In a sequential composition (second figure), two or more processors `@Pi` are chained so that the output stream of `@Pi` become the input stream of `@Pi+1`.
In a parallel composition (third figure), the input stream is sent concurrently to several processors `@Pi`, whose output streams are merged into a resulting stream according to one of several possible merging criteria: union (`u` flag), intersection (`i` flag) and difference (`d` flag) of quads collected from different parallel branches.
For both types of composition, RDFpro introduces special buffers in front of each RDF processor that collect a fraction of incoming quads and triggers their processing in separate threads when full; the fraction is adapted at runtime using heuristics trying to ensure that all CPU cores are exploited.

An example of composition and the corresponding RDFpro command line syntax are shown below

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="composition.png" alt="Composition example"/>
</div>

In this example, a Turtle+gzip file `file.ttl.gz` is read, TBox and VOID statistics are extracted in parallel and their union (`U` flag) is written to an RDF/XML file onto.rdf.
This example shows how I/O can be done not only using input and output streams but also with specialized `@read` and `@write` processors that augment or dump the current stream at any point of the pipeline.
Actually, while input and output streams are a central feature for interconnecting processors and for an application to interact with the RDFpro library, the RDFpro command line tool ignores them: it builds the composite processor as requested, feed it with an empty stream and dumps its final result, relying on the presence of `@read` and `@write` processors for all the I/O.

The availability of several builtin processors, the possibility to implement new processors for specific tasks reusing RDFpro infrastructure and the capability to arbitrarily compose processors provide a lot of flexibility, that we hope can help users in addressing a variety of tasks, especially in Linked Data integration scenario where large datasets have to be processed.
