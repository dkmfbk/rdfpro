
RDFpro usage
============

 * [RDFpro invocation](#invocation)
 * [Pipeline specification](#pipeline)
 * [I/O processors](#ioprocs)
    * [@read](#read)
    * [@write](#write)
    * [@query](#query)
    * [@update](#update)
 * [Transformation processors](#transformprocs)
    * [@transform](#transform)
    * [@groovy](#groovy)
    * [@rdfs](#rdfs)
    * [@smush](#smush)
    * [@unique](#unique)
 * [Other processors](#otherprocs)
    * [@stats](#stats)
    * [@tbox](#tbox)
    * [@prefix](#prefix)


### <a class="anchor" id="invocation"></a> RDFpro invocation

The command line tool can be invoked using the `rdfpro` script. Its syntax is:

    rdfpro -v | -h | [-V] SPEC

where options -v and -h display respectively the tool version and its online help text, while `SPEC` is the specification of the RDF processing pipeline that is built and executed by the tool; option -V enables the 'verbose' mode where additional debugging information is logged.


### <a class="anchor" id="pipeline"></a> Pipeline specification

The pipeline specification `SPEC` is based on the recursive application of the following rules:

    SPEC ::=
        @p args                            instantiates builtin processor @p with argument list args
        @p1 args1 ... @pN argsN            sequence composition of processors @p1 ... @pN with their args
        { @p1 args1 , ... , @pN argsN }S   parallel composition of processors @p1 ... @pN with their args
                                           and their outputs merged using set/multiset operator S

Denoting with Mo(q), Mi(q) the mulitplicity of a quad in the output stream, `i`-th input stream, the available set/multiset operators `S` and their semantics are:

  * `<no letter>` or `a` -- multiset sum of `@p1` ... `@pN` outputs, with `Mo(q) = sum(Mi(q))` (very fast);
  * `u` -- set union of `@p1` ... `@pN` outputs, with `Mo(q) = max(Mi(q), 1)` (same behaviour of processor `@unique`);
  * `U` -- multiset union of `@p1` ... `@pN` outputs, with `Mo(q) = max(Mi(1))`;
  * `i` -- set intersection of `@p1` ... `@pN` outputs, with `Mo(q) = min(Mi(q), 1)`;
  * `I` -- multiset intersection of `@p1` ... `@pN` outputs, with `Mo(q) = min(Mi(q))`;
  * `d` -- set difference `@p1` \ `@p2` \ ... \ `@pN`), with `Mo(q) = max(0, min(M0(q), 1) - sum_i>0(Mi(q)))`;
  * `D` -- multiset difference `@p1` \ `@p2` \ ... \ `@pN`), with `Mo(q) = max(0, M0(q) - sum_i>0(Mi(q)))`;
  * `s` -- symmetric set difference of `@p1` and `@p2` outputs, i.e. `@p1 \ @p2 union @p2 \ @p1` (using set difference and union);
  * `S` -- symmetric multiset difference of `@p1` and `@p2` outputs, i.e. `@p1 \ @p2 union @p2 \ @p1` (using multiset difference and union).

Note that set operators different from `a` are implemented using sorting (`sort`) and thus are slower than the default `a`.

As an example, the following command invokes `rdfpro` in verbose mode with a pipeline that reads a Turtle+gzip file `file.ttl.gz`, extract TBox and VOID statistics in parallel and writes their set union (`u` flag) to the RDF/XML file `onto.rdf`.

    rdfpro -V @read file.ttl { @stats , @tbox }u @write onto.rdf


### <a class="anchor" id="ioprocs"></a> I/O processors

The builtin processors `@read`, `@write`, `@query` and `@update` allow to move data in and out of RDFpro. We describe them below, reporting both long and short name (e.g., `@read` and `@r`) and their arguments.

#### <a class="anchor" id="read"></a> @read

    @read|@r [-b BASE] [-p] URL...

Reads quads from one or more files, injecting them in the input stream to produce the emitted output stream.
Multiple files are read in parallel and multiple threads are used to parse files that use a line-oriented RDF syntax (NTriples, NQuads, TQL).

Option `-b BASE` can be used to specify a base URI for resolving relative URIs in the input files.

Option `-p` causes BNodes in input files to be preserved.
If not specified, the default is to rewrite BNodes on a per-file basis, so that no BNode clash may occur between BNodes in different files or between a parsed BNode and a BNode in the processor input stream.

Arguments `URL...` identify the files to read; local files (absolute or relative paths) as well as `file://`, `http://`, `https://` URLs can be supplied.
For each file, its RDF format and compression scheme are detected based on the extension (e.g., ttl.gz -> gzipped Turtle).
This information must be explicitly provided in case the extension is not informative, by prepending the correct extension as `.ext:` to the URL (e.g., by transforming `my_unknown_file` to `.ttl.gz:my_unknown_file`).

The following RDF formats are detected and supported: `rdf`, `rj`, `jsonld`, `nt`, `nq`, `trix`, `trig`, `tql`, `ttl`, `n3`, `brf`, `geonames`.
The following compression schemes are detected and supported (provided the corresponding native compression/decompression utility is available): `gz`, `bz2`, `xz`, `7z`.
Shell expansion can be exploited to list multiple files.

#### <a class="anchor" id="write"></a> @write

    @write|@w URL...

Writes quads from the input stream to files.

Arguments `URL...` identify the files to write.
Currently, only `file://` URLs, possibly given as absolute or relative paths, can be used.
If multiple files are specified, quads are allocated to them according to a round-robin strategy that produces files of similar sizes.
This behaviour can be used to split a large dataset into smaller and more manageable files.
RDF formats and compression schemes are detected from the file extensions, or (similarly to `@read`) can be explicitly supplied using the notation `.ext:filename` (e.g., `.ttl.gz:filename`).

The same RDF formats (except `geonames`) and compression schemes of `@read` are supported.
The output stream of this processor is the input stream unchanged, thus allowing to chain `@write` with other downstream processors.

#### <a class="anchor" id="query"></a> @query

    @query [-q QUERY] [-f FILE] [-p] URL

Downloads quads from a SPARQL endpoint using a SPARQL CONSTRUCT or SELECT query.

Option `-q QUERY` supplies the SPARQL query inline.
SELECT queries produce quads based on the bindings of variables s, p, o, c.
CONSTRUCT queries return quads in the default graph (i.e., plain triples).

Option `-f FILE` provides a file containing the SPARQL query to be submitted.
This option is mutually exclusive with option `-q`.

Option `-p` causes BNodes in downloaded data to be preserved.
Note however that BNodes from a SPARQL query may change from execution to execution depending on the endpoint implementation, as in principle they are scoped locally to the query.

Argument `URL` specifies the URL of the SPARQL endpoint.

Downloaded quads are emitted in the output stream together with quads from the input stream.


#### <a class="anchor" id="update"></a> @update

    @update [-s SIZE] URL

Uploads quads in the input stream to a SPARQL endpoint using SPARQL Update INSERT DATA calls.

Option `-s SIZE` specifies the maximum size of a chunk of data uploaded with a SPARQL Update call (default 1024 quads).

Argument `URL` specifies the URL of the SPARQL endpoint.

The output stream of this processor is the input stream unchanged, thus allowing to chain `@update` with other downstream processors.


### <a class="anchor" id="transformprocs"></a> Data transformation processors

Processors `@transform`, `@groovy`, `@rdfs`, `@smush`, `@unique` implement different forms of data transformation.

#### <a class="anchor" id="transform"></a> @transform

    @transform|@t [EXP]

Discards and/or replaces quads based on the rules contained in the supplied string.
Given `X` a quad component (possible values: `s`, `p`, `o`, `c`), the string contains three types of rules:

  * `+X value list` -- quad is dropped if `X` does not belong to `value list`;
  * `-X value list` -- quad is dropped if `X` belongs to `value list`;
  * `=X value` -- quad component `X` is replaced with `value` (evaluated after filters).

Note: for a given component `X`, only a rule `+X` or `-X` can appear. If you have more than one of such rules, consider adding multiple @transform processors to your pipeline.
Values must be encoded in Turtle. The following wildcard values are supported:

  * `<*>` -- any URI;
  * `_:*` -- any BNode;
  * `*` -- any literal;
  * `*@*` -- any literal with a language;
  * `*@xyz` -- any literal with language `xyz`;
  * `*^^*` -- any typed literal;
  * `*^^<uri>` -- any literal with datatype `<uri>`;
  * `*^^ns:uri` -- any literal with datatype `ns:uri`;
  * `*^^ns:*` -- any typed literal with datatype prefixed with `ns:`;
  * `ns:*` -- any URI prefixed with `ns:`;
  * `<ns*>` -- any URI with namespace URI `ns`.

*Examples*. We report some concrete examples of how to use the filtering mechanism:

  * `@transform '-p foaf:name'` -- removes all `foaf:name` quads;
  * `@transform '+p rdf:type +s <*> +o <*>'` -- keeps only `rdf:type` quads whose subject and object are URIs;
  * `@transform '+p rdf:type =c <http://example.org/mygraph>'` -- extracts `rdf:type` quads, placing them in named graph `<http://example.org/mygraph>`.

#### <a class="anchor" id="groovy"></a> @groovy

    @groovy [-p] SCRIPT [ARG...]

Transform the stream using a user-supplied Groovy `SCRIPT`. Extra arguments `ARG...` are passed to the script. Option `-p` enables script pooling, i.e., the execution of multiple script instances in parallel each one on a subset of the input stream; pooling is faster but makes meaningless the use of global variables in the script (there is no such thing as a global state).

The script is executed for each quad (preserving script variables among invocations) passing it the following modifiable variable (note: changing a variable implies a modification of the current quad):

  * `q` - the current quad being transformed
  * `s`, `p`, `o`, `c` the quad subject, predicate, object and graph (context)
  * `t` the type of an `rdf:type` quad (otherwise undefined)
  * `l` and `d` the literal label and datatype (undefined if object is not a literal)

In alternative, the script may provide a function `handle(quad)` that is called for each quad in place of executing the whole script. Independently of the use of `handle(quad)`, the following three callback functions can be defined by the script:

  * `init(args)` - called when the script is initialized with the `ARG...` arguments supplied to the @groovy processor
  * `start(pass)` - called each time a pass on input data is started, supplying the 0-based integer index of the pass
  * `end(pass)` - called each time a pass on input data is completed, supplying the 0-based integer index of the pass

The body of the script may use any Groovy construct or library function (including Java libraries that are accessible in Groovy). In addition, scripts may use the syntax `prefix:name` and `<uri_string>` to enter respectively QNames and full URIs (resolved to Sesame URI constants).
Groovy implementation of SPARQL 1.1 functions are available to the script (just enter their name in lowercase) as well as the following special functions

  * `emit()` - emits the input quad (possibly changed by operating on variables `q`, `s`, `p`, `o`, `c`, `t`, `l`, `d`)
  * `emitIf(condition)` - emits the input quad only if `condition` evaluates to `true`
  * `emitIfNot(condition)` - emits the input quad only if `condition` evaluates to `false`
  * `emit(quad)` - emits the supplied quad, passed as a Sesame `Statement` object
  * `emit(s, p, o, c)` - emits the quad formed by the supplied subject, predicate, object and context (any value can be passed for them - proper conversion will be done by RDFpro)
  * `error(message)` - halts the script execution with the supplied error message
  * `error(message, throwable)` - halts the script execution with the supplied error message and exception object
  * `log(message)` - logs the specified message

*Important: input quads are never propagated automatically. Emission of quads in the output stream occurs only by invoking one of the `emitXXX(...)` methods!*

Finally, note that the boolean flag `__rdfpro__` is set to true when the script is run inside RDFpro (to distinguish from other forms of script invocation, e.g. from the command line).

#### <a class="anchor" id="rdfs"></a> @rdfs

    @rdfs [-e RULES] [-d] [-t] [-C | -c URI] [-b BASE] [-p] [URL...]

Emits the RDFS deductive closure of input quads.
One or more TBox files are loaded and their RDFS closure is computed and (possibly) emitted first.
Next, the domain, range, sub-class and sub-property axioms from the TBox are used to do inference on input quads one at a time, placing inferences in the same graph of the input quad.

Option `-e RULES` specifies the RDFS rules to exclude and accepts a comma-separated list of rule names.
Available rules are `rdfd2`, `rdfs1`, `rdfs2`, `rdfs3`, `rdfs4a`, `rdfs4b`, `rdfs5`, `rdfs6`, `rdfs7`, `rdfs8`, `rdfs9`, `rdfs10`, `rdfs11`, `rdfs12`, `rdfs13`.
Rules `rdfs4a`, `rdfs4b` and `rdfs8` generates a lot of quads of the form `<x rdf:type rdfs:Resource>`. Exclude these rules to reduce the volume of generated data.

Option `-d` causes simple OWL axioms in the TBox to be decomposed to their corresponding RDFS axioms, where possible, so that they can affect RDFS reasoning (e.g., `owl:equivalentClass` can be expressed in terms of `rdfs:subClassOf`).

Option `-t` causes uninformative <x rdf:type \_:b> statements, with \_:b a BNode, to be dropped (default: keep).

Options `-C` and `-c URI` control the graph where the TBox closure is emitted.
This graph is the default (unnamed) graph if option `-C` is specified, otherwise the URI given by `-c` is used as the targed graph.
If none of these options is specified, the TBox closure is not emitted.

Option `-b BASE` specifies the base URI to be used for resolving relative URIs in the TBox file.

Option `-p` can be used to preserve BNodes in TBox files.
If not given, BNodes are rewritten on a per-file basis, similarly to `@read` behaviour.

Arguments `URL...` identify the TBox files to be read.
The same restrictions and explicit extension prefix supported by `@read` can be used.

Note. The algorighm used by `@rdfs` avoids expensive join operations and works with arbitrarily large datasets, provided that their TBox fits into memory.
The result represents the complete RDFS closure if the TBOX: (i) contains all the `rdfs:domain`, `rdfs:range`, `rdfs:subClassOf` or `rdfs:subPropertyOf` axioms in the input stream; and (ii) it contains no quad matching patterns:

  * `X rdfs:subPropertyOf {rdfs:domain|rdfs:range|rdfs:subPropertyOf|rdfs:subClassOf}`
  * `X {rdf:type|rdfs:domain|rdfs:range|rdfs:subClassOf} rdfs:ContainerMembershipProperty`
  * `X {rdf:type|rdfs:domain|rdfs:range|rdfs:subClassOf} rdfs:Datatype`

#### <a class="anchor" id="smush"></a> @smush

    @smush NAMESPACE...

Performs smushing, i.e., identifies `owl:sameAs` equivalence classes and, for each of them, selects a URI as the 'canonical URI' for the class which replaces other alias URIs in input quads.

Arguments `NAMESPACE...` provides the ranked list of namespace URIs or prefixes (resolved based on [`prefix.cc`](http://prefix.cc/) used to select the canonical URI.
At least a namespace must be supplied.

Aliases are not discarded but are emitted using `owl:sameAs` quads that link them to canonical URIs.

#### <a class="anchor" id="unique"></a> @unique

    @unique|@u [-m]

Discards duplicates in the input stream, using external sorting.

Option `-m` causes quads with the same `s,p,o` components but different graphs to be merged in a new graph that represents the 'fusion' of the source graphs (if more than one, otherwise the unique source graph is reused).
The fusion graph is described with (i.e., it is the subject of) all the quads that describe the associated source graphs.


### <a class="anchor" id="otherprocs"></a> Other processors

The remaining processors `@stats`, `@tbox` serve various extraction tasks, while `@prefix` can be used to improve the serialization of produced RDF.

#### <a class="anchor" id="stats"></a> @stats

    @stats [-n NAMESPACE] [-p URI] [-c URI] [-t NUM] [-o]

Emits VOID structural statistics for input quads.
A VOID dataset is associated to the whole input and to each set of graphs associated to the same 'source' URI with a configurable property in a configurable graph.
Class and property partitions are then generated for each of these datasets.
In addition to standard VOID terms, the processor emits additional quads based on an extension vocabulary to express the number of TBox, ABox, `rdf:type` and `owl:sameAs` quads, the average number of properties per entity and informative labels and examples for each TBox term, which are then viewable in tools such as Protégé.

Option `-n NAMESPACE` specifies the namespace where to put generated URIs for VOID dataset (default `stats:`).

Option `-p URI` specifies the property linking graphs to their source URI.
If not supplied, graph-source link will not be read.

Option `-c URI` specifies the graph where to look for graph-source links.
If not supplied, links will be gathered from any graph.

Option `-t NUM` causes the emission of VOID statistics only for concepts having at least `NUM` instances or quads.
This option can be used to reduce the amount of data generated by `@stats`, especially if the output must be visualized in tools such as Protégé.

Option `-o` enables the computation of `void:classes` and `void:properties`, which is memory-intensive (computation may fail if thousands or more of distinct properties are used in the data).

Internally, `@stats` makes use of the `sort` utility to (conceptually) sort the quad stream twice: first based on the subject to group quads about the same entity and compute entity-based and distinct subjects statistics; then based on the object to compute distinct objects statistics.
Therefore, computing VOID statistics is a quite slow operation.

#### <a class="anchor" id="tbox"></a> @tbox

    @tbox

Filters the input stream by emitting only quads belonging to RDFS or OWL TBox axioms (no check is done on the graph component).
Note that OWL TBox axioms are not used for inference by RDFpro (only RDFS is supported).

#### <a class="anchor" id="prefix"></a> @prefix

    @prefix|@p [-f FILE]

Adds prefix-to-namespace bindings to data in the input stream so to enable writing more compact and readable files.

Option `-f FILE` specifies the file providing available bindings, consisting of multiple `namespace prefix1 prefix2 ...` lines each associating a namespace URI to one or more prefixes.
If not specified, the default is to use bindings from [prefix.cc](http://prefix.cc/).
