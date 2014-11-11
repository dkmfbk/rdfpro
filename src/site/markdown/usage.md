
RDFpro usage
============

 * [RDFpro invocation](#invocation)
 * [Pipeline specification](#pipeline)
 * [I/O processors](#ioprocs)
    * [@read](#read)
    * [@write](#write)
    * [@download](#download)
    * [@upload](#upload)
 * [Transformation processors](#transformprocs)
    * [@filter (deprecated)](#filter)
    * [@transform](#transform)
    * [@infer](#infer)
    * [@smush](#smush)
    * [@unique](#unique)
 * [Other processors](#otherprocs)
    * [@stats](#stats)
    * [@tbox](#tbox)
    * [@prefix](#prefix)


### <a name="invocation"></a> RDFpro invocation

The command line tool can be invoked using the `rdfpro` script. Its syntax is:

    rdfpro -v | -h | [-V] SPEC

where options -v and -h display respectively the tool version and its online help text, while `SPEC` is the specification of the RDF processing pipeline that is built and executed by the tool; option -V enables the 'verbose' mode where additional debugging information is logged.


### <a name="pipeline"></a> Pipeline specification

The pipeline specification `SPEC` is based on the recursive application of the following rules:

    SPEC ::=
        @p args                            instantiates builtin processor @p with argument list args
        @p1 args1 ... @pN argsN            sequence composition of processors @p1 ... @pN with their args
        { @p1 args1 , ... , @pN argsN }M   parallel composition of processors @p1 ... @pN with their args
                                           and their outputs merged according to merge criterion M

The available merge criteria `M` and their semantics are:

  * `<no letter>` or `a` -- union of `@p1` ... `@pN` outputs with duplicates (very fast);
  * `u` -- union of `@p1` ... `@pN` outputs with duplicate quads removed (same behaviour of processor `@unique`);
  * `U` -- union of `@p1` ... `@pN` outputs with quads having the same `s,p,o` components merged in a single quad, whose graph is the fusion of source graphs (same as `@unique -m`);
  * `i` -- intersection of `@p1` ... `@pN` outputs without duplicate quads;
  * `d` -- difference `@p1` \ (union of `@p2` ... `@pN`) without duplicate quads.

Note that options `u`, `U`, `i` and `d` are implemented using sorting (`sort`) and thus are slower than the default `a`.

As an example, the following command invokes `rdfpro` in verbose mode with a pipeline that reads a Turtle+gzip file `file.ttl.gz`, extract TBox and VOID statistics in parallel and writes their union (`u` flag) to the RDF/XML file `onto.rdf`.

    rdfpro @read file.ttl { @stats , @tbox }u @write onto.rdf


### <a name="ioprocs"></a> I/O processors

The builtin processors `@read`, `@write`, `@download` and `@upload` allow to move data in and out of RDFpro. We describe them below, reporting both long and short name (e.g., `@read` and `@r`) and their arguments.

#### <a name="read"></a> @read

    @read|@r [-b BASE] [-w] FILE...

Reads quads from one or more files, injecting them in the input stream to produce the emitted output stream. Multiple files are read in parallel and multiple threads are used to parse files that use a line-oriented RDF syntax (NTriples, NQuads, TQL).
Option `-b` can be used to specify a base URI for resolving relative URIs in the input files.
Option `-w` causes BNodes in input files to be rewritten on a per-file basis, so that no BNode clash may occur between BNodes in different files or between a parsed BNode and a BNode in the processor input stream.
Shell expansion can be used to list multiple files. For each file, its RDF format and compression scheme are detected based on the extension (e.g., ttl.gz -> gzipped Turtle). This information must be explicitly provided in case the extension is not informative, by prepending the correct extension as `prefix:` to the file name (e.g., by transforming `my_unknown_file` to `ttl.gz:my_unknown_file`).
The following RDF formats are detected and supported: `rdf`, `rj`, `jsonld`, `nt`, `nq`, `trix`, `trig`, `tql`, `ttl`, `n3`, `brf`, `geonames`. The following compression schemes are detected and supported (provided the corresponding native compression/decompression utility is available): `gz`, `bz2`, `xz`, `7z`.

#### <a name="write"></a> @write

    @write|@w FILE...

Writes quads from the input stream to one or more files.
If multiple files are specified, quads are allocated to them according to a round-robin strategy that produces files of similar sizes; this behaviour can be used to split a large dataset into smaller and more manageable files.
RDF formats and compression schemes are detected from the file extensions, or (similarly to `@read`) can be explicitly supplied using the notation `prefix:filename` (e.g., `ttl.gz:filename`).
The output stream of this processor is the input stream unchanged, thus allowing to chain `@write` with other downstream processors.

#### <a name="download"></a> @download

    @download|@d [-q QUERY] [-f FILE] [-w] URL

Downloads quads from the SPARQL endpoint at `URL`, injecting them in the input stream to produce the emitted output stream. The processor makes use of a CONSTRUCT or SELECT SPARQL query to download data. SELECT queries produce quads based on the bindings of variables s, p, o, c. CONSTRUCT queries return quads in the default graph (i.e., plain triples).
The query is either supplied inline via option `-q` or read from a file using option `-f`. Similarly to `@read`, option `-w` causes BNodes in downloaded data to be rewritten so to avoid clashes with BNodes of the input stream.

#### <a name="upload"></a> @upload

    @upload|@l [-s SIZE] URL

Uploads quads in the input stream to the SPARQL endpoint at `URL`, using one or more SPARQL Update INSERT DATA operations with chunks of `SIZE` quads at most (default 1024 quads).
The output stream of this processor is the input stream unchanged, thus allowing to chain `@upload` with other downstream processors.


### <a name="transformprocs"></a> Data transformation processors

Processors `@filter` (deprecated), `@transform`, `@infer`, `@smush`, `@unique` implement different forms of data transformation.

#### <a name="filter"></a> @filter

*Deprecated. Use @transform instead*

    @filter|@f [MATCH_EXP] [-r REPLACE_EXP] [-k]

Discards and/or replaces quads in the input stream.
A match expression `MATCH_EXP` is used to select quads in the input stream (if omitted, all quads are selected).
Selected quads are optionally rewritten based on a replacement expression `REPLACE_EXP`, while unselected quads are normally discarded unless option `-k` (keep) is specified.

*Match expressions*. `MATCH_EXP` is a sequence of `TARGET` `RULE`... blocks. `TARGET` is a string with:
  * zero or more letters `s,p,o,c,t` (`t` = class in `rdf:type` quad) that select quad components (if no letter is specified, all component are selected);
  * one or more letters `u,b,l,@,^` that select URIs, BNodes, literals, literal languages and literal datatypes of a component.
`RULE` is either `+EXP` or `-EXP`, where expression `EXP` is either `'constant'`, `"constant"`, `<uri>`, `|regex|` or `prefix:name`.
Regular expressions follow [Java syntax](http://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html) and may contain capturing groups `(...)` that are later reused in the replace expression; these groups are numbered starting from 1 and considering the whole `MATCH_EXP`.
Evaluation of the match expression on an input quad considers each component in turn. For each component rules are selected (based on `TARGET`) and are evaluated in order. If a `+EXP` rule matches the evaluation moves to the next
component. A quad is accepted if no `-EXP` rule matches.

*Replace expressions*. `REPLACE_EXP` is a sequence of `TARGET` `EXP` blocks. `TARGET` is defined as for match expressions. `EXP` is either `'constant'`, `"constant"`, `<uri>`, `|pattern|` or `prefix:name`.
Patterns may contain `\i` references to matched groups, using the numbering criteria described above for `MATCH_EXP`.
Evaluation of a replace expression on a selected quad considerd each component in turn. For each component the first applicable `EXP` is selected (based on TARGET), if exists, in which case it is evaluated and the produced value used to replace the currently considered quad component.

*Examples*. We report some concrete examples of how to use the filtering mechanism:

  * `rdfpro @filter 'pu +rdf:type -* sobl -* ou -owl:Thing'` -- keeps only `rdf:type` quads whose subject and object are URIs (`sobl -*`) and with the object different from `owl:Thing`
  * `rdfpro @filter 'pu +rdf:type -*' -r 'cu <http://example.org/mygraph>'` -- extracts `rdf:type` quads, placing them in named graph `<http://example.org/mygraph>`
  * `rdfpro @filter 'pu +foaf:name -* ol +|(.*)|' -r 'ol |Mr. \1|' -k` -- rewrites `foaf:name` quads, prepending `Mr. ` to their literal object (note the use of `-k` to propagate unselected quads)

#### <a name="transform"></a> @transform

    @transform|@t [-p] SCRIPT [ARG...]

Transform the stream using a user-supplied Groovy `SCRIPT`. Extra arguments `ARG...` are passed to the script. Option `-p` enables script pooling, i.e., the execution of multiple script instances in parallel each one on a subset of the input stream; pooling is faster but makes meaningless the use of global variables in the script (there is no such thing as a global state).

The script is executed for each quad (preserving script variables among invocations) passing it the following modifiable variable (note: changing a variable implies a modification of the current quad):

  * `q` - the current quad being transformed
  * `s`, `p`, `o`, `c` the quad subject, predicate, object and graph (context)
  * `t` the type of an `rdf:type` quad (otherwise undefined)
  * `l` and `d` the literal label and datatype (undefined if object is not a literal)

In alternative, the script may provide a function `handle(quad)` that is called for each quad in place of executing the whole script. Independently of the use of `handle(quad)`, the following three callback functions can be defined by the script:

  * `init(args)` - called when the script is initialized with the `ARG...` arguments supplied to the @transform processor
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


#### <a name="infer"></a> @infer

    @infer|@i [-r RULES] [-d] [-C | -c URI] [-b BASE] [-w] [FILE...]

Emits the RDFS deductive closure of input quads. Inference is done on the TBox first.
The TBox is read from one or more filesm using options `-b` and `-w` to handle base URI and BNodes (as in @read).
The TBox closure is emitted in the default graph if option `-C` is specified, or to a specific named graph is option `-c` is given; if none of `-C` or `-c` is given, the TBox closure is not emitted.
Next, the domain, range, sub-class and sub-property axioms are used to do inference on input quads one at a time, placing inferences in the same graph of the input quad.
The set of RDFS inference rules to be used can be optionally specified with option `-r`, which accepts a comma-separated list of rule names (available rules are `rdfd2`, `rdfs1`, `rdfs2`, `rdfs3`, `rdfs4a`, `rdfs4b`, `rdfs5`, `rdfs6`, `rdfs7`, `rdfs8`, `rdfs9`, `rdfs10`, `rdfs11`, `rdfs12`, `rdfs13`)
This scheme avoids expensive join operations and works with arbitrarily large datasets, provided that their TBox fits into memory. The result represents the complete RDFS closure if the TBOX: (i) contains all the `rdfs:domain`, `rdfs:range`, `rdfs:subClassOf` or `rdfs:subPropertyOf` axioms in the input stream; and (ii) it contains no quad matching patterns:

  * `X rdfs:subPropertyOf {rdfs:domain|rdfs:range|rdfs:subPropertyOf|rdfs:subClassOf}`
  * `X {rdf:type|rdfs:domain|rdfs:range|rdfs:subClassOf} rdfs:ContainerMembershipProperty`
  * `X {rdf:type|rdfs:domain|rdfs:range|rdfs:subClassOf} rdfs:Datatype`

Tip: to avoid the derivation of uninformative `X rdf:type rdfs:Resource` triples, avoid using rules `rdfs4a`, `rdfs4b` and `rdfs8`, i.e., specify `-r rdfs1,rdfs2,rdfs3,rdfs5,rdfs6,rdfs7,rdfs9,rdfs10,rdfs11,rdfs12,rdfs13` to enable the remaining rules.

#### <a name="smush"></a> @smush

    @smush|@s [-s SIZE] NAMESPACE...

Performs smushing, i.e., identifies `owl:sameAs` equivalence classes and, for each of them, selects a URI as the 'canonical URI' for the class which replaces other alias URIs in input quads. Aliases are not discarded but are emitted using `owl:sameAs` quads that link them to canonical URIs.
Operatively, smushing makes use of a big hash table with open addressing scheme and thus a fixed size (for performance reasons). The default size in bytes of that table is 128MB but can be changed with option `-s`. You should increase it if necessary; as a rough criterion, consider multiplying the expected number of aliased URIs by 16 (4 bytes per entry, 0.25 load factor).

#### <a name="unique"></a> @unique

    @unique|@u [-m]

Discards duplicates in the input stream. Without option `-m` (merge) the processor just discards duplicate quads.
Otherwise, it merges all quads with the same `s,p,o` components, producing a unique quad that is placed in a graph that represents the 'fusion' of all the source graphs (if more than one, otherwise the unique source graph is reused).
The fusion graph is described with (i.e., it is the subject of) all the quads that describe the associated source graphs.


### <a name="otherprocs"></a> Other processors

The remaining processors `@stats`, `@tbox` serve various extraction tasks, while `@prefix` can be used to improve the serialization of produced RDF.

#### <a name="stats"></a> @stats

    @stats|@x [-n NAMESPACE] [-p URI] [-c URI]

Emits VOID structural statistics for input quads. A VOID dataset is associated to the whole input and to each set of graphs associated to the same 'source' URI with a configurable property, specified with option `-p`; if option `-c` is given, these association quads are searched only in the graph with the URI specified.
Class and property partitions are then generated for each of these datasets. In addition to standard VOID terms, the processor emits additional quads based on an extension vocabulary to express the number of TBox, ABox, `rdf:type` and `owl:sameAs` quads, the average number of properties per entity and informative labels and examples for each TBox term, which are then viewable in tools such as Protégé.
The URIs of VOID datasets are automatically generated and placed in the namespace specified with option `-n` (a default namespace is used if `-n` is not supplied).
Internally, `@stats` makes use of the `sort` utility to (conceptually) sort the quad stream twice: first based on the subject to group quads about the same entity and compute entity-based and distinct subjects statistics; then based on the object to compute distinct objects statistics. Therefore, computing VOID statistics is quite a slow operation.

#### <a name="tbox"></a> @tbox

    @tbox|@t

Filters the input stream by emitting only quads belonging to RDFS or OWL TBox axioms (no check is done on the graph component). Note that OWL TBox axioms are not used for inference by RDFpro (only RDFS is supported).

#### <a name="prefix"></a> @prefix

    @prefix|@p [-f FILE]

Adds missing prefix-to-namespace bindings for namespaces in [prefix.cc](http://prefix.cc/) or in the file specified by option `-f`, so to enable writing more compact and readable files.
If a file is used, it consists of multiple `namespace prefix1 prefix2 ...` lines that associate a namespace URI to one or more prefixes.
