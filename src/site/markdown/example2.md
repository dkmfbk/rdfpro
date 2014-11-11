
RDFpro usage examples
=====================

In this example, we show how to use RDFpro to select and integrate RDF data from Freebase, GeoNames and DBpedia in the four languaged EN, ES, IT and NL, performing smushing, inference, deduplication and statistics extraction.


## Scenario 1. Dataset Analysis

Dataset analysis comprises all the tasks aimed at providing a qualitative and quantitative characterization of the contents of an RDF dataset, such as the extraction of the data TBox or of instance-level ABox data statistics (e.g., [VOID](http://www.w3.org/TR/void/)). When processing RDF, dataset analysis can be applied both to input and output data. In the first case, it helps identifying relevant data and required pre-processing tasks, especially when the dataset scope is broad (as occurs with many LOD datasets) or its documentation is poor. In the second case, it provides a characterization of output data that is useful for validation and documentation purposes.

We describe here how RDFpro can be used for large-scale dataset analysis. We consider the tasks of extracting TBox and VOID statistics from [Freebase](https://developers.google.com/freebase/data) data, whose schema and statistics are not available online, and the task of comparing this Freebase release with a previous release in order to identify newly added triples (we omit the further analysis of this delta, as it can use the same TBox and statistics extraction techniques applied here to the whole Freebase).

### Processing

The figures below show how to use RDFpro to extract TBox and VOID statistics (left figure) and to identify newly added triples (right figure) in the considered scenario:

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="example/analysis.png" alt="Dataset analysis processing steps"/>
</div>

We use the `@tbox` and `@stats` processors to extract TBox and VOID statistics. The two processors can be used separately by invoking RDFpro twice as follows (note the use of the `-t 100` option: it require to emit statistics of classes and properties with at least 100 instances, which is necessary in order to load the generated statistics in tools such as Protégé - see next):

    rdfpro @read freebase.nt.gz @tbox @write tbox.tql.gz

    rdfpro @read freebase.nt.gz @stats -t 100 @write stats.tql.gz

The two processors can also be composed in a single pipeline where Freebase data is read once and fed to both processors in parallel, as in the figure above. The corresponding RDFpro command is:

    rdfpro @read freebase.nt.gz { @tbox @write tbox.tql.gz , @stats -t 100 @write stats.tql.gz }

Extraction of newly added triples can be done exploiting the parallel composition with the difference merge criterion (flag `d`) to combine quads, using the following command:

    rdfpro { @read freebase.nt.gz , @read ../freebase/freebase_old.nt.gz }d @write new-triples.tql.gz

### Results

The table below reports the tasks execution times, throughputs, input and output sizes (quads and gzipped bytes) we measured on our test machine. Additionally, when running the comparison task we measured a disk usage of 92.8 GB for the temporary files produced by the sorting-based difference merge criterion (∼18 bytes per input triple).

<table>
<thead>
<tr>
<th rowspan="2">Task</th>
<th colspan="2">Input size</th>
<th colspan="2">Output size</th>
<th colspan="2">Throughput</th>
<th rowspan="2">Time<br/>[s]</th>
</tr>
<tr>
<th>[Mquads]</th>
<th>[MB]</th>
<th>[Mquads]</th>
<th>[MB]</th>
<th>[Mquads/s]</th>
<th>[MB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>1. TBox extraction</td>
<td>2863</td>
<td>28339</td>
<td>0.23</td>
<td>3.01</td>
<td>1.43</td>
<td>14.12</td>
<td>2006</td>
</tr>
<tr>
<td>2. Statistics extraction</td>
<td>2863</td>
<td>28339</td>
<td>0.13</td>
<td>1.36</td>
<td>0.34</td>
<td>3.36</td>
<td>8443</td>
</tr>
<tr>
<td>1-2 Aggregated</td>
<td>2863</td>
<td>28339</td>
<td>0.36</td>
<td>4.35</td>
<td>0.34</td>
<td>3.36</td>
<td>8426</td>
</tr>
<tr>
<td>3. Comparison</td>
<td>5486</td>
<td>55093</td>
<td>260</td>
<td>1894</td>
<td>0.42</td>
<td>4.25</td>
<td>12955</td>
</tr>
</tbody>
</table>

Comparing the two Freebase releases resulted the most expensive task due to sorting and involved input size. When performed jointly, TBox and statistics extraction present performance figures close to statistics extraction alone, as data parsing is performed once and the cost of TBox extraction (excluded parsing) is negligible. This is an example of how the aggregation of multiple processing tasks in a single computation, enabled by RDFpro streaming model and composition facilities, can generally lead to better performances due to a reduction of I/O overhead.

To provide an idea of the how analysis results can be used to explore the dataset, the figure below shows the joint browsing of extracted TBox and statistics in Protégé, exploiting the specific concept annotations emitted by `@stats`. The class and property hierarchies are augmented with the number of entities and property triples (marked as 1 in the figure), as well as with the detected property usage (2), e.g., `O` for object property, `I` for inverse functional; each concept is annotated with an example instance and a VOID partition individual (3), which provides numeric statistics about the concept (4).

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="example/protege.png" alt="Browsing extracted TBox and VOID statistics with Protégé"/>
</div>


## Scenario 2. Dataset Filtering

When dealing with large RDF datasets, dataset filtering (or slicing) is often required to extract a small subset of interesting data, identified, e.g., based on a previous dataset analysis. Dataset filtering typically consists in (i) identifying the entities of interest in the dataset, based on selection conditions on their URIs, `rdf:type` or other properties; and (ii) extracting all the quads about these entities expressing selected RDF properties. These two operations can be implemented using multiple streaming passes in RDFpro.

We consider here a concrete scenario where the dataset is [Freebase](https://developers.google.com/freebase/data), the entities of interest are musical group (i.e., their `rdf:type` is `fb:music.musical` group) that are still active (i.e., there is no associated property `fb:music.artist.active_end`), and the properties to extract are the group name, genre and place of origin (respectively, `rdfs:label`, `fb:music.artist.genre` and `fb:music.artist.origin`).


### Processing

We implement the task with two invocations of RDFpro as shown in the figure below.

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="example/filtering.png" alt="Dataset filtering processing steps"/>
</div>

The first invocation (marked as 1 in the figure) generates an RDF file listing as subjects the URIs of the entities of interest. This is done with two parallel `@transform` processors, extracting respectively musical groups and no more active musical entities, whose outputs are combined with the difference merge criterion using the following RDFpro command:

    rdfpro @read freebase.nt.gz \
           { @transform -p "emitIf(t == fb:music.musical_group)" , \
             @transform -p "if(p == fb:music.artist.active_end) emit(s, rdf:type, fb:music.musical_group, null)" }d \
           @write entities.tql.gz

The second invocation (marked as 2 in the figure) uses another `@transform` processor to extract desired quads, testing predicates and requiring subjects to be contained in the previously extracted file (whose URIs are indexed in memory by a specific function in the `@transform` expression). The corresponding RDFpro command is:

    rdfpro @read freebase.nt.gz \
           @transform "def init(args) { instances = loadSet('./instances.tql', 's'); };
                       emitIf((p == rdfs:label || p == fb:music.artist.genre || p == fb:music.artist.origin)
                              && instances.match(s) );" \
           @write output.tql.gz


### Results

The table below reports the execution times, throughputs, input and output sizes of the two invocations of RDFpro on the test machine.

<table>
<thead>
<tr>
<th rowspan="2">Task</th>
<th colspan="2">Input size</th>
<th colspan="2">Output size</th>
<th colspan="2">Throughput</th>
<th rowspan="2">Time<br/>[s]</th>
</tr>
<tr>
<th>[Mquads]</th>
<th>[MB]</th>
<th>[Mquads]</th>
<th>[MB]</th>
<th>[Mquads/s]</th>
<th>[MB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>1. Select entities</td>
<td>2863</td>
<td>28339</td>
<td>0.20</td>
<td>0.73</td>
<td>1.36</td>
<td>13.4</td>
<td>2111</td>
</tr>
<tr>
<td>2. Extract quads</td>
<td>2863</td>
<td>28339</td>
<td>0.42</td>
<td>5.17</td>
<td>1.15</td>
<td>11.4</td>
<td>2481</td>
</tr>
</tbody>
</table>

Although simple, the example shows how practical, large-scale filtering tasks are feasible with RDFpro. Streaming allows processing large amounts of data, while sorting enables the use of the intersection, union and difference merge criteria to implement, respectively, the conjunction, disjunction and negation of entity selection conditions.

More complex scenarios may be addressed with additional invocations that progressively augment the result (e.g., a third invocation can identify albums of selected artists, while a fourth invocation can extract the quads describing them). In cases where RDFpro model is insufficient or impractical (e.g., due to the need for recursive extraction of related entities, aggregation or join conditions), the tool can still be used to perform a first coarse-grained filtering that reduces the number of quads and eases their downstream processing.


## Scenario 3. Dataset Merging

A common usage scenario is dataset merging, where multiple RDF datasets are integrated and prepared for application consumption. Data preparation typically comprises smushing, inference materialization and data deduplication
(possibly with provenance tracking). These tasks make the use of the resulting dataset more easy and efficient, as reasoning and entity aliasing have been already accounted for.

We consider here a concrete dataset merging scenario with data from the following sources (total ~3394 MQ):

  * the Freebase complete [dataset](http://download.freebaseapps.com/) dataset, containing ~2863 MQ as of October 2014

  * the GeoNames [data file](http://download.geonames.org/all-geonames-rdf.zip), [ontology](http://www.geonames.org/ontology/ontology_v3.1.rdf) and [schema mappings](http://www.geonames.org/ontology/mappings_v3.01.rdf) w.r.t. DBpedia, for a total of ~125 MQ

  * the following DBpedia 3.9 files for the four EN, ES, IT, NL DBpedia chapters, for a total of 406 MQ:
    - DBpedia [ontology](http://downloads.dbpedia.org/3.9/dbpedia_3.9.owl.bz2)
    - article categories, [EN](http://downloads.dbpedia.org/3.9/en/article_categories_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/article_categories_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/article_categories_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/article_categories_en_uris_nl.ttl.bz2)
    - category labels, [EN](http://downloads.dbpedia.org/3.9/en/category_labels_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/category_labels_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/category_labels_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/category_labels_nl.ttl.bz2)
    - external links, [EN](http://downloads.dbpedia.org/3.9/en/external_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/external_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/external_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/external_links_nl.ttl.bz2)
    - geographic coordinates, [EN](http://downloads.dbpedia.org/3.9/en/geo_coordinates_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/geo_coordinates_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/geo_coordinates_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/geo_coordinates_nl.ttl.bz2)
    - homepages, [EN](http://downloads.dbpedia.org/3.9/en/homepages_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/homepages_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/homepages_it.ttl.bz2)
    - image links, [EN](http://downloads.dbpedia.org/3.9/en/images_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/images_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/images_it.ttl.bz2)
    - instance types, [EN](http://downloads.dbpedia.org/3.9/en/instance_types_en.ttl.bz2), [EN (heuristics)](http://downloads.dbpedia.org/3.9/en/instance_types_heuristic_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/instance_types_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/instance_types_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/instance_types_nl.ttl.bz2)
    - labels, [EN](http://downloads.dbpedia.org/3.9/en/labels_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/labels_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/labels_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/labels_nl.ttl.bz2)
    - mapping-based properties, [EN (cleaned)](http://downloads.dbpedia.org/3.9/en/mappingbased_properties_cleaned_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/mappingbased_properties_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/mappingbased_properties_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/mappingbased_properties_nl.ttl.bz2)
    - short abstracts, [EN](http://downloads.dbpedia.org/3.9/en/short_abstracts_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/short_abstracts_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/short_abstracts_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/short_abstracts_nl.ttl.bz2)
    - SKOS categories, [EN](http://downloads.dbpedia.org/3.9/en/skos_categories_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/skos_categories_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/skos_categories_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/skos_categories_nl.ttl.bz2)
    - Wikipedia links, [EN](http://downloads.dbpedia.org/3.9/en/wikipedia_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/wikipedia_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/wikipedia_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/wikipedia_links_nl.ttl.bz2)
    - interlanguage links, [EN](http://downloads.dbpedia.org/3.9/en/interlanguage_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/interlanguage_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/interlanguage_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/interlanguage_links_nl.ttl.bz2)
    - DBpedia IRI - URI `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/iri_same_as_uri_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/iri_same_as_uri_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/iri_same_as_uri_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/iri_same_as_uri_nl.ttl.bz2)
    - DBpedia - Freebase `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/freebase_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/freebase_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/freebase_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/freebase_links_nl.ttl.bz2)
    - DBpedia - GeoNames `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/geonames_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/geonames_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/geonames_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/geonames_links_nl.ttl.bz2)
    - person data, [EN](http://downloads.dbpedia.org/3.9/en/persondata_en.ttl.bz2)
    - PND codes, [EN](http://downloads.dbpedia.org/3.9/en/pnd_en.ttl.bz2)

  * vocabulary definition files for [FOAF](http://xmlns.com/foaf/0.1/), [SKOS](http://www.w3.org/2004/02/skos/core#), [DCTERMS](http://purl.org/dc/terms/), [WGS84](http://www.w3.org/2003/01/geo/wgs84_pos#), [GEORSS](http://www.w3.org/2005/Incubator/geo/XGR-geo/W3C_XGR_Geo_files/geo_2007.owl)

Basically, we import all Freebase and GeoNames data as they only provide global dump files, while we import selected DBpedia dump files leaving out files for Wikipedia inter-page, redirect and disambiguation links and page and revision IDs/URIs. Bash script [download.sh](example/download.sh) can be used to automatize the download of all the selected dump files, placing them in multiple directories `vocab` (for vocabularies), `freebase`, `geonames`, `dbp_en` (DBpedia EN), `dbp_es` (DBpedia ES), `dbp_it` (DBpedia IT) and `dbp_nl` (DBpedia NL).


### Processing

The figure below shows the required processing steps.

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="example/merging.png" alt="Dataset merging processing steps"/>
</div>

A preliminary processing phase (marked as 1 in the figure) is required to transform input data and extract the TBox axioms required for inference. Data transformation serves (i) to track provenance, by placing quads in different named graphs based on the source dataset; and (ii) to adopt optimal serialization format (Turtle Quads) and compression scheme (gzip) that speed up further processing. The corresponding RDFpro command is.

    rdfpro { @read -w metadata.trig , \
             @read -w vocab/* @filter -r "cu '<graph:vocab>'" , \
             @read -w freebase/* @filter "spou -|http://rdf\.freebase\.com/key/.*|
                 -|http://rdf\.freebase\.com/ns/common\.(notable_for\|document).*|
                 -fb:common.topic -fb:common.topic.article -fb:common.topic.notable_for
                 -fb:common.topic.notable_types -fb:common.topic.topic_equivalent_webpage
                 -|http://rdf\.freebase\.com/ns/(type\|user\|base\|freebase\|dataworld\|pipeline\|atom\|community)\..*|" \
                 -r "cu '<graph:freebase>'" , \
             @read -w geonames/*.rdf geonames:geonames/all-geonames-rdf.zip \
                 @filter "pu -gn:childrenFeatures -gn:locationMap
                     -gn:nearbyFeatures -gn:neighbouringFeatures -gn:countryCode
                     -gn:parentFeature -gn:wikipediaArticle -rdfs:isDefinedBy
                     -rdf:type" -r "cu '<graph:geonames>'" , \
             { @read -w dbp_en/* @filter -r "cu '<graph:dbp_en>'" , \
               @read -w dbp_es/* @filter -r "cu '<graph:dbp_es>'" , \
               @read -w dbp_it/* @filter -r "cu '<graph:dbp_it>'" , \
               @read -w dbp_nl/* @filter -r "cu '<graph:dbp_nl>'" } \
                 @filter "ou -bibo pu -dc:rights -dc:language -foaf:primaryTopic" } \
           @filter "ol -'' o@ +'en' +'es' +'it' +'nl' -* o^ +xsd -*" \
           @write filtered.tql.gz

The reported command shows how a parallel and parallel and sequence composition of `@read` and `@filter` can be used to process in a single step a number of RDF files, applying filtering both to separate file groups and globally.
Some notes on the implemented filtering rules:

  * Freebase filtering aims at removing redundant `http://rdf.freebase.com/key/` triples and triples belonging to Freebase specific domains (e.g., users, user KBs, schemas, ...). We also remove triples of limited informative value.
  * GeoNames filtering aims at removing triples that are uninformative (`rdfs:isDefinedBy`, `rdf:type`), redundant (`gn:countryCode`, `gn:parentFeature`, `gn:wikipediaArticle`, the latter providing the same information of links to DBpedia) or that points to auto-generated resources for which there is no data in the dump (`gn:childrenFeatures`, `gn:locationMap`, `gn:nearbyFeatures`, `gn:neighbouringFeatures`).
  * DBpedia filtering aims at removing triples that have limited informative value (`dc:rights`, `dc:language` for images and Wikipedia pages) or that are redundant (`foaf:primaryTopic` and all triples in the [`bibo`](http://purl.org/ontology/bibo/) namespace).
  * the global filtering (`"ol -'' o@ +'en' +'es' +'it' +'nl' -* o^ +xsd -*"`) aims at removing literals with a language different from `en`, `es`, `it` and `nl`, literals that are empty and literals with a datatype not in the XML Schema vocabulary.
  * the `-r "cu '<GRAPH_URI>'"` options serve to place filtered triples in different graphs, so that we can keep track of which source they come from

The second task of the preprocessing phase, TBox extraction, is performed with the following command:

    rdfpro @read filtered.tql.gz \
           @tbox \
           @filter "spou -bibo -con -owl:Thing -schema:Thing -foaf:Document -dc:subject -foaf:page -dct:relation" \
           @write tbox.tql.gz


The main processing phase (marked as 2) consists in the cascaded execution of smushing, RDFS inference and deduplication to produce the merged dataset. Smushing identifies `owl:sameAs` equivalence classes and assigns a canonical URI to each of them. RDFS inference excludes rules `rdfs4a`, `rdfs4b` and `rdfs8` to avoid materializing uninformative `<X rdf:type rdfs:Resource>` quads. Deduplication takes quads with the same subject, predicate and object (possibly produced by previous steps) and merges them in a single quad inside a graph linked to all the original sources.










TBox quads are extracted from filtered data and stored, filtering out unwanted top level classes (`owl:Thing`, `schema:Thing`, `foaf:Document`) and vocabulary alignments (to [`bibo`](http://purl.org/ontology/bibo/) and [`con`](http://www.w3.org/2000/10/swap/pim/contact#) terms and `dc:subject`).

##### Step 3 Smushing

    rdfpro @read filtered.tql.gz \
           @smush -S 2048M http://dbpedia http://it.dbpedia http://es.dbpedia \
               http://nl.dbpedia http://rdf.freebase.com http://sws.geonames.org \
           @write smushed.tql.gz

Filtered data is smushed so to use canonical URIs for each `owl:sameAs` equivalence class, producing an intermediate smushed file. Note the use of a smush buffer of 2GB and the specification of a ranked list of namespaces for selecting the canonical URIs.

##### Step 4 Inference

    rdfpro @read smushed.tql.gz \
           @infer -c '<graph:vocab>' -r rdfs1,rdfs2,rdfs3,rdfs5,rdfs6,rdfs7,rdfs9,rdfs10,rdfs11,rdfs12,rdfs13 -d tbox.tql.gz \
           @filter "tu -bibo -con -owl:Thing -schema:Thing -foaf:Document pu -bibo -con -dc:subject -foaf:page -dct:relation" \
           @write inferred.tql.gz

The deductive closure of smushed data is computed and saved, using the extracted TBox and excluding RDFS rules `rdfs4a`, `rdfs4b` and `rdfs8` (and keeping the remaining ones) to avoid inferring uninformative `X rdf:type rdfs:Resource` quads.
The closed TBox is placed in graph `<graph:vocab>`. A further filtering is done to be sure that no unwanted triple is present in the result dataset due to inference.

##### Step 5 Merging

    rdfpro @read inferred.tql.gz \
           @unique -m \
           @write dataset.tql.gz

Quads with the same subject, predicate and object are merged and placed in a graph linked to the original sources to track provenance (note the use of the `-m` option).













### Results

The table below reports the execution times, throughputs and input and output sizes of each step, covering both the cases where steps are performed separately via intermediate files and multiple invocations of RDFpro (upper part of the table), or aggregated per processing phase using composition capabilities (lower part). Also in this scenario, the aggregation of multiple processing tasks leads to a marked reduction of the total processing time (33% reduction from 47803 s to 31981 s) due to the elimination of the I/O overhead for intermediate files. RDFpro also reported the use of ∼2 GB of memory for smushing an `owl:sameAs` graph of ∼38M URIs and ∼8M equivalence classes (∼56 bytes/URI).

<table>
<thead>
<tr>
<th rowspan="2">Step</th>
<th colspan="2">Input size</th>
<th colspan="2">Output size</th>
<th colspan="2">Throughput</th>
<th rowspan="2">Time<br/>[s]</th>
</tr>
<tr>
<th>[Mquads]</th>
<th>[MB]</th>
<th>[Mquads]</th>
<th>[MB]</th>
<th>[Mquads/s]</th>
<th>[MB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>1. Transform</td>
<td>3394</td>
<td>33524</td>
<td>3394</td>
<td>36903</td>
<td>0.42</td>
<td>4.12</td>
<td>8137</td>
</tr>
<tr>
<td>2. TBox extraction</td>
<td>3394</td>
<td>36903</td>
<td>&lt;1</td>
<td>4</td>
<td>1.28</td>
<td>13.9</td>
<td>2656</td>
</tr>
<tr>
<td>3. Smushing</td>
<td>3394</td>
<td>36903</td>
<td>3424</td>
<td>38823</td>
<td>0.37</td>
<td>3.98</td>
<td>9265</td>
</tr>
<tr>
<td>4. Inference</td>
<td>3424</td>
<td>38823</td>
<td>5615</td>
<td>51927</td>
<td>0.32</td>
<td>3.66</td>
<td>10612</td>
</tr>
<tr>
<td>5. Deduplication</td>
<td>5615</td>
<td>51927</td>
<td>4085</td>
<td>31297</td>
<td>0.33</td>
<td>3.03</td>
<td>17133</td>
</tr>
<tr>
<td>1-2 Aggregated</td>
<td>3394</td>
<td>33524</td>
<td>3394</td>
<td>36903</td>
<td>0.41</td>
<td>4.06</td>
<td>8247</td>
</tr>
<tr>
<td>3-5 Aggregated</td>
<td>3394</td>
<td>36903</td>
<td>4085</td>
<td>31446</td>
<td>0.14</td>
<td>1.56</td>
<td>23734</td>
</tr>
</tbody>
</table>


## Combining Filtering, Merging and Analyisis

While addressed separately, the three scenarios of dataset analysis, filtering and merging previously presented are often combined in practice, e.g., to remove unwanted ABox and TBox quads from input data, merge remaining quads and analyse the result producing statistics that describe and document it. We report here an example of such combination, considering a typical integration scenario involving the merging of selected RDF data from Freebase, GeoNames and DBpedia in the four languaged EN, ES, IT and NL, performing filtering (of source data), smushing, inference, deduplication and statistics extraction.

w.r.t. merging scenario we leave out raw infobox properties (as lower-quality if compared to mapping-based properties) and extended abstract (as too long).


### Data download and selection

We assume the goal is to gather and integrate data about entities (e.g., persons, location, organizations) from the three Freebase, GeoNames and DBpedia sources.
In a first selection step we choose the following dump files as input for our integration process:

  * the Freebase complete [RDF dataset](http://download.freebaseapps.com/), containing over 2.6 billions triples.

  * the GeoNames [data file](http://download.geonames.org/all-geonames-rdf.zip), [ontology](http://www.geonames.org/ontology/ontology_v3.1.rdf) and [schema mappings](http://www.geonames.org/ontology/mappings_v3.01.rdf) w.r.t. DBpedia

  * the following DBpedia 3.9 files for the four EN, ES, IT, NL DBpedia chapters:
    - DBpedia [ontology](http://downloads.dbpedia.org/3.9/dbpedia_3.9.owl.bz2)
    - article categories, [EN](http://downloads.dbpedia.org/3.9/en/article_categories_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/article_categories_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/article_categories_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/article_categories_en_uris_nl.ttl.bz2)
    - category labels, [EN](http://downloads.dbpedia.org/3.9/en/category_labels_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/category_labels_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/category_labels_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/category_labels_nl.ttl.bz2)
    - external links, [EN](http://downloads.dbpedia.org/3.9/en/external_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/external_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/external_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/external_links_nl.ttl.bz2)
    - geographic coordinates, [EN](http://downloads.dbpedia.org/3.9/en/geo_coordinates_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/geo_coordinates_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/geo_coordinates_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/geo_coordinates_nl.ttl.bz2)
    - homepages, [EN](http://downloads.dbpedia.org/3.9/en/homepages_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/homepages_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/homepages_it.ttl.bz2)
    - image links, [EN](http://downloads.dbpedia.org/3.9/en/images_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/images_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/images_it.ttl.bz2)
    - instance types, [EN](http://downloads.dbpedia.org/3.9/en/instance_types_en.ttl.bz2), [EN (heuristics)](http://downloads.dbpedia.org/3.9/en/instance_types_heuristic_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/instance_types_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/instance_types_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/instance_types_nl.ttl.bz2)
    - labels, [EN](http://downloads.dbpedia.org/3.9/en/labels_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/labels_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/labels_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/labels_nl.ttl.bz2)
    - mapping-based properties, [EN (cleaned)](http://downloads.dbpedia.org/3.9/en/mappingbased_properties_cleaned_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/mappingbased_properties_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/mappingbased_properties_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/mappingbased_properties_nl.ttl.bz2)
    - short abstracts, [EN](http://downloads.dbpedia.org/3.9/en/short_abstracts_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/short_abstracts_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/short_abstracts_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/short_abstracts_nl.ttl.bz2)
    - SKOS categories, [EN](http://downloads.dbpedia.org/3.9/en/skos_categories_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/skos_categories_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/skos_categories_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/skos_categories_nl.ttl.bz2)
    - Wikipedia links, [EN](http://downloads.dbpedia.org/3.9/en/wikipedia_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/wikipedia_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/wikipedia_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/wikipedia_links_nl.ttl.bz2)
    - interlanguage links, [EN](http://downloads.dbpedia.org/3.9/en/interlanguage_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/interlanguage_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/interlanguage_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/interlanguage_links_nl.ttl.bz2)
    - DBpedia IRI - URI `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/iri_same_as_uri_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/iri_same_as_uri_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/iri_same_as_uri_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/iri_same_as_uri_nl.ttl.bz2)
    - DBpedia - Freebase `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/freebase_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/freebase_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/freebase_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/freebase_links_nl.ttl.bz2)
    - DBpedia - GeoNames `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/geonames_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/geonames_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/geonames_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/geonames_links_nl.ttl.bz2)
    - person data, [EN](http://downloads.dbpedia.org/3.9/en/persondata_en.ttl.bz2)
    - PND codes, [EN](http://downloads.dbpedia.org/3.9/en/pnd_en.ttl.bz2)

  * vocabulary definition files for [FOAF](http://xmlns.com/foaf/0.1/), [SKOS](http://www.w3.org/2004/02/skos/core#), [DCTERMS](http://purl.org/dc/terms/), [WGS84](http://www.w3.org/2003/01/geo/wgs84_pos#), [GEORSS](http://www.w3.org/2005/Incubator/geo/XGR-geo/W3C_XGR_Geo_files/geo_2007.owl)

Basically, we import all Freebase and GeoNames data as they only provide global dump files, while we import selected DBpedia dump files leaving out files for Wikipedia inter-page, redirect and disambiguation links and page and revision IDs/URIs (as not relevant in this scenario), raw infobox properties (as lower-quality if compared to mapping-based properties) and extended abstract (as too long).

Bash script [download.sh](example/download.sh) can be used to automatize the download of all the selected dump files, placing them in multiple directories `vocab` (for vocabularies), `freebase`, `geonames`, `dbp_en` (DBpedia EN), `dbp_es` (DBpedia ES), `dbp_it` (DBpedia IT) and `dbp_nl` (DBpedia NL).


### Data processing (single steps)

Processing with RDFpro involves the six steps shown in the figure below.
These steps can be executed individually by invoking `rdfpro` six times, as described next.
Bash script [process_single.sh](example/process_single.sh) can be used to issue these commands.


<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="example/processing.png" alt="Data processing steps"/>
</div>


##### Step 1 Filtering

    rdfpro { @read -w metadata.trig , \
             @read -w vocab/* @filter -r "cu '<graph:vocab>'" , \
             @read -w freebase/* @filter "spou -|http://rdf\.freebase\.com/key/.*|
                 -|http://rdf\.freebase\.com/ns/common\.(notable_for\|document).*|
                 -fb:common.topic -fb:common.topic.article -fb:common.topic.notable_for
                 -fb:common.topic.notable_types -fb:common.topic.topic_equivalent_webpage
                 -|http://rdf\.freebase\.com/ns/(type\|user\|base\|freebase\|dataworld\|pipeline\|atom\|community)\..*|" \
                 -r "cu '<graph:freebase>'" , \
             @read -w geonames/*.rdf geonames:geonames/all-geonames-rdf.zip \
                 @filter "pu -gn:childrenFeatures -gn:locationMap
                     -gn:nearbyFeatures -gn:neighbouringFeatures -gn:countryCode
                     -gn:parentFeature -gn:wikipediaArticle -rdfs:isDefinedBy
                     -rdf:type" -r "cu '<graph:geonames>'" , \
             { @read -w dbp_en/* @filter -r "cu '<graph:dbp_en>'" , \
               @read -w dbp_es/* @filter -r "cu '<graph:dbp_es>'" , \
               @read -w dbp_it/* @filter -r "cu '<graph:dbp_it>'" , \
               @read -w dbp_nl/* @filter -r "cu '<graph:dbp_nl>'" } \
                 @filter "ou -bibo pu -dc:rights -dc:language -foaf:primaryTopic" } \
           @filter "ol -'' o@ +'en' +'es' +'it' +'nl' -* o^ +xsd -*" \
           @write filtered.tql.gz

Downloaded dump files are filtered to extract desired RDF quads and place them in separate graphs to track provenance.
A [metadata file](example/metadata.trig) is added to link each graph to the URI of the associated source (e.g. Freebase).
The command above shows how a parallel and sequence composition of `@read` and `@filter` can be used to process in a single step a number of RDF files, applying filtering both to separate file groups and globally.
Some notes on the implemented filtering rules:

  * Freebase filtering aims at removing redundant `http://rdf.freebase.com/key/` triples and triples belonging to Freebase specific domains (e.g., users, user KBs, schemas, ...). We also remove triples of limited informative value.
  * GeoNames filtering aims at removing triples that are uninformative (`rdfs:isDefinedBy`, `rdf:type`), redundant (`gn:countryCode`, `gn:parentFeature`, `gn:wikipediaArticle`, the latter providing the same information of links to DBpedia) or that points to auto-generated resources for which there is no data in the dump (`gn:childrenFeatures`, `gn:locationMap`, `gn:nearbyFeatures`, `gn:neighbouringFeatures`).
  * DBpedia filtering aims at removing triples that have limited informative value (`dc:rights`, `dc:language` for images and Wikipedia pages) or that are redundant (`foaf:primaryTopic` and all triples in the [`bibo`](http://purl.org/ontology/bibo/) namespace).
  * the global filtering (`"ol -'' o@ +'en' +'es' +'it' +'nl' -* o^ +xsd -*"`) aims at removing literals with a language different from `en`, `es`, `it` and `nl`, literals that are empty and literals with a datatype not in the XML Schema vocabulary.
  * the `-r "cu '<GRAPH_URI>'"` options serve to place filtered triples in different graphs, so that we can keep track of which source they come from

##### Step 2 TBox extraction

    rdfpro @read filtered.tql.gz \
           @tbox \
           @filter "spou -bibo -con -owl:Thing -schema:Thing -foaf:Document -dc:subject -foaf:page -dct:relation" \
           @write tbox.tql.gz

TBox quads are extracted from filtered data and stored, filtering out unwanted top level classes (`owl:Thing`, `schema:Thing`, `foaf:Document`) and vocabulary alignments (to [`bibo`](http://purl.org/ontology/bibo/) and [`con`](http://www.w3.org/2000/10/swap/pim/contact#) terms and `dc:subject`).

##### Step 3 Smushing

    rdfpro @read filtered.tql.gz \
           @smush -S 2048M http://dbpedia http://it.dbpedia http://es.dbpedia \
               http://nl.dbpedia http://rdf.freebase.com http://sws.geonames.org \
           @write smushed.tql.gz

Filtered data is smushed so to use canonical URIs for each `owl:sameAs` equivalence class, producing an intermediate smushed file. Note the use of a smush buffer of 2GB and the specification of a ranked list of namespaces for selecting the canonical URIs.

##### Step 4 Inference

    rdfpro @read smushed.tql.gz \
           @infer -c '<graph:vocab>' -r rdfs1,rdfs2,rdfs3,rdfs5,rdfs6,rdfs7,rdfs9,rdfs10,rdfs11,rdfs12,rdfs13 -d tbox.tql.gz \
           @filter "tu -bibo -con -owl:Thing -schema:Thing -foaf:Document pu -bibo -con -dc:subject -foaf:page -dct:relation" \
           @write inferred.tql.gz

The deductive closure of smushed data is computed and saved, using the extracted TBox and excluding RDFS rules `rdfs4a`, `rdfs4b` and `rdfs8` (and keeping the remaining ones) to avoid inferring uninformative `X rdf:type rdfs:Resource` quads.
The closed TBox is placed in graph `<graph:vocab>`. A further filtering is done to be sure that no unwanted triple is present in the result dataset due to inference.

##### Step 5 Merging

    rdfpro @read inferred.tql.gz \
           @unique -m \
           @write dataset.tql.gz

Quads with the same subject, predicate and object are merged and placed in a graph linked to the original sources to track provenance (note the use of the `-m` option).

##### Step 6 Statistics extraction

    rdfpro { @read tbox.tql.gz , @read dataset.tql.gz @stats } @prefix @write statistics.tql.gz

VOID statistics are extracted and merged with TBox data, forming an annotated ontology that documents the produced dataset.


### Data processing (aggregate steps)

The 6 steps previously listed can be also aggregated to reduce overhead for writing and reading back intermediate files, exploiting RDFpro capability to arbitrarily compose processors and write intermediate results.
In particular, steps 1-2 can be aggregated as follows:

    rdfpro { @read -w metadata.trig , \
             @read -w vocab/* @filter -r "cu '<graph:vocab>'" , \
             @read -w freebase/* @filter "spou -|http://rdf\.freebase\.com/key/.*|
                 -|http://rdf\.freebase\.com/ns/common\.(notable_for\|document).*|
                 -fb:common.topic -fb:common.topic.article -fb:common.topic.notable_for
                 -fb:common.topic.notable_types -fb:common.topic.topic_equivalent_webpage
                 -|http://rdf\.freebase\.com/ns/(type\|user\|base\|freebase\|dataworld\|pipeline\|atom\|community)\..*|" \
                 -r "cu '<graph:freebase>'" , \
             @read -w geonames/*.rdf geonames:geonames/all-geonames-rdf.zip \
                 @filter "pu -gn:childrenFeatures -gn:locationMap
                     -gn:nearbyFeatures -gn:neighbouringFeatures -gn:countryCode
                     -gn:parentFeature -gn:wikipediaArticle -rdfs:isDefinedBy
                     -rdf:type" -r "cu '<graph:geonames>'" , \
             { @read -w dbp_en/* @filter -r "cu '<graph:dbp_en>'" , \
               @read -w dbp_es/* @filter -r "cu '<graph:dbp_es>'" , \
               @read -w dbp_it/* @filter -r "cu '<graph:dbp_it>'" , \
               @read -w dbp_nl/* @filter -r "cu '<graph:dbp_nl>'" } \
                 @filter "ou -bibo pu -dc:rights -dc:language -foaf:primaryTopic" } \
           @filter "ol -'' o@ +'en' +'es' +'it' +'nl' -* o^ +xsd -*" \
           @write filtered2.tql.gz \
           @tbox \
           @filter "spou -bibo -con -owl:Thing -schema:Thing -foaf:Document -dc:subject
               -foaf:page -dct:relation" \
           @write tbox2.tql.gz

Similarly, steps 3-6 can be aggregated in a single macro-step:

    rdfpro @read filtered.tql.gz \
           @smush -S 2048M http://dbpedia http://it.dbpedia http://es.dbpedia \
               http://nl.dbpedia http://rdf.freebase.com http://sws.geonames.org \
           @infer -c '<graph:vocab>' -r rdfs1,rdfs2,rdfs3,rdfs5,rdfs6,rdfs7,rdfs9,rdfs10,rdfs11,rdfs12,rdfs13 -d tbox.tql.gz \
           @filter "tu -bibo -con -owl:Thing -schema:Thing -foaf:Document pu -bibo -con -dc:subject -foaf:page -dct:relation" \
           @unique -m \
           @write dataset2.tql.gz \
           @stats \
           @read tbox.tql.gz \
           @write statistics2.tql.gz

Bash script [process_aggregated.sh](example/process_aggregated.sh) can be used to issue these two commands.


### Results

The table below reports the results of executing the processing steps individually and aggregated on an Intel Core I7 860 machine with 16 GB RAM and a 500GB 7200RPM hard disk.
Smushing and inference add duplicates that are removed with merging.
TBox extraction and filtering are fast, while other steps are slower because complex or due to the need to sort data or process it in multiple passes.
The aggregation of processing steps leads to a sensible reduction of the total processing time from 18953 s to 13531 s.

<table>
<thead>
<tr>
<th rowspan="2">Processing step</th>
<th colspan="2">Input size</th>
<th colspan="2">Output size</th>
<th colspan="2">Throughput</th>
<th rowspan="2">Time<br/>[s]</th>
</tr>
<tr>
<th>[Mquads]</th>
<th>[GB]</th>
<th>[Mquads]</th>
<th>[GB]</th>
<th>[Mquads/s]</th>
<th>[MB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>Step 1 - Filtering</td>
<td>3019.89</td>
<td>29.31</td>
<td>750.78</td>
<td>9.68</td>
<td>0.57</td>
<td>5.70</td>
<td>5266</td>
</tr>
<tr>
<td>Step 2 - TBox extraction</td>
<td>750.78</td>
<td>9.68</td>
<td>0.15</td>
<td>0.01</td>
<td>1.36</td>
<td>18.00</td>
<td>551</td>
</tr>
<tr>
<td>Step 3 - Smushing</td>
<td>750.78</td>
<td>9.68</td>
<td>780.86</td>
<td>10.33</td>
<td>0.31</td>
<td>4.04</td>
<td>2453</td>
</tr>
<tr>
<td>Step 4 - Inference</td>
<td>781.01</td>
<td>10.34</td>
<td>1693.59</td>
<td>15.56</td>
<td>0.22</td>
<td>2.91</td>
<td>3630</td>
</tr>
<tr>
<td>Step 5 - Merging</td>
<td>1693.59</td>
<td>15.56</td>
<td>954.91</td>
<td>7.77</td>
<td>0.38</td>
<td>3.61</td>
<td>4413</td>
</tr>
<tr>
<td>Step 6 - Statistics extraction</td>
<td>954.91</td>
<td>7.77</td>
<td>0.32</td>
<td>0.01</td>
<td>0.36</td>
<td>3.02</td>
<td>2640</td>
</tr>
<tr>
<td>Steps 1-2 aggregated</td>
<td>3019.89</td>
<td>29.31</td>
<td>750.92</td>
<td>9.69</td>
<td>0.56</td>
<td>5.60</td>
<td>5363</td>
</tr>
<tr>
<td>Steps 3-6 aggregated</td>
<td>750.92</td>
<td>9.69</td>
<td>955.23</td>
<td>7.78</td>
<td>0.09</td>
<td>1.21</td>
<td>8168</td>
</tr>
</tbody>
</table>
