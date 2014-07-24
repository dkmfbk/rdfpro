
RDFpro usage example
====================

In this example, we show how to use RDFpro to select and integrate RDF data from Freebase, GeoNames and DBpedia in the four languaged EN, ES, IT and NL, performing smushing, inference, deduplication and statistics extraction.

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
