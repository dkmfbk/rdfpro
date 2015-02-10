
RDFpro usage example (SemDev paper)
===================================

We describe here an example of using RDFpro for integrating RDF data from Freebase, GeoNames and DBpedia (version 3.9) in the four languaged EN, ES, IT and NL, performing smushing, inference, deduplication and statistics extraction.

This example is taken from the [ISWC SemDev 2014 paper](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2014iswcSemDev01.pdf) and here we provide further details included the concrete RDFpro commands necessary to carry out the integration task.
Note, however, that the numbers here reported (collected after repeating the processing in January 2015) are different from the ones in the paper, as both the accessed data sources and RDFpro have changed in the meanwhile (more data available, faster RDFpro implementation).


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

    rdfpro { @read metadata.trig , \
             @read vocab/* @transform '=c <graph:vocab>' , \
             @read freebase/* @transform '-spo fb:common.topic fb:common.topic.article fb:common.topic.notable_for
                 fb:common.topic.notable_types fb:common.topic.topic_equivalent_webpage <http://rdf.freebase.com/key/*>
                 <http://rdf.freebase.com/ns/common.notable_for*> <http://rdf.freebase.com/ns/common.document*>
                 <http://rdf.freebase.com/ns/type.*> <http://rdf.freebase.com/ns/user.*> <http://rdf.freebase.com/ns/base.*>
                 <http://rdf.freebase.com/ns/freebase.*> <http://rdf.freebase.com/ns/dataworld.*>
                 <http://rdf.freebase.com/ns/pipeline.*> <http://rdf.freebase.com/ns/atom.*> <http://rdf.freebase.com/ns/community.*>
                 =c <graph:freebase>' , \
             @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip \
                 @transform '-p gn:childrenFeatures gn:locationMap
                     gn:nearbyFeatures gn:neighbouringFeatures gn:countryCode
                     gn:parentFeature gn:wikipediaArticle rdfs:isDefinedBy
                     rdf:type =c <graph:geonames>' , \
             { @read dbp_en/* @transform '=c <graph:dbp_en>' , \
               @read dbp_es/* @transform '=c <graph:dbp_es>' , \
               @read dbp_it/* @transform '=c <graph:dbp_it>' , \
               @read dbp_nl/* @transform '=c <graph:dbp_nl>' } \
                 @transform '-o bibo:* -p dc:rights dc:language foaf:primaryTopic' } \
           @transform '+o <*> _:* * *^^xsd:* *@en *@es *@it *@nl' \
           @transform '-o "" ""@en ""@es ""@it ""@nl' \
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
           @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
           @write tbox.tql.gz

TBox quads are extracted from filtered data and stored, filtering out unwanted top level classes (`owl:Thing`, `schema:Thing`, `foaf:Document`) and vocabulary alignments (to [`bibo`](http://purl.org/ontology/bibo/) and [`con`](http://www.w3.org/2000/10/swap/pim/contact#) terms and `dc:subject`).

##### Step 3 Smushing

    rdfpro @read filtered.tql.gz \
           @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
                  '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
           @write smushed.tql.gz

Filtered data is smushed so to use canonical URIs for each `owl:sameAs` equivalence class, producing an intermediate smushed file. Note the specification of a ranked list of namespaces for selecting the canonical URIs.

##### Step 4 Inference

    rdfpro @read smushed.tql.gz \
           @rdfs -c '<graph:vocab>' -e rdfs4a,rdfs4b,rdfs8 -d tbox.tql.gz \
           @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
           @write inferred.tql.gz

The deductive closure of smushed data is computed and saved, using the extracted TBox and excluding RDFS rules `rdfs4a`, `rdfs4b` and `rdfs8` (and keeping the remaining ones) to avoid inferring uninformative `X rdf:type rdfs:Resource` quads.
The closed TBox is placed in graph `<graph:vocab>`. A further filtering is done to be sure that no unwanted triple is present in the result dataset due to inference.

##### Step 5 Merging

    rdfpro @read inferred.tql.gz \
           @unique -m \
           @write dataset.tql.gz

Quads with the same subject, predicate and object are merged and placed in a graph linked to the original sources to track provenance (note the use of the `-m` option).

##### Step 6 Statistics extraction

    rdfpro { @read tbox.tql.gz , @read dataset.tql.gz @stats } @write statistics.tql.gz

VOID statistics are extracted and merged with TBox data, forming an annotated ontology that documents the produced dataset.


### Data processing (aggregate steps)

The 6 steps previously listed can be also aggregated to reduce overhead for writing and reading back intermediate files, exploiting RDFpro capability to arbitrarily compose processors and write intermediate results.
In particular, steps 1-2 can be aggregated as follows:

    rdfpro { @read metadata.trig , \
             @read vocab/* @transform '=c <graph:vocab>' , \
             @read freebase/* @transform '-spo fb:common.topic fb:common.topic.article fb:common.topic.notable_for
                 fb:common.topic.notable_types fb:common.topic.topic_equivalent_webpage <http://rdf.freebase.com/key/*>
                 <http://rdf.freebase.com/ns/common.notable_for*> <http://rdf.freebase.com/ns/common.document*>
                 <http://rdf.freebase.com/ns/type.*> <http://rdf.freebase.com/ns/user.*> <http://rdf.freebase.com/ns/base.*>
                 <http://rdf.freebase.com/ns/freebase.*> <http://rdf.freebase.com/ns/dataworld.*>
                 <http://rdf.freebase.com/ns/pipeline.*> <http://rdf.freebase.com/ns/atom.*>  <http://rdf.freebase.com/ns/community.*>
                 =c <graph:freebase>' , \
             @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip \
                 @transform '-p gn:childrenFeatures gn:locationMap
                     gn:nearbyFeatures gn:neighbouringFeatures gn:countryCode
                     gn:parentFeature gn:wikipediaArticle rdfs:isDefinedBy
                     rdf:type =c <graph:geonames>' , \
             { @read dbp_en/* @transform '=c <graph:dbp_en>' , \
               @read dbp_es/* @transform '=c <graph:dbp_es>' , \
               @read dbp_it/* @transform '=c <graph:dbp_it>' , \
               @read dbp_nl/* @transform '=c <graph:dbp_nl>' } \
                 @transform '-o bibo:* -p dc:rights dc:language foaf:primaryTopic' } \
           @transform '+o <*> _:* * *^^xsd:* *@en *@es *@it *@nl' \
           @transform '-o "" ""@en ""@es ""@it ""@nl' \
           @write filtered.tql.gz \
           @tbox \
           @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
           @write tbox.tql.gz

Similarly, steps 3-6 can be aggregated in a single macro-step:

    rdfpro @read filtered.tql.gz \
           @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
                  '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
           @rdfs -c '<graph:vocab>' -e rdfs4a,rdfs4b,rdfs8 -d tbox.tql.gz \
           @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
           @unique -m \
           @write dataset.tql.gz \
           @stats \
           @read tbox.tql.gz \
           @write statistics.tql.gz

Bash script [process_aggregated.sh](example/process_aggregated.sh) can be used to issue these two commands.


### Results

The table below reports the results of executing the processing steps individually and aggregated on an Intel Core I7 860 machine with 16 GB RAM and a 500GB 7200RPM hard disk, using `pigz` and `pbzip2` as compressors/decompressors and `sort -S 4096M --batch-size=128 --compress-program=pigz` as the sort command.
Smushing and inference add duplicates that are removed with merging.
TBox extraction and filtering are fast, while other steps are slower because complex or due to the need to sort data or process it in multiple passes.
The aggregation of processing steps leads to a sensible reduction of the total processing time from 17500 s to 10905 s (note: times were respectively 18953 s and 13531 s in July 2014 test).

<!--
                               Input size             Output size            Time
                               Quads      Size        Quads      Size
    Step 1 - Filtering         3174641760 33208598528 769671526  10350542495 4194
    Step 2 - TBox extraction   769671526  10350542495 149650     1335279     412
    Step 3 - Smushing          769671526  10350542495 799755339  11049733537 2265
    Step 4 - Inference         799904989  11051068816 1690704128 16655132884 3780
    Step 5 - Merging           1690704128 16655132884 964179957  9511455269  4254
    Step 6 - Statistics        964329607  9512790548  297867     3390328     2595
    Steps 1-2 aggregated       3174641760 33208598528 769821176  10351877774 4315
    Steps 3-6 aggregated       769821176  10351877774 964477824  9520662330  6590
-->

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
<th>[MiB]</th>
<th>[Mquads]</th>
<th>[MiB]</th>
<th>[Mquads/s]</th>
<th>[MiB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>Step 1 - Filtering</td>
<td title="3020 @ 07/2014">3175</td>
<td title="30013 @ 07/2014">31670</td>
<td title="751 @ 07/2014">770</td>
<td title="9912 @ 07/2014">9871</td>
<td title="0.57 @ 07/2014">0.76</td>
<td title="5.70 @ 07/2014">7.55</td>
<td title="5266 @ 07/2014">4194</td>
</tr>
<tr>
<td>Step 2 - TBox extraction</td>
<td title="751 @ 07/2014">770</td>
<td title="9912 @ 07/2014">9871</td>
<td title="&lt;1 @ 07/2014">&lt;1</td>
<td title="~1 @ 07/2014">~1</td>
<td title="1.36 @ 07/2014">1.87</td>
<td title="18.00 @ 07/2014">23.95</td>
<td title="551 @ 07/2014">412</td>
</tr>
<tr>
<td>Step 3 - Smushing</td>
<td title="751 @ 07/2014">770</td>
<td title="9912 @ 07/2014">9871</td>
<td title="781 @ 07/2014">800</td>
<td title="10578 @ 07/2014">10538</td>
<td title="0.31 @ 07/2014">0.34</td>
<td title="4.04 @ 07/2014">4.36</td>
<td title="2453 @ 07/2014">2265</td>
</tr>
<tr>
<td>Step 4 - Inference</td>
<td title="781 @ 07/2014">800</td>
<td title="10578 @ 07/2014">10539</td>
<td title="1694 @ 07/2014">1691</td>
<td title="15933 @ 07/2014">15884</td>
<td title="0.22 @ 07/2014">0.21</td>
<td title="2.91 @ 07/2014">2.79</td>
<td title="3630 @ 07/2014">3780</td>
</tr>
<tr>
<td>Step 5 - Merging</td>
<td title="1694 @ 07/2014">1691</td>
<td title="15933 @ 07/2014">15884</td>
<td title="955 @ 07/2014">964</td>
<td title="7956 @ 07/2014">9071</td>
<td title="0.38 @ 07/2014">0.40</td>
<td title="3.61 @ 07/2014">3.73</td>
<td title="4413 @ 07/2014">4254</td>
</tr>
<tr>
<td>Step 6 - Statistics extraction</td>
<td title="955 @ 07/2014">964</td>
<td title="7956 @ 07/2014">9072</td>
<td title="&lt;1 @ 07/2014">&lt;1</td>
<td title="~3 @ 07/2014">~3</td>
<td title="0.36 @ 07/2014">0.37</td>
<td title="3.02 @ 07/2014">3.50</td>
<td title="2640 @ 07/2014">2595</td>
</tr>
<tr>
<td>Steps 1-2 aggregated</td>
<td title="3020 @ 07/2014">3175</td>
<td title="30013 @ 07/2014">31670</td>
<td title="751 @ 07/2014">770</td>
<td title="9913 @ 07/2014">9872</td>
<td title="0.56 @ 07/2014">0.74</td>
<td title="5.60 @ 07/2014">7.34</td>
<td title="5363 @ 07/2014">4315</td>
</tr>
<tr>
<td>Steps 3-6 aggregated</td>
<td title="751 @ 07/2014">770</td>
<td title="9913 @ 07/2014">9872</td>
<td title="955 @ 07/2014">964</td>
<td title="7967 @ 07/2014">9080</td>
<td title="0.09 @ 07/2014">0.12</td>
<td title="1.21 @ 07/2014">1.50</td>
<td title="8168 @ 07/2014">6590</td>
</tr>
</tbody>
</table>
