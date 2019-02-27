
RDFpro usage examples (SAC paper)
=================================

We exemplify here the usage of RDFpro in three scenarios:

  * [Dataset Analysis scenario](#scenario1), where we extract statistics from a dataset and compare it with another one;
  * [Dataset Filtering scenario](#scenario2), where we extract a subset of a dataset;
  * [Dataset Merging scenario](#scenario3), where we combine data from multiple sources, performing smushing, RDFS inference and deduplication.

These scenarios are taken from the [SAC 2015 paper](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2015sac.pdf) and further extended to show the concrete RDFpro commands necessary to carry out the required processing steps.
Note, however, that the numbers here reported (collected after repeating the processing in January 2015) are different from the ones in the paper, as both the accessed data sources and RDFpro have changed in the meanwhile (more data available, faster RDFpro implementation).


## <a name="scenario1"></a> Scenario 1. Dataset Analysis

Dataset analysis comprises all the tasks aimed at providing a qualitative and quantitative characterization of the contents of an RDF dataset, such as the extraction of the data TBox or of instance-level ABox data statistics (e.g., [VOID](http://www.w3.org/TR/void/)). When processing RDF, dataset analysis can be applied both to input and output data. In the first case, it helps identifying relevant data and required pre-processing tasks, especially when the dataset scope is broad (as occurs with many LOD datasets) or its documentation is poor. In the second case, it provides a characterization of output data that is useful for validation and documentation purposes.

We describe here how RDFpro can be used for large-scale dataset analysis. We consider the tasks of extracting TBox and VOID statistics from [Freebase](https://developers.google.com/freebase/data) data, whose schema and statistics are not available online, and the task of comparing the latest Freebase release with a previous release in order to identify newly added triples (we omit the further analysis of this delta, as it can use the same TBox and statistics extraction techniques applied here to the whole Freebase).

### Processing

We assume the latest version of Freebase is available as file `freebase_new.nt.gz`, while a previous version is available as file `freebase_old.nt.gz`.
The figures below show how to use RDFpro to extract TBox and VOID statistics (left figure) and to identify newly added triples (right figure) in the considered scenario:

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<table style="width=100%">
<tr>
<td style="border: none; background-color: white!important; text-align: center"><img src="examples_sac/analysis1.png" alt="Dataset analysis processing steps"/></td>
<td style="border: none; background-color: white!important; text-align: center"><img src="examples_sac/analysis2.png" alt="Dataset analysis processing steps"/></td>
</tr>
</table>
</div>

We use the `@tbox` and `@stats` processors to extract TBox and VOID statistics. The two processors can be used separately by invoking RDFpro twice as follows (note the use of the `-t 100` option: it require to emit statistics of classes and properties with at least 100 instances, which is necessary in order to load the generated statistics in tools such as Protégé - see below):

<pre class="prettyprint lang-sh"><![CDATA[
rdfpro @read freebase_new.nt.gz @tbox @write tbox.tql.gz
rdfpro @read freebase_new.nt.gz @stats -t 100 @write stats.tql.gz
]]></pre>

The two processors can also be composed in a single pipeline where Freebase data is read once and fed to both processors in parallel, as in the figure above. The corresponding RDFpro command is:

<pre class="prettyprint lang-sh"><![CDATA[
rdfpro @read freebase_new.nt.gz { @tbox @write tbox.tql.gz , @stats -t 100 @write stats.tql.gz }
]]></pre>

Extraction of newly added triples can be done exploiting the parallel composition with the difference set operator (flag `d`) to combine quads, using the command:

<pre class="prettyprint lang-sh"><![CDATA[
rdfpro { @read freebase_new.nt.gz , @read freebase_old.nt.gz }d @write new-triples.tql.gz
]]></pre>

### Results

The table below reports the tasks execution times, throughputs, input and output sizes (quads and gzipped bytes) we measured on our test machine, an Intel Core I7 860 workstation with 16 GB RAM and a 500GB 7200RPM hard disk, using `pigz` and `pbzip2` as compressors/decompressors and `sort -S 4096M --batch-size=128 --compress-program=pigz` as the sort command; our 'latest' Freebase release (2789 Mquads) was obtained on 2015/01/18, while our old release (2623 Mquads) was downloaded on 2014/07/10. In addition to the table below, when running the comparison task we measured a disk usage of 92.8 GB for storing the temporary files produced by the sorting-based difference set operator (∼18 bytes per input triple).

<!--
                               Input size             Output size            Time
                               Quads      Size        Quads      Size
    1. TBox extraction         2788992097 29842317645 234018     2757679     1321
    2. Statistics extraction   2788992097 29842317645 131222     1391663     7819
    1-2 Aggregated             2788992097 29842317645 365240     4149342     7784
    3. Comparison              5412372266 57895816562 338909781  3062713292  14207
-->

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
<th>[MiB]</th>
<th>[Mquads]</th>
<th>[MiB]</th>
<th>[Mquads/s]</th>
<th>[MiB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>1. TBox extraction</td>
<td title="2863 @ 09/2014">2789</td>
<td title="28339 @ 09/2014">28460</td>
<td title="0.23 @ 09/2014">0.23</td>
<td title="3.01 @ 09/2014">2.63</td>
<td title="1.43 @ 09/2014">2.11</td>
<td title="14.12 @ 09/2014">21.54</td>
<td title="2006 @ 09/2014">1321</td>
</tr>
<tr>
<td>2. Statistics extraction</td>
<td title="2863 @ 09/2014">2789</td>
<td title="28339 @ 09/2014">28460</td>
<td title="0.13 @ 09/2014">0.13</td>
<td title="1.36 @ 09/2014">1.33</td>
<td title="0.34 @ 09/2014">0.36</td>
<td title="3.36 @ 09/2014">3.64</td>
<td title="8443 @ 09/2014">7819</td>
</tr>
<tr>
<td>1-2 Aggregated</td>
<td title="2863 @ 09/2014">2789</td>
<td title="28339 @ 09/2014">28460</td>
<td title="0.36 @ 09/2014">0.37</td>
<td title="4.35 @ 09/2014">3.96</td>
<td title="0.34 @ 09/2014">0.36</td>
<td title="3.36 @ 09/2014">3.66</td>
<td title="8426 @ 09/2014">7784</td>
</tr>
<tr>
<td>3. Comparison</td>
<td title="5486 @ 09/2014">5412</td>
<td title="55093 @ 09/2014">55214</td>
<td title="260 @ 09/2014">339</td>
<td title="1894 @ 09/2014">2921</td>
<td title="0.42 @ 09/2014">0.38</td>
<td title="4.25 @ 09/2014">3.89</td>
<td title="12955 @ 09/2014">14207</td>
</tr>
</tbody>
</table>

Comparing the two Freebase releases results the most expensive task due to sorting and involved input size. When performed jointly, TBox and statistics extraction present performance figures close to statistics extraction alone, as data parsing is performed once and the cost of TBox extraction (excluded parsing) is negligible. This is an example of how the aggregation of multiple processing tasks in a single RDFpro computation can generally lead to better performances due to a reduction of I/O overhead.

To provide an idea of the how analysis results can be used to explore the dataset, the figure below shows the joint browsing of extracted TBox and statistics in Protégé, exploiting the specific concept annotations emitted by `@stats`. The class and property hierarchies are augmented with the number of entities and property triples (marked as 1 in the figure), as well as with the detected property usage (2), e.g., `O` for object property, `I` for inverse functional; each concept is annotated with an example instance and a VOID partition individual (3), which provides numeric statistics about the concept (4).

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="examples_sac/protege.png" alt="Browsing extracted TBox and VOID statistics with Protégé"/>
</div>


## <a name="scenario2"></a> Scenario 2. Dataset Filtering

When dealing with large RDF datasets, dataset filtering (or slicing) may be required to extract a small subset of interesting data, identified, e.g., based on a previous dataset analysis. Dataset filtering typically consists in (i) identifying the entities of interest in the dataset, based on selection conditions on their URIs, `rdf:type` or other properties; and (ii) extracting all the quads about these entities expressing selected RDF properties. These two operations can be implemented using multiple streaming passes in RDFpro.

We consider here a concrete scenario where the dataset is [Freebase](https://developers.google.com/freebase/data), the entities of interest are musical group (i.e., their `rdf:type` is `fb:music.musical` group) that are still active (i.e., there is no associated property `fb:music.artist.active_end`), and the properties to extract are the group name, genre and place of origin (respectively, `rdfs:label`, `fb:music.artist.genre` and `fb:music.artist.origin`).


### Processing

We assume that the Freebase dump is available as file `freebase_new.nt.gz`.
We implement the filtering task with two invocations of RDFpro as shown in the figure below.

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="examples_sac/filtering.png" alt="Dataset filtering processing steps"/>
</div>

The first invocation (marked as 1 in the figure) generates an RDF file listing as subjects the URIs of the entities of interest. This is done with two parallel `@groovy` processors, extracting respectively musical groups and no more active musical entities, whose outputs are combined with the difference merge criterion using the following RDFpro command:

<pre class="prettyprint lang-sh"><![CDATA[
rdfpro @read freebase.nt.gz \
       { @groovy -p 'emitIf(t == fb:music.musical_group)' , \
         @groovy -p 'if(p == fb:music.artist.active_end) emit(s, rdf:type, fb:music.musical_group, null)' }d \
       @write entities.tql.gz
]]></pre>

The second invocation (marked as 2 in the figure) uses another `@groovy` processor to extract the desired quads, testing predicates and requiring subjects to be contained in the previously extracted file (whose URIs are indexed in memory by a specific function in the `@groovy` expression). The corresponding RDFpro command is:

<pre class="prettyprint lang-sh"><![CDATA[
rdfpro @read freebase.nt.gz \
       @groovy 'def init(args) { instances = loadSet("./instances.tql", "s"); };
                emitIf((p == rdfs:label || p == fb:music.artist.genre || p == fb:music.artist.origin)
                       && instances.match(s) );' \
       @write output.tql.gz
]]></pre>

### Results

The table below reports the execution times, throughputs, input and output sizes of the two invocations of RDFpro on our test machine (see previous scenario for the specs), when applied to the Freebase dump (2789 Mquads) downloaded on 2015/01/18.

<!--
                               Input size             Output size            Time
                               Quads      Size        Quads      Size
    1. Select entities         2788992097 29842317645 207758     867960      1467
    2. Extract quads           2789199855 29843185605 413908     5306945     2689
-->

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
<th>[MiB]</th>
<th>[Mquads]</th>
<th>[MiB]</th>
<th>[Mquads/s]</th>
<th>[MiB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>1. Select entities</td>
<td title="2863 @ 09/2014">2789</td>
<td title="28339 @ 09/2014">28460</td>
<td title="0.20 @ 09/2014">0.21</td>
<td title="0.73 @ 09/2014">0.83</td>
<td title="1.36 @ 09/2014">1.90</td>
<td title="13.4 @ 09/2014">19.40</td>
<td title="2111 @ 09/2014">1467</td>
</tr>
<tr>
<td>2. Extract quads</td>
<td title="2863 @ 09/2014">2789</td>
<td title="28339 @ 09/2014">28461</td>
<td title="0.42 @ 09/2014">0.41</td>
<td title="5.17 @ 09/2014">5.06</td>
<td title="1.15 @ 09/2014">1.04</td>
<td title="11.4 @ 09/2014">10.58</td>
<td title="2481 @ 09/2014">2689</td>
</tr>
</tbody>
</table>

Although simple, the example shows how practical, large-scale filtering tasks are feasible with RDFpro. Streaming allows processing large amounts of data, while sorting enables the use of the intersection, union and difference set operators to implement, respectively, the conjunction, disjunction and negation of entity selection conditions.

More complex scenarios may be addressed with additional invocations that progressively augment the result (e.g., a third invocation can identify albums of selected artists, while a fourth invocation can extract the quads describing them). In cases where RDFpro model is insufficient or impractical (e.g., due to the need for recursive extraction of related entities, aggregation or join conditions), RDFpro can still be used to perform a first coarse-grained filtering that reduces the number of quads and eases their downstream processing.


## <a name="scenario3"></a> Scenario 3. Dataset Merging

A common usage scenario is dataset merging, where multiple RDF datasets are integrated and prepared for application consumption. Data preparation typically comprises smushing, inference materialization and data deduplication
(possibly with provenance tracking). These tasks make the use of the resulting dataset more easy and efficient, as reasoning and entity aliasing have been already accounted for.

We consider here a concrete dataset merging scenario with data from the following sources, for a total of ~3394 MQ (this scenario is similar to the one considered in the [SemDev paper](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2014iswcSemDev01.pdf) and described [here](example.html), although here we consider much more data and leave out statistics extraction):

  * the Freebase complete [dataset](http://download.freebaseapps.com/) dataset, containing ~2789 MQ as of 18 January 2015

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
    - mapping-based properties (specific), [EN](http://downloads.dbpedia.org/3.9/en/specific_mappingbased_properties_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/specific_mappingbased_properties_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/specific_mappingbased_properties_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/specific_mappingbased_properties_nl.ttl.bz2)
    - raw infobox properties, [EN](http://downloads.dbpedia.org/3.9/en/raw_infobox_properties_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/raw_infobox_properties_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/raw_infobox_properties_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/raw_infobox_properties_nl.ttl.bz2)
    - raw infobox property definitions, [EN](http://downloads.dbpedia.org/3.9/en/raw_infobox_property_definitions_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/raw_infobox_property_definitions_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/raw_infobox_property_definitions_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/raw_infobox_property_definitions_nl.ttl.bz2)
    - short abstracts, [EN](http://downloads.dbpedia.org/3.9/en/short_abstracts_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/short_abstracts_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/short_abstracts_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/short_abstracts_nl.ttl.bz2)
    - long abstracts, [EN](http://downloads.dbpedia.org/3.9/en/long_abstracts_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/long_abstracts_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/long_abstracts_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/long_abstracts_nl.ttl.bz2)
    - SKOS categories, [EN](http://downloads.dbpedia.org/3.9/en/skos_categories_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/skos_categories_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/skos_categories_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/skos_categories_nl.ttl.bz2)
    - Wikipedia links, [EN](http://downloads.dbpedia.org/3.9/en/wikipedia_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/wikipedia_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/wikipedia_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/wikipedia_links_nl.ttl.bz2)
    - interlanguage links, [EN](http://downloads.dbpedia.org/3.9/en/interlanguage_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/interlanguage_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/interlanguage_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/interlanguage_links_nl.ttl.bz2)
    - DBpedia IRI - URI `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/iri_same_as_uri_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/iri_same_as_uri_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/iri_same_as_uri_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/iri_same_as_uri_nl.ttl.bz2)
    - DBpedia - Freebase `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/freebase_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/freebase_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/freebase_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/freebase_links_nl.ttl.bz2)
    - DBpedia - GeoNames `owl:sameAs` links, [EN](http://downloads.dbpedia.org/3.9/en/geonames_links_en.ttl.bz2), [ES](http://downloads.dbpedia.org/3.9/es/geonames_links_es.ttl.bz2), [IT](http://downloads.dbpedia.org/3.9/it/geonames_links_it.ttl.bz2), [NL](http://downloads.dbpedia.org/3.9/nl/geonames_links_nl.ttl.bz2)
    - person data, [EN](http://downloads.dbpedia.org/3.9/en/persondata_en.ttl.bz2)
    - PND codes, [EN](http://downloads.dbpedia.org/3.9/en/pnd_en.ttl.bz2)

  * vocabulary definition files for [FOAF](http://xmlns.com/foaf/0.1/), [SKOS](http://www.w3.org/2004/02/skos/core#), [DCTERMS](http://purl.org/dc/terms/), [WGS84](http://www.w3.org/2003/01/geo/wgs84_pos#), [GEORSS](http://www.w3.org/2005/Incubator/geo/XGR-geo/W3C_XGR_Geo_files/geo_2007.owl)

In other words, we import all Freebase and GeoNames data as they only provide global dump files, while we import selected DBpedia dump files leaving out files for Wikipedia inter-page, redirect and disambiguation links and page and revision IDs/URIs. Bash script [download.sh](examples_sac/download.sh) can be used to automatize the download of all the selected dump files, placing them in multiple directories `vocab` (for vocabularies), `freebase`, `geonames`, `dbp_en` (DBpedia EN), `dbp_es` (DBpedia ES), `dbp_it` (DBpedia IT) and `dbp_nl` (DBpedia NL).


### Processing

The figure below shows the required processing steps (the most significant processor for each step is shown).

<div style="text-align: center; padding-top: 20px; padding-bottom: 20px">
<img src="examples_sac/merging.png" alt="Dataset merging processing steps"/>
</div>

The five steps can be executed both individually (bash script [process_single.sh](examples_sac/process_single.sh)) or by aggregating the first two and the last three (bash script [process_aggregated.sh](examples_sac/process_aggregated.sh). The RDFpro commands for the single steps are reported below.

  * *Transform*. Data transformation serves (i) to track provenance, by placing quads in different named graphs based on the source dataset; and (ii) to adopt optimal serialization format (Turtle Quads) and compression scheme (gzip) that speed up further processing.

    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro { @read metadata.trig , \
             @read vocab/* @transform '=c <graph:vocab>' , \
             @read freebase/* @transform '-spo fb:type.object.name fb:type.object.type <http://rdf.freebase.com/key/*>
                 <http://rdf.freebase.com/ns/user.*> <http://rdf.freebase.com/ns/base.*> =c <graph:freebase>' , \
             @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip \
                 @transform '-p gn:childrenFeatures gn:locationMap gn:nearbyFeatures gn:neighbouringFeatures
                     gn:countryCode gn:parentFeature gn:wikipediaArticle rdfs:isDefinedBy rdf:type =c <graph:geonames>' , \
             { @read dbp_en/* @transform '=c <graph:dbp_en>' , \
               @read dbp_es/* @transform '=c <graph:dbp_es>' , \
               @read dbp_it/* @transform '=c <graph:dbp_it>' , \
               @read dbp_nl/* @transform '=c <graph:dbp_nl>' } \
                 @transform '-o bibo:* -p dc:rights dc:language foaf:primaryTopic' } \
           @transform '+o <*> _:* * *^^xsd:* *@en *@es *@it *@nl' \
           @transform '-o "" ""@en ""@es ""@it ""@nl' \
           @write filtered.tql.gz
    ]]></pre>

  * *TBox extraction*. This step extracts the TBox needed for RDFS inference.

    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro @read filtered.tql.gz \
           @tbox \
           @transform '-o owl:Thing schema:Thing foaf:Document bibo:* con:* -p dc:subject foaf:page dct:relation bibo:* con:*' \
           @write tbox.tql.gz
    ]]></pre>

  * *Smushing*. Smushing identifies `owl:sameAs` equivalence classes and assigns a canonical URI to each of them.

    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro @read filtered.tql.gz \
           @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>'
                  '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
           @write smushed.tql.gz
    ]]></pre>

  * *Inference*. RDFS inference excludes rules `rdfs4a`, `rdfs4b` and `rdfs8` to avoid materializing uninformative `<X rdf:type rdfs:Resource>` quads.

    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro @read smushed.tql.gz \
           @rdfs -c '<graph:vocab>' -d tbox.tql.gz \
           @write inferred.tql.gz
    ]]></pre>

  * *Deduplication*. Deduplication takes quads with the same subject, predicate and object (possibly produced by previous steps) and merges them in a single quad inside a graph linked to all the original sources.

    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro @read inferred.tql.gz \
           @unique -m \
           @write dataset.tql.gz
    ]]></pre>

The RDFpro commands for the aggregated steps are reported below:

  * *Transform + TBox extraction*

    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro { @read metadata.trig , \
             @read vocab/* @transform '=c <graph:vocab>' , \
             @read freebase/* @transform '-spo fb:type.object.name fb:type.object.type <http://rdf.freebase.com/key/*>
                 <http://rdf.freebase.com/ns/user.*> <http://rdf.freebase.com/ns/base.*> =c <graph:freebase>' , \
             @read geonames/*.rdf .geonames:geonames/all-geonames-rdf.zip \
                 @transform '-p gn:childrenFeatures gn:locationMap gn:nearbyFeatures gn:neighbouringFeatures
                     gn:countryCode gn:parentFeature gn:wikipediaArticle rdfs:isDefinedBy rdf:type =c <graph:geonames>' , \
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
    ]]></pre>

  * *Smushing + Inference + Deduplication*
  
    <pre class="prettyprint lang-sh"><![CDATA[
    rdfpro @read filtered.tql.gz \
           @smush '<http://dbpedia>' '<http://it.dbpedia>' '<http://es.dbpedia>' \
                  '<http://nl.dbpedia>' '<http://rdf.freebase.com>' '<http://sws.geonames.org>' \
           @rdfs -c '<graph:vocab>' -d tbox.tql.gz \
           @unique -m \
           @write dataset.tql.gz
    ]]></pre>
    
### Results

The table below reports the execution times, throughputs and input and output sizes of each step, covering both the cases where steps are performed separately via intermediate files and multiple invocations of RDFpro (upper part of the table), or aggregated per processing phase using composition capabilities (lower part). RDFpro also reported the use of ∼2 GB of memory for smushing an `owl:sameAs` graph of ∼38M URIs and ∼8M equivalence classes (∼56 bytes/URI).

<!--
                               Input size             Output size            Time
                               Quads      Size        Quads      Size
    Step 1 - Transform         3309523311 35230880150 3309523311 38423094528 7139
    Step 2 - TBox extraction   3309523311 38423094528 326348     3512736     2057
    Step 3 - Smushing          3309523311 38423094528 3339607280 40456381902 8094
    Step 4 - Inference         3339933628 40459894638 5657929231 52559464635 11242
    Step 5 - Deduplication     5657929231 52559464635 4031696443 34653266025 16759
    Steps 1-2 aggregated       3309523311 35230880150 3309849659 38426567593 6930
    Steps 3-5 aggregated       3309849659 38426567593 4031696443 34637997211 19557
-->

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
<th>[MiB]</th>
<th>[Mquads]</th>
<th>[MiB]</th>
<th>[Mquads/s]</th>
<th>[MiB/s]</th>
</tr>
</thead>
<tbody>
<tr>
<td>1. Transform</td>
<td title="3394 @ 09/2014">3310</td>
<td title="33524 @ 09/2014">33599</td>
<td title="3394 @ 09/2014">3310</td>
<td title="36903 @ 09/2014">36643</td>
<td title="0.42 @ 09/2014">0.46</td>
<td title="4.12 @ 09/2014">4.71</td>
<td title="8137 @ 09/2014">7139</td>
</tr>
<tr>
<td>2. TBox extraction</td>
<td title="3394 @ 09/2014">3310</td>
<td title="36903 @ 09/2014">36643</td>
<td title="&lt;1 @ 09/2014">&lt;1</td>
<td title="~4 @ 09/2014">~4</td>
<td title="1.28 @ 09/2014">1.61</td>
<td title="13.9 @ 09/2014">17.81</td>
<td title="2656 @ 09/2014">2057</td>
</tr>
<tr>
<td>3. Smushing</td>
<td title="3394 @ 09/2014">3310</td>
<td title="36903 @ 09/2014">36643</td>
<td title="3424 @ 09/2014">3340</td>
<td title="38823 @ 09/2014">38582</td>
<td title="0.37 @ 09/2014">0.41</td>
<td title="3.98 @ 09/2014">4.53</td>
<td title="9265 @ 09/2014">8094</td>
</tr>
<tr>
<td>4. Inference</td>
<td title="3424 @ 09/2014">3340</td>
<td title="38823 @ 09/2014">38586</td>
<td title="5615 @ 09/2014">5658</td>
<td title="51927 @ 09/2014">50125</td>
<td title="0.32 @ 09/2014">0.30</td>
<td title="3.66 @ 09/2014">3.43</td>
<td title="10612 @ 09/2014">11242</td>
</tr>
<tr>
<td>5. Deduplication</td>
<td title="5615 @ 09/2014">5658</td>
<td title="51927 @ 09/2014">50125</td>
<td title="4085 @ 09/2014">4032</td>
<td title="31297 @ 09/2014">33048</td>
<td title="0.33 @ 09/2014">0.34</td>
<td title="3.03 @ 09/2014">3.00</td>
<td title="17133 @ 09/2014">16759</td>
</tr>
<tr>
<td>1-2 Aggregated</td>
<td title="3394 @ 09/2014">3310</td>
<td title="33524 @ 09/2014">33599</td>
<td title="3394 @ 09/2014">3310</td>
<td title="36903 @ 09/2014">36646</td>
<td title="0.41 @ 09/2014">0.48</td>
<td title="4.06 @ 09/2014">4.85</td>
<td title="8247 @ 09/2014">6930</td>
</tr>
<tr>
<td>3-5 Aggregated</td>
<td title="3394 @ 09/2014">3310</td>
<td title="36903 @ 09/2014">36646</td>
<td title="4085 @ 09/2014">4032</td>
<td title="31446 @ 09/2014">33033</td>
<td title="0.14 @ 09/2014">0.17</td>
<td title="1.56 @ 09/2014">1.87</td>
<td title="23734 @ 09/2014">19557</td>
</tr>
</tbody>
</table>

Also in this scenario, the aggregation of multiple processing tasks leads to a marked reduction of the total processing time from 45291 s to 26487 s (times were respectively 47803 s and 31981 s in September 2014 test) due to the elimination of the I/O overhead for intermediate files. While addressed separately, the three scenarios of dataset analysis, filtering and merging are often combined in practice, e.g., to remove unwanted ABox and TBox quads from input data, merge remaining quads and analyze the result producing statistics that describe and document it; an example of such combination is reported in the [ISWC SemDev paper](https://dkm-static.fbk.eu/people/rospocher/files/pubs/2014iswcSemDev01.pdf) and further detailed in this [page](example.html).
