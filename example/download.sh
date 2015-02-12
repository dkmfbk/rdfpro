#!/bin/sh

mkdir -p vocab
wget --header="Accept: application/rdf+xml" -O vocab/foaf.rdf http://xmlns.com/foaf/0.1/
wget --header="Accept: application/rdf+xml" -O vocab/skos.rdf http://www.w3.org/2004/02/skos/core#
wget --header="Accept: application/rdf+xml" -O vocab/wgs84_pos.rdf http://www.w3.org/2003/01/geo/wgs84_pos#
wget --header="Accept: application/rdf+xml" -O vocab/dcterms.rdf http://purl.org/dc/terms/
wget -N --directory-prefix=vocab http://www.w3.org/2005/Incubator/geo/XGR-geo/W3C_XGR_Geo_files/geo_2007.owl

mkdir -p geonames
wget -N --directory-prefix=geonames \
    http://www.geonames.org/ontology/ontology_v3.1.rdf \
    http://www.geonames.org/ontology/mappings_v3.01.rdf \
    http://download.geonames.org/all-geonames-rdf.zip

mkdir -p freebase
if [ ! -f freebase/freebase.nt.gz ]; then
    wget -O freebase/freebase.nt.gz http://download.freebaseapps.com/
fi

mkdir -p dbp_en
wget -N --directory-prefix=dbp_en \
    http://downloads.dbpedia.org/3.9/dbpedia_3.9.owl.bz2 \
    http://downloads.dbpedia.org/3.9/en/article_categories_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/category_labels_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/external_links_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/geo_coordinates_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/homepages_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/images_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/instance_types_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/instance_types_heuristic_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/labels_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/mappingbased_properties_cleaned_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/persondata_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/pnd_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/short_abstracts_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/skos_categories_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/wikipedia_links_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/interlanguage_links_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/iri_same_as_uri_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/freebase_links_en.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/en/geonames_links_en.ttl.bz2

mkdir -p dbp_es
wget -N --directory-prefix=dbp_es \
    http://downloads.dbpedia.org/3.9/es/article_categories_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/category_labels_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/external_links_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/geo_coordinates_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/homepages_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/images_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/instance_types_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/labels_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/mappingbased_properties_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/short_abstracts_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/skos_categories_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/wikipedia_links_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/interlanguage_links_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/iri_same_as_uri_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/freebase_links_es.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/es/geonames_links_es.ttl.bz2

mkdir -p dbp_it
wget -N --directory-prefix=dbp_it \
    http://downloads.dbpedia.org/3.9/it/article_categories_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/category_labels_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/external_links_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/geo_coordinates_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/homepages_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/images_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/instance_types_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/labels_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/mappingbased_properties_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/short_abstracts_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/skos_categories_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/wikipedia_links_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/interlanguage_links_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/iri_same_as_uri_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/freebase_links_it.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/it/geonames_links_it.ttl.bz2

mkdir -p dbp_nl
wget -N --directory-prefix=dbp_nl \
    http://downloads.dbpedia.org/3.9/nl/article_categories_en_uris_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/category_labels_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/external_links_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/geo_coordinates_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/instance_types_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/labels_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/mappingbased_properties_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/short_abstracts_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/skos_categories_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/wikipedia_links_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/interlanguage_links_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/iri_same_as_uri_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/freebase_links_nl.ttl.bz2 \
    http://downloads.dbpedia.org/3.9/nl/geonames_links_nl.ttl.bz2
