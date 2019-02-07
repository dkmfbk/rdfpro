/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.vocab;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Constants for the VOID vocabulary.
 */
public final class VOID {

    /** Recommended prefix for the vocabulary namespace: "void". */
    public static final String PREFIX = "void";

    /** Vocabulary namespace: "http://rdfs.org/ns/void#". */
    public static final String NAMESPACE = "http://rdfs.org/ns/void#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(VOID.PREFIX, VOID.NAMESPACE);

    // CLASSES

    /** Class void:Dataset. */
    public static final IRI DATASET = VOID.createIRI("Dataset");

    /** Class void:DatasetDescription. */
    public static final IRI DATASET_DESCRIPTION = VOID.createIRI("DatasetDescription");

    /** Class void:Linkset. */
    public static final IRI LINKSET = VOID.createIRI("Linkset");

    /** Class void:TechnicalFeature. */
    public static final IRI TECHNICAL_FEATURE = VOID.createIRI("TechnicalFeature");

    // PROPERTIES

    /** Property void:class. */
    public static final IRI CLASS = VOID.createIRI("class");

    /** Property void:classes. */
    public static final IRI CLASSES = VOID.createIRI("classes");

    /** Property void:classPartition. */
    public static final IRI CLASS_PARTITION = VOID.createIRI("classPartition");

    /** Property void:dataDump. */
    public static final IRI DATA_DUMP = VOID.createIRI("dataDump");

    /** Property void:distinctObjects. */
    public static final IRI DISTINCT_OBJECTS = VOID.createIRI("distinctObjects");

    /** Property void:distinctSubjects. */
    public static final IRI DISTINCT_SUBJECTS = VOID.createIRI("distinctSubjects");

    /** Property void:documents. */
    public static final IRI DOCUMENTS = VOID.createIRI("documents");

    /** Property void:entities. */
    public static final IRI ENTITIES = VOID.createIRI("entities");

    /** Property void:exampleResource. */
    public static final IRI EXAMPLE_RESOURCE = VOID.createIRI("exampleResource");

    /** Property void:feature. */
    public static final IRI FEATURE = VOID.createIRI("feature");

    /** Property void:inDataset. */
    public static final IRI IN_DATASET = VOID.createIRI("inDataset");

    /** Property void:linkPredicate. */
    public static final IRI LINK_PREDICATE = VOID.createIRI("linkPredicate");

    /** Property void:objectsTarget. */
    public static final IRI OBJECTS_TARGET = VOID.createIRI("objectsTarget");

    /** Property void:openSearchDescription. */
    public static final IRI OPEN_SEARCH_DESCRIPTION = VOID.createIRI("openSearchDescription");

    /** Property void:properties. */
    public static final IRI PROPERTIES = VOID.createIRI("properties");

    /** Property void:property. */
    public static final IRI PROPERTY = VOID.createIRI("property");

    /** Property void:propertyPartition. */
    public static final IRI PROPERTY_PARTITION = VOID.createIRI("propertyPartition");

    /** Property void:rootResource. */
    public static final IRI ROOT_RESOURCE = VOID.createIRI("rootResource");

    /** Property void:sparqlEndpoint. */
    public static final IRI SPARQL_ENDPOINT = VOID.createIRI("sparqlEndpoint");

    /** Property void:subjectsTarget. */
    public static final IRI SUBJECTS_TARGET = VOID.createIRI("subjectsTarget");

    /** Property void:subset. */
    public static final IRI SUBSET = VOID.createIRI("subset");

    /** Property void:target. */
    public static final IRI TARGET = VOID.createIRI("target");

    /** Property void:triples. */
    public static final IRI TRIPLES = VOID.createIRI("triples");

    /** Property void:uriLookupEndpoint. */
    public static final IRI URI_LOOKUP_ENDPOINT = VOID.createIRI("uriLookupEndpoint");

    /** Property void:uriRegexPattern. */
    public static final IRI URI_REGEX_PATTERN = VOID.createIRI("uriRegexPattern");

    /** Property void:uriSpace. */
    public static final IRI URI_SPACE = VOID.createIRI("uriSpace");

    /** Property void:vocabulary. */
    public static final IRI VOCABULARY = VOID.createIRI("vocabulary");

    // ALL TERMS

    public static Set<IRI> TERMS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            VOID.DATASET, VOID.DATASET_DESCRIPTION, VOID.LINKSET, VOID.TECHNICAL_FEATURE,
            VOID.CLASS, VOID.CLASSES, VOID.CLASS_PARTITION, VOID.DATA_DUMP, VOID.DISTINCT_OBJECTS,
            VOID.DISTINCT_SUBJECTS, VOID.DOCUMENTS, VOID.ENTITIES, VOID.EXAMPLE_RESOURCE,
            VOID.FEATURE, VOID.IN_DATASET, VOID.LINK_PREDICATE, VOID.OBJECTS_TARGET,
            VOID.OPEN_SEARCH_DESCRIPTION, VOID.PROPERTIES, VOID.PROPERTY, VOID.PROPERTY_PARTITION,
            VOID.ROOT_RESOURCE, VOID.SPARQL_ENDPOINT, VOID.SUBJECTS_TARGET, VOID.SUBSET,
            VOID.TARGET, VOID.TRIPLES, VOID.URI_LOOKUP_ENDPOINT, VOID.URI_REGEX_PATTERN,
            VOID.URI_SPACE, VOID.VOCABULARY)));

    // HELPER METHODS

    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(VOID.NAMESPACE, localName);
    }

    private VOID() {
    }

}
