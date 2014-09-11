/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti <francesco.corcoglioniti@gmail.com> with support by
 * Marco Rospocher, Marco Amadori and Michele Mostarda.
 * 
 * To the extent possible under law, the author has dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the VOID vocabulary.
 */
final class VOID {

    /** Recommended prefix for the vocabulary namespace: "void". */
    public static final String PREFIX = "voidx";

    /** Vocabulary namespace: "http://rdfs.org/ns/void#". */
    public static final String NAMESPACE = "http://rdfs.org/ns/void#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class void:Dataset. */
    public static final URI DATASET = createURI("Dataset");

    /** Class void:DatasetDescription. */
    public static final URI DATASET_DESCRIPTION = createURI("DatasetDescription");

    /** Class void:Linkset. */
    public static final URI LINKSET = createURI("Linkset");

    /** Class void:TechnicalFeature. */
    public static final URI TECHNICAL_FEATURE = createURI("TechnicalFeature");

    // PROPERTIES

    /** Property void:class. */
    public static final URI CLASS = createURI("class");

    /** Property void:classes. */
    public static final URI CLASSES = createURI("classes");

    /** Property void:classPartition. */
    public static final URI CLASS_PARTITION = createURI("classPartition");

    /** Property void:dataDump. */
    public static final URI DATA_DUMP = createURI("dataDump");

    /** Property void:distinctObjects. */
    public static final URI DISTINCT_OBJECTS = createURI("distinctObjects");

    /** Property void:distinctSubjects. */
    public static final URI DISTINCT_SUBJECTS = createURI("distinctSubjects");

    /** Property void:documents. */
    public static final URI DOCUMENTS = createURI("documents");

    /** Property void:entities. */
    public static final URI ENTITIES = createURI("entities");

    /** Property void:exampleResource. */
    public static final URI EXAMPLE_RESOURCE = createURI("exampleResource");

    /** Property void:feature. */
    public static final URI FEATURE = createURI("feature");

    /** Property void:inDataset. */
    public static final URI IN_DATASET = createURI("inDataset");

    /** Property void:linkPredicate. */
    public static final URI LINK_PREDICATE = createURI("linkPredicate");

    /** Property void:objectsTarget. */
    public static final URI OBJECTS_TARGET = createURI("objectsTarget");

    /** Property void:openSearchDescription. */
    public static final URI OPEN_SEARCH_DESCRIPTION = createURI("openSearchDescription");

    /** Property void:properties. */
    public static final URI PROPERTIES = createURI("properties");

    /** Property void:property. */
    public static final URI PROPERTY = createURI("property");

    /** Property void:propertyPartition. */
    public static final URI PROPERTY_PARTITION = createURI("propertyPartition");

    /** Property void:rootResource. */
    public static final URI ROOT_RESOURCE = createURI("rootResource");

    /** Property void:sparqlEndpoint. */
    public static final URI SPARQL_ENDPOINT = createURI("sparqlEndpoint");

    /** Property void:subjectsTarget. */
    public static final URI SUBJECTS_TARGET = createURI("subjectsTarget");

    /** Property void:subset. */
    public static final URI SUBSET = createURI("subset");

    /** Property void:target. */
    public static final URI TARGET = createURI("target");

    /** Property void:triples. */
    public static final URI TRIPLES = createURI("triples");

    /** Property void:uriLookupEndpoint. */
    public static final URI URI_LOOKUP_ENDPOINT = createURI("uriLookupEndpoint");

    /** Property void:uriRegexPattern. */
    public static final URI URI_REGEX_PATTERN = createURI("uriRegexPattern");

    /** Property void:uriSpace. */
    public static final URI URI_SPACE = createURI("uriSpace");

    /** Property void:vocabulary. */
    public static final URI VOCABULARY = createURI("vocabulary");

    // ALL TERMS

    public static Set<URI> TERMS = Collections.unmodifiableSet(new HashSet<URI>(Arrays.asList(
            DATASET, DATASET_DESCRIPTION, LINKSET, TECHNICAL_FEATURE, CLASS, CLASSES,
            CLASS_PARTITION, DATA_DUMP, DISTINCT_OBJECTS, DISTINCT_SUBJECTS, DOCUMENTS, ENTITIES,
            EXAMPLE_RESOURCE, FEATURE, IN_DATASET, LINK_PREDICATE, OBJECTS_TARGET,
            OPEN_SEARCH_DESCRIPTION, PROPERTIES, PROPERTY, PROPERTY_PARTITION, ROOT_RESOURCE,
            SPARQL_ENDPOINT, SUBJECTS_TARGET, SUBSET, TARGET, TRIPLES, URI_LOOKUP_ENDPOINT,
            URI_REGEX_PATTERN, URI_SPACE, VOCABULARY)));

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private VOID() {
    }

}
