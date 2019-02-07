/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2015 by Francesco Corcoglioniti with support by Alessio Palmero Aprosio and Marco
 * Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.vocab;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * Constants for the RDFpro Rules (RR) vocabulary.
 *
 * @see <a href="http://rdfpro.fbk.eu/ontologies/rules">vocabulary specification</a>
 */
public final class RR {

    /** Recommended prefix for the vocabulary namespace: "rr". */
    public static final String PREFIX = "rr";

    /** Vocabulary namespace: "http://dkm.fbk.eu/rdfpro-rules#". */
    public static final String NAMESPACE = "http://rdfpro.fbk.eu/ontologies/rules#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    // CLASSES

    /** Class rr:Rule. */
    public static final IRI RULE = createIRI("Rule");

    /** Class rr:FixpointRule. */
    public static final IRI FIXPOINT_RULE = createIRI("FixpointRule");

    /** Class rr:NonFixpointRule. */
    public static final IRI NON_FIXPOINT_RULE = createIRI("NonFixpointRule");

    /** Class rr:StaticTerm. */
    public static final IRI META_VOCABULARY_TERM = createIRI("MetaVocabularyTerm");

    // PROPERTIES

    /** Property rr:delete. */
    public static final IRI DELETE = createIRI("delete");

    /** Property rr:insert. */
    public static final IRI INSERT = createIRI("insert");

    /** Property rr:where. */
    public static final IRI WHERE = createIRI("where");

    /** Property rr:where. */
    public static final IRI PHASE = createIRI("phase");

    /** Property rr:head. */
    public static final IRI HEAD = createIRI("head"); // alias for rr:insert

    /** Property rr:body. */
    public static final IRI BODY = createIRI("body"); // alias for rr:where

    /** Property rr:prefix. */
    public static final IRI PREFIX_PROPERTY = createIRI("prefix");

    // FUNCTIONS

    /** Function rr:sid. */
    public static final IRI SID = createIRI("sid");

    /** Function rr:mint. */
    public static final IRI MINT = createIRI("mint");

    /** Function rr:compatibleDatatype. */
    public static final IRI COMPATIBLE_DATATYPE = createIRI("compatibleDatatype");

    /** Function rr:starSelectGraph. */
    public static final IRI STAR_SELECT_GRAPH = createIRI("starSelectGraph");

    /** Function rr:namespace. */
    public static final IRI NAMESPACE_P = createIRI("namespace");

    /** Function rr:localName. */
    public static final IRI LOCAL_NAME = createIRI("localName");

    /** Function rr:in. */
    public static final IRI IN = createIRI("in");

    // HELPER METHODS

    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(NAMESPACE, localName);
    }

    private RR() {
    }

}
