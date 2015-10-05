package eu.fbk.rdfpro.vocab;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

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
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class rr:Rule. */
    public static final URI RULE = createURI("Rule");

    /** Class rr:FixpointRule. */
    public static final URI FIXPOINT_RULE = createURI("FixpointRule");

    /** Class rr:NonFixpointRule. */
    public static final URI NON_FIXPOINT_RULE = createURI("NonFixpointRule");

    /** Class rr:StaticTerm. */
    public static final URI META_VOCABULARY_TERM = createURI("MetaVocabularyTerm");

    // PROPERTIES

    /** Property rr:delete. */
    public static final URI DELETE = createURI("delete");

    /** Property rr:insert. */
    public static final URI INSERT = createURI("insert");

    /** Property rr:where. */
    public static final URI WHERE = createURI("where");

    /** Property rr:where. */
    public static final URI PHASE = createURI("phase");

    /** Property rr:head. */
    public static final URI HEAD = createURI("head"); // alias for rr:insert

    /** Property rr:body. */
    public static final URI BODY = createURI("body"); // alias for rr:where

    /** Property rr:prefix. */
    public static final URI PREFIX_PROPERTY = createURI("prefix");

    // FUNCTIONS

    /** Function rr:mint. */
    public static final URI MINT = createURI("mint");

    /** Function rr:compatibleDatatype. */
    public static final URI COMPATIBLE_DATATYPE = createURI("compatibleDatatype");

    /** Function rr:starSelectGraph. */
    public static final URI STAR_SELECT_GRAPH = createURI("starSelectGraph");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private RR() {
    }

}
