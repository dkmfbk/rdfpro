package eu.fbk.rdfpro.rules;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

public final class RR {

    /** Recommended prefix for the vocabulary namespace: "rr". */
    public static final String PREFIX = "rr";

    /** Vocabulary namespace: "http://dkm.fbk.eu/rdfpro-rules#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/rdfpro-rules#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // CLASSES

    /** Class rr:Rule. */
    public static final URI RULE = createURI("Rule");

    /** Class rr:StaticRule. */
    public static final URI STATIC_RULE = createURI("StaticRule");

    /** Class rr:DynamicRule. */
    public static final URI DYNAMIC_RULE = createURI("DynamicRule");

    // PROPERTIES

    /** Property rr:head. */
    public static final URI HEAD = createURI("head");

    /** Property rr:body. */
    public static final URI BODY = createURI("body");

    /** Property rr:data. */
    public static final URI DATA = createURI("data");

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private RR() {
    }

}
