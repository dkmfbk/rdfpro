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
package eu.fbk.rdfpro.vocab;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Namespace;
import org.openrdf.model.URI;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Constants for the VOID Extension (VOIDX) vocabulary.
 *
 * @see <a href="http://dkm.fbk.eu/ontologies/voidx">vocabulary specification</a>
 */
public final class VOIDX {

    /** Recommended prefix for the vocabulary namespace: "voidx". */
    public static final String PREFIX = "voidx";

    /** Vocabulary namespace: "http://dkm.fbk.eu/ontologies/voidx#". */
    public static final String NAMESPACE = "http://dkm.fbk.eu/ontologies/voidx#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new NamespaceImpl(PREFIX, NAMESPACE);

    // PROPERTIES

    /** Property voidx:label. */
    public static final URI LABEL = createURI("label");

    /** Property voidx:example. */
    public static final URI EXAMPLE = createURI("example");

    /** Property voidx:type. */
    public static final URI TYPE = createURI("type");

    /** Property voidx:source. */
    public static final URI SOURCE = createURI("source");

    /** Property voidx:globalStats. */
    public static final URI GLOBAL_STATS = createURI("globalStats");

    /** Property voidx:sourceStats. */
    public static final URI SOURCE_STATS = createURI("sourceStats");

    /** Property voidx:tboxTriples. */
    public static final URI TBOX_TRIPLES = createURI("tboxTriples");

    /** Property voidx:aboxTriples. */
    public static final URI ABOX_TRIPLES = createURI("aboxTriples");

    /** Property voidx:typeTriples. */
    public static final URI TYPE_TRIPLES = createURI("typeTriples");

    /** Property voidx:sameAsTriples. */
    public static final URI SAME_AS_TRIPLES = createURI("sameAsTriples");

    /** Property voidx:averageProperties. */
    public static final URI AVERAGE_PROPERTIES = createURI("averageProperties");

    // ALL TERMS

    public static Set<URI> TERMS = Collections.unmodifiableSet(new HashSet<URI>(Arrays.asList(
            LABEL, EXAMPLE, TYPE, SOURCE, GLOBAL_STATS, SOURCE_STATS, TBOX_TRIPLES, ABOX_TRIPLES,
            TYPE_TRIPLES, SAME_AS_TRIPLES, AVERAGE_PROPERTIES)));

    // HELPER METHODS

    private static URI createURI(final String localName) {
        return ValueFactoryImpl.getInstance().createURI(NAMESPACE, localName);
    }

    private VOIDX() {
    }

}
