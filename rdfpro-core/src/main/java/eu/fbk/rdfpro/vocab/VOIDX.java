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
 * Constants for the VOID Extension (VOIDX) vocabulary.
 *
 * @see <a href="http://rdfpro.fbk.eu/ontologies/voidx">vocabulary specification</a>
 */
public final class VOIDX {

    /** Recommended prefix for the vocabulary namespace: "voidx". */
    public static final String PREFIX = "voidx";

    /** Vocabulary namespace: "http://rdfpro.fbk.eu/ontologies/voidx#". */
    public static final String NAMESPACE = "http://rdfpro.fbk.eu/ontologies/voidx#";

    /** Immutable {@link Namespace} constant for the vocabulary namespace. */
    public static final Namespace NS = new SimpleNamespace(VOIDX.PREFIX, VOIDX.NAMESPACE);

    // PROPERTIES

    /** Property voidx:label. */
    public static final IRI LABEL = VOIDX.createIRI("label");

    /** Property voidx:example. */
    public static final IRI EXAMPLE = VOIDX.createIRI("example");

    /** Property voidx:type. */
    public static final IRI TYPE = VOIDX.createIRI("type");

    /** Property voidx:source. */
    public static final IRI SOURCE = VOIDX.createIRI("source");

    /** Property voidx:globalStats. */
    public static final IRI GLOBAL_STATS = VOIDX.createIRI("globalStats");

    /** Property voidx:sourceStats. */
    public static final IRI SOURCE_STATS = VOIDX.createIRI("sourceStats");

    /** Property voidx:tboxTriples. */
    public static final IRI TBOX_TRIPLES = VOIDX.createIRI("tboxTriples");

    /** Property voidx:aboxTriples. */
    public static final IRI ABOX_TRIPLES = VOIDX.createIRI("aboxTriples");

    /** Property voidx:typeTriples. */
    public static final IRI TYPE_TRIPLES = VOIDX.createIRI("typeTriples");

    /** Property voidx:sameAsTriples. */
    public static final IRI SAME_AS_TRIPLES = VOIDX.createIRI("sameAsTriples");

    /** Property voidx:averageProperties. */
    public static final IRI AVERAGE_PROPERTIES = VOIDX.createIRI("averageProperties");

    // ALL TERMS

    public static Set<IRI> TERMS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(VOIDX.LABEL, VOIDX.EXAMPLE, VOIDX.TYPE, VOIDX.SOURCE,
                    VOIDX.GLOBAL_STATS, VOIDX.SOURCE_STATS, VOIDX.TBOX_TRIPLES, VOIDX.ABOX_TRIPLES,
                    VOIDX.TYPE_TRIPLES, VOIDX.SAME_AS_TRIPLES, VOIDX.AVERAGE_PROPERTIES)));

    // HELPER METHODS

    private static IRI createIRI(final String localName) {
        return SimpleValueFactory.getInstance().createIRI(VOIDX.NAMESPACE, localName);
    }

    private VOIDX() {
    }

}
