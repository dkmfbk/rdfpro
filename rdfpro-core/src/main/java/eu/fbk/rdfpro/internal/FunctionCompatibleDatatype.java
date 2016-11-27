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
package eu.fbk.rdfpro.internal;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import eu.fbk.rdfpro.vocab.RR;

public class FunctionCompatibleDatatype implements Function {

    private static final Set<Pair> COMPATIBILITIES;

    static {
        final Set<Pair> set = new HashSet<>();

        set.add(new Pair(RDFS.LITERAL, RDF.XMLLITERAL));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.DURATION));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.DATETIME));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.DATE));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.TIME));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.GYEARMONTH));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.GMONTHDAY));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.GYEAR));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.GMONTH));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.GDAY));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.BOOLEAN));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.BASE64BINARY));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.HEXBINARY));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.FLOAT));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.DOUBLE));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.DECIMAL));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.ANYURI));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.QNAME));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.NOTATION));
        set.add(new Pair(RDFS.LITERAL, XMLSchema.STRING));
        set.add(new Pair(XMLSchema.DECIMAL, XMLSchema.INTEGER));
        set.add(new Pair(XMLSchema.INTEGER, XMLSchema.NON_POSITIVE_INTEGER));
        set.add(new Pair(XMLSchema.INTEGER, XMLSchema.NON_NEGATIVE_INTEGER));
        set.add(new Pair(XMLSchema.INTEGER, XMLSchema.LONG));
        set.add(new Pair(XMLSchema.NON_POSITIVE_INTEGER, XMLSchema.NEGATIVE_INTEGER));
        set.add(new Pair(XMLSchema.NON_NEGATIVE_INTEGER, XMLSchema.POSITIVE_INTEGER));
        set.add(new Pair(XMLSchema.NON_NEGATIVE_INTEGER, XMLSchema.UNSIGNED_LONG));
        set.add(new Pair(XMLSchema.UNSIGNED_LONG, XMLSchema.UNSIGNED_INT));
        set.add(new Pair(XMLSchema.UNSIGNED_INT, XMLSchema.UNSIGNED_SHORT));
        set.add(new Pair(XMLSchema.UNSIGNED_SHORT, XMLSchema.UNSIGNED_BYTE));
        set.add(new Pair(XMLSchema.LONG, XMLSchema.INT));
        set.add(new Pair(XMLSchema.INT, XMLSchema.SHORT));
        set.add(new Pair(XMLSchema.SHORT, XMLSchema.BYTE));
        set.add(new Pair(XMLSchema.STRING, XMLSchema.NORMALIZEDSTRING));
        set.add(new Pair(XMLSchema.NORMALIZEDSTRING, XMLSchema.TOKEN));
        set.add(new Pair(XMLSchema.TOKEN, XMLSchema.LANGUAGE));
        set.add(new Pair(XMLSchema.TOKEN, XMLSchema.NAME));
        set.add(new Pair(XMLSchema.TOKEN, XMLSchema.NMTOKEN));
        set.add(new Pair(XMLSchema.NAME, XMLSchema.NCNAME));
        set.add(new Pair(XMLSchema.NCNAME, XMLSchema.ID));
        set.add(new Pair(XMLSchema.NCNAME, XMLSchema.IDREF));
        set.add(new Pair(XMLSchema.NCNAME, XMLSchema.ENTITY));

        while (true) {
            final int sizeBefore = set.size();
            final List<Pair> pairs = new ArrayList<>(set);
            for (final Pair pair1 : pairs) {
                for (final Pair pair2 : pairs) {
                    if (pair1.child.equals(pair2.parent)) {
                        set.add(new Pair(pair1.parent, pair2.child));
                    }
                }
            }
            if (set.size() == sizeBefore) {
                break;
            }
        }

        for (final IRI iri : new IRI[] { RDFS.LITERAL, RDF.XMLLITERAL, XMLSchema.DURATION,
                XMLSchema.DATETIME, XMLSchema.DATE, XMLSchema.TIME, XMLSchema.GYEARMONTH,
                XMLSchema.GMONTHDAY, XMLSchema.GYEAR, XMLSchema.GMONTH, XMLSchema.GDAY,
                XMLSchema.BOOLEAN, XMLSchema.BASE64BINARY, XMLSchema.HEXBINARY, XMLSchema.FLOAT,
                XMLSchema.DOUBLE, XMLSchema.DECIMAL, XMLSchema.ANYURI, XMLSchema.QNAME,
                XMLSchema.NOTATION, XMLSchema.STRING, XMLSchema.INTEGER,
                XMLSchema.NON_POSITIVE_INTEGER, XMLSchema.NON_NEGATIVE_INTEGER, XMLSchema.LONG,
                XMLSchema.NEGATIVE_INTEGER, XMLSchema.POSITIVE_INTEGER, XMLSchema.UNSIGNED_LONG,
                XMLSchema.UNSIGNED_INT, XMLSchema.UNSIGNED_SHORT, XMLSchema.UNSIGNED_BYTE,
                XMLSchema.INT, XMLSchema.SHORT, XMLSchema.BYTE, XMLSchema.NORMALIZEDSTRING,
                XMLSchema.TOKEN, XMLSchema.LANGUAGE, XMLSchema.NAME, XMLSchema.NMTOKEN,
                XMLSchema.NCNAME, XMLSchema.ID, XMLSchema.IDREF, XMLSchema.ENTITY, }) {
            set.add(new Pair(iri, iri));
        }

        COMPATIBILITIES = set;
    }

    @Override
    public String getURI() {
        return RR.COMPATIBLE_DATATYPE.stringValue();
    }

    @Override
    public Value evaluate(final ValueFactory factory, final Value... args)
            throws ValueExprEvaluationException {
        final IRI d1 = (IRI) args[0]; // parent
        final IRI d2 = (IRI) args[1]; // child
        final Pair pair = new Pair(d1, d2);
        return factory.createLiteral(FunctionCompatibleDatatype.COMPATIBILITIES.contains(pair));
    }

    private static final class Pair {

        public IRI parent;

        public IRI child;

        public Pair(final IRI parent, final IRI child) {
            this.parent = parent;
            this.child = child;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Pair)) {
                return false;
            }
            final Pair other = (Pair) object;
            return this.parent.equals(other.parent) && this.child.equals(other.child);
        }

        @Override
        public int hashCode() {
            return this.parent.hashCode() * 37 + this.child.hashCode();
        }

    }

}
