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
import java.util.List;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.vocab.RR;

public class FunctionMint implements Function {

    private static final String DEFAULT_NS = "urn:hash:";

    @Override
    public String getURI() {
        return RR.MINT.stringValue();
    }

    @Override
    public Value evaluate(final ValueFactory factory, final Value... args)
            throws ValueExprEvaluationException {

        String ns = null;
        String name = null;
        final List<String> strings = new ArrayList<>(8);

        for (final Value arg : args) {
            if (arg instanceof URI) {
                final URI uri = (URI) arg;
                final String string = uri.stringValue();
                strings.add("\u0001");
                strings.add(string);
                if (ns == null) {
                    ns = uri.getNamespace();
                }
                if (name == null) {
                    final char ch = string.charAt(string.length() - 1);
                    if (ch != ':' && ch != '/' && ch != '#') {
                        name = uri.getLocalName();
                    }
                }

            } else if (arg instanceof BNode) {
                final BNode bnode = (BNode) arg;
                strings.add("\u0002");
                strings.add(bnode.getID());
                if (name == null) {
                    name = bnode.getID();
                }

            } else {
                final Literal l = (Literal) arg;
                if (l.getLanguage() != null) {
                    strings.add("\u0003");
                    strings.add(l.getLanguage());
                } else if (l.getDatatype() != null) {
                    strings.add("\u0004");
                    strings.add(l.getDatatype().stringValue());
                } else {
                    strings.add("\u0005");
                }
                strings.add(l.getLabel());
                if (name == null) {
                    name = l.getLabel();
                }
            }
        }

        final Hash hash = Hash.murmur3(strings.toArray(new String[strings.size()]));

        final StringBuilder builder = new StringBuilder();
        builder.append(ns != null ? ns : DEFAULT_NS);
        if (name != null) {
            builder.append(name.replaceAll("\\s+", "_")).append('_');
        }
        builder.append(hash.toString());

        return factory.createURI(builder.toString());
    }

}
