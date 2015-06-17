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

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.Statements;

/**
 * Mapper function in a MapReduce job.
 * <p>
 * A {@code Mapper} object is used in a MapReduce job (see
 * {@link RDFProcessors#mapReduce(Mapper, Reducer, boolean)}) for mapping a statement to zero or
 * more {@code Value keys}; statements are then grouped by key and each partition is processed in
 * a reduce job (see {@link Reducer}) to produce the final output statements. Mapping is done by
 * method {@link #map(Statement)}. It can return zero keys to drop the statement, or a key array
 * including {@link #BYPASS_KEY} to force the statement to be directly emitted in output,
 * bypassing the reduce stage.
 * </p>
 * <p>
 * A common type of {@code Mapper} is the one extracting a component or hashing a subset of
 * components of the input statement. This mapper is already implemented and can be instantiated
 * using the factory method {@link #select(String)}.
 * </p>
 * <p>
 * Implementations of this interface should be thread-safe, as method {@code map()} is meant to be
 * invoked concurrently by different threads on different statements.
 * </p>
 */
@FunctionalInterface
public interface Mapper {

    /** Special key used to bypass the reduce stage and directly emit the statement in output. */
    URI BYPASS_KEY = new URIImpl("rdfpro:bypass");

    /**
     * Maps a statement to zero or more {@code Value} keys. When used in a MapReduce job,
     * returning zero keys has the effect of dropping the statements, otherwise the statement will
     * be put in each key partition, each one being later processed in a reduce job (see
     * {@link Reducer}); if one of the keys is {@link #BYPASS_KEY}, then the statement will be
     * (also) directly emitted in output, bypassing the reduce stage (this can be used to apply
     * the MapReduce paradigm to only a subset of statements). Null keys are admissible in output
     * (e.g., to denote the default context).
     *
     * @param statement
     *            the statement to map, not null
     * @return an array with zero or more {@code Value} keys associated to the statement, not null
     * @throws RDFHandlerException
     *             on error
     */
    Value[] map(Statement statement) throws RDFHandlerException;

    /**
     * Returns a {@code Mapper} returning a concatenation of all the keys produced by the
     * {@code Mapper}s supplied for the input statement. Duplicate keys for the same statement are
     * merged.
     *
     * @param mappers
     *            the mappers whose output has to be concatenated
     * @return the created {@code Mapper}
     */
    public static Mapper concat(final Mapper... mappers) {
        return new Mapper() {

            @Override
            public Value[] map(final Statement statement) throws RDFHandlerException {
                final List<Value> keys = new ArrayList<>(mappers.length);
                for (int i = 0; i < mappers.length; ++i) {
                    for (final Value key : mappers[i].map(statement)) {
                        if (!keys.contains(key)) {
                            keys.add(key);
                        }
                    }
                }
                return keys.toArray(new Value[keys.size()]);
            }

        };
    }

    /**
     * Returns a {@code Mapper} returning a single key based on one or more selected components of
     * the input statement. Parameter {@code components} is a {@code s} , {@code p}, {@code o},
     * {@code c} string specifying which components should be selected. If a single component is
     * selected it is returned as the key unchanged. If multiple components are selected, they are
     * merged and hashed to produce the returned key. In any case, exactly one key is returned for
     * each input statement.
     *
     * @param components
     *            a string of symbols {@code s} , {@code p}, {@code o}, {@code c} specifying which
     *            components to select, not null nor empty
     * @return the created {@code Mapper}
     */
    public static Mapper select(final String components) {

        final String comp = components.trim().toLowerCase();

        int num = 0;
        for (int i = 0; i < comp.length(); ++i) {
            final char c = comp.charAt(i);
            final int b = c == 's' ? 0x8 : c == 'p' ? 0x4 : c == 'o' ? 0x2 : c == 'c' ? 0x1 : -1;
            if (b < 0 || (num & b) != 0) {
                throw new IllegalArgumentException("Invalid components '" + components + "'");
            }
            num = num | b;
        }
        final int mask = num;

        if (mask == 0x08 || mask == 0x04 || mask == 0x02 || mask == 0x01) {
            return new Mapper() {

                @Override
                public Value[] map(final Statement statement) throws RDFHandlerException {
                    switch (mask) {
                    case 0x08:
                        return new Value[] { statement.getSubject() };
                    case 0x04:
                        return new Value[] { statement.getPredicate() };
                    case 0x02:
                        return new Value[] { statement.getObject() };
                    case 0x01:
                        return new Value[] { statement.getContext() };
                    default:
                        throw new Error();
                    }
                }

            };
        }

        return new Mapper() {

            private final boolean hasSubj = (mask & 0x80) != 0;

            private final boolean hasPred = (mask & 0x40) != 0;

            private final boolean hasObj = (mask & 0x20) != 0;

            private final boolean hasCtx = (mask & 0x10) != 0;

            @Override
            public Value[] map(final Statement statement) throws RDFHandlerException {

                int header = 0;
                int count = 0;

                if (hasSubj) {
                    final int bits = classify(statement.getSubject());
                    header |= bits << 24;
                    count += bits & 0xF;
                }
                if (hasPred) {
                    final int bits = classify(statement.getPredicate());
                    header |= bits << 16;
                    count += bits & 0xF;
                }
                if (hasObj) {
                    final int bits = classify(statement.getObject());
                    header |= bits << 8;
                    count += bits & 0xF;
                }
                if (hasCtx) {
                    final int bits = classify(statement.getContext());
                    header |= bits;
                    count += bits & 0xF;
                }

                final String[] strings = new String[count];
                int index = 0;
                strings[index++] = Integer.toString(header);
                if (hasSubj) {
                    index = add(strings, index, statement.getSubject());
                }
                if (hasPred) {
                    index = add(strings, index, statement.getPredicate());
                }
                if (hasObj) {
                    index = add(strings, index, statement.getObject());
                }
                if (hasCtx) {
                    index = add(strings, index, statement.getContext());
                }

                final String hash = Hash.murmur3(strings).toString();
                return new Value[] { Statements.VALUE_FACTORY.createBNode(hash) };
            }

            private int classify(final Value value) {
                if (value == null) {
                    return 0;
                } else if (value instanceof BNode) {
                    return 0x11;
                } else if (value instanceof URI) {
                    return 0x21;
                }
                final Literal l = (Literal) value;
                if (l.getLanguage() != null) {
                    return 0x52;
                } else if (l.getDatatype() != null) {
                    return 0x42;
                } else {
                    return 0x31;
                }
            }

            private int add(final String[] strings, int index, final Value value) {
                if (value instanceof URI || value instanceof BNode) {
                    strings[index++] = value.stringValue();
                } else if (value instanceof Literal) {
                    final Literal l = (Literal) value;
                    strings[index++] = l.getLabel();
                    if (l.getLanguage() != null) {
                        strings[index++] = l.getLanguage();
                    } else if (l.getDatatype() != null) {
                        strings[index++] = l.getDatatype().stringValue();
                    }
                }
                return index;
            }

        };
    }

}