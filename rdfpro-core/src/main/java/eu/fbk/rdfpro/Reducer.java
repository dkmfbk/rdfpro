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

import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Scripting;

/**
 * Reduce function in a MapReduce job.
 * <p>
 * A {@code Reducer} object is used in a MapReduce job (see
 * {@link RDFProcessors#mapReduce(Mapper, Reducer, boolean)}) to process a partition of statements
 * associated to a certain {@code Value} key produced by a {@link Mapper} in a previous map phase
 * (e.g., all the statements having a specific subject).
 * </p>
 * <p>
 * Implementations of this interface should be thread-safe, as multiple reduce jobs can be fired
 * in parallel with method {@code reduce()} being invoked concurrently by different threads on
 * different statement partitions.
 * </p>
 */
@FunctionalInterface
public interface Reducer {

    /**
     * The identity reducer that emits all the quads of a partition unchanged.
     */
    Reducer IDENTITY = new Reducer() {

        @Override
        public void reduce(final Value key, final Statement[] statements, final RDFHandler handler)
                throws RDFHandlerException {
            for (final Statement statement : statements) {
                handler.handleStatement(statement);
            }
        }

    };

    /**
     * Returns a filtered version of the input reducer that operates only on partitions satisfying
     * the existential and forall predicates supplied.
     *
     * @param reducer
     *            the reducer to filter
     * @param existsPredicate
     *            the exists predicate, that must be satisfied by at least a partition quad in
     *            order for the partition to be processed; if null no existential filtering is
     *            done
     * @param forallPredicate
     *            the forall predicate, that must be satisfied by all the quads of a partition in
     *            order for it to be processed; if null, no forall filtering is applied
     * @return the resulting filtered reducer
     */
    static Reducer filter(final Reducer reducer,
            @Nullable final Predicate<Statement> existsPredicate,
            @Nullable final Predicate<Statement> forallPredicate) {

        if (existsPredicate != null) {
            if (forallPredicate != null) {
                return new Reducer() {

                    @Override
                    public void reduce(final Value key, final Statement[] statements,
                            final RDFHandler handler) throws RDFHandlerException {
                        boolean exists = false;
                        for (final Statement statement : statements) {
                            if (!forallPredicate.test(statement)) {
                                return;
                            }
                            exists = exists || existsPredicate.test(statement);
                        }
                        if (exists) {
                            reducer.reduce(key, statements, handler);
                        }
                    }

                };
            } else {
                return new Reducer() {

                    @Override
                    public void reduce(final Value key, final Statement[] statements,
                            final RDFHandler handler) throws RDFHandlerException {
                        for (final Statement statement : statements) {
                            if (existsPredicate.test(statement)) {
                                reducer.reduce(key, statements, handler);
                                return;
                            }
                        }
                    }

                };
            }
        } else {
            if (forallPredicate != null) {
                return new Reducer() {

                    @Override
                    public void reduce(final Value key, final Statement[] statements,
                            final RDFHandler handler) throws RDFHandlerException {
                        for (final Statement statement : statements) {
                            if (!forallPredicate.test(statement)) {
                                return;
                            }
                        }
                        reducer.reduce(key, statements, handler);
                    }

                };
            } else {
                return reducer;
            }
        }
    }

    /**
     * Returns a {@code Reducer} that emits the output of multiple reductions on the same quad
     * partition.
     *
     * @param reducers
     *            the reducers whose output has to be concatenated
     * @return the created {@code Reducer}
     */
    static Reducer concat(final Reducer... reducers) {
        return new Reducer() {

            @Override
            public void reduce(final Value key, final Statement[] statements,
                    final RDFHandler handler) throws RDFHandlerException {
                for (final Reducer reducer : reducers) {
                    reducer.reduce(key, statements, handler);
                }
            }

        };
    }

    /**
     * Parses a {@code Reducer} out of the supplied expression string. The expression must be a
     * {@code language: expression} script.
     *
     * @param expression
     *            the expression to parse
     * @return the parsed reducer, or null if a null expression was supplied
     */
    @Nullable
    static Mapper parse(@Nullable final String expression) {
        return expression == null ? null //
                : Scripting.compile(Mapper.class, expression, "k", "p", "h");
    }

    /**
     * Processes the statement partition associated to a certain key, emitting output statements
     * to the supplied {@code RDFHandler}.
     *
     * @param key
     *            the partition key, possibly null
     * @param statements
     *            a modifiable array with the statements belonging to the partition, not null
     * @param handler
     *            the {@code RDFHandler} where to emit output statements, not null
     * @throws RDFHandlerException
     *             on error
     */
    void reduce(@Nullable Value key, Statement[] statements, RDFHandler handler)
            throws RDFHandlerException;

}