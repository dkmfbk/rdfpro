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
package eu.fbk.rdfpro;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;

/**
 * Rule engine abstraction.
 * <p>
 * Implementation note: concrete rule engine implementations should extend this abstract class and
 * implement one or both methods {@link #doEval(Collection)} and
 * {@link #doEval(RDFHandler, boolean)}.
 * </p>
 */
public abstract class RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuleEngine.class);

    private static final String IMPLEMENTATION = Environment
            .getProperty("rdfpro.rules.implementation", "eu.fbk.rdfpro.RuleEngineImpl");

    // private static final String IMPLEMENTATION = Environment.getProperty(
    // "rdfpro.rules.implementation", "eu.fbk.rdfpro.RuleEngineDrools");

    private final Ruleset ruleset;

    /**
     * Creates a new {@code RuleEngine} using the {@code Ruleset} specified. The ruleset must not
     * contain unsafe rules.
     *
     * @param ruleset
     *            the ruleset, not null and without unsafe rules
     */
    protected RuleEngine(final Ruleset ruleset) {

        // Check the input ruleset
        Objects.requireNonNull(ruleset);
        for (final Rule rule : ruleset.getRules()) {
            if (!rule.isSafe()) {
                throw new IllegalArgumentException("Ruleset contains unsafe rule " + rule);
            }
        }

        // Store the ruleset
        this.ruleset = ruleset;
    }

    /**
     * Factory method for creating a new {@code RuleEngine} using the {@code Ruleset} specified.
     * The ruleset must not contain unsafe rules. The engine implementation instantiated is based
     * on the value of configuration property {@code rdfpro.rules.implementation}, which contains
     * the qualified name of a concrete class extending abstract class {@code RuleEngine}.
     *
     * @param ruleset
     *            the ruleset, not null and without unsafe rules
     * @return the created rule engine
     */
    public static RuleEngine create(final Ruleset ruleset) {

        // Check parameters
        Objects.requireNonNull(ruleset);

        try {
            // Log the operation
            if (RuleEngine.LOGGER.isTraceEnabled()) {
                RuleEngine.LOGGER.trace("Creating '{}' engine with ruleset:\n{}\n",
                        RuleEngine.IMPLEMENTATION, ruleset);
            }

            // Locate the RuleEngine constructor to be used
            final Class<?> clazz = Class.forName(RuleEngine.IMPLEMENTATION);
            final Constructor<?> constructor = clazz.getConstructor(Ruleset.class);

            // Instantiate the engine via reflection
            return (RuleEngine) constructor.newInstance(ruleset);

        } catch (final IllegalAccessException | ClassNotFoundException | NoSuchMethodException
                | InstantiationException ex) {
            // Configuration is wrong
            throw new Error("Illegal rule engine implementation: " + RuleEngine.IMPLEMENTATION,
                    ex);

        } catch (final InvocationTargetException ex) {
            // Configuration is ok, but the RuleEngine cannot be created
            Throwables.throwIfUnchecked(ex.getCause());
            throw new RuntimeException(ex.getCause());
        }
    }

    /**
     * Returns the ruleset applied by this engine
     *
     * @return the ruleset
     */
    public final Ruleset getRuleset() {
        return this.ruleset;
    }

    /**
     * Evaluates rules on the {@code QuadModel} specified.
     *
     * @param model
     *            the model the engine will operate on
     */
    public final void eval(final Collection<Statement> model) {

        // Check parameters
        Objects.requireNonNull(model);

        // Handle two cases, respectively with/without logging information emitted
        if (!RuleEngine.LOGGER.isDebugEnabled()) {

            // Logging disabled: directly forward to doEval()
            this.doEval(model);

        } else {

            // Logging enabled: log relevant info before and after forwarding to doEval()
            final long ts = System.currentTimeMillis();
            final int inputSize = model.size();
            RuleEngine.LOGGER.debug(
                    "Rule evaluation started: {} input statements, {} rule(s), model input",
                    inputSize, this.ruleset.getRules().size());
            this.doEval(model);
            RuleEngine.LOGGER.debug(
                    "Rule evaluation completed: {} input statements, {} output statements, {} ms",
                    inputSize, model.size(), System.currentTimeMillis() - ts);
        }
    }

    /**
     * Evaluates rules in streaming mode, emitting resulting statements to the {@code RDFHandler}
     * supplied.
     *
     * @param handler
     *            the handler where to emit resulting statements
     * @param deduplicate
     *            true if the output should not contain duplicate statements
     * @return an {@code RDFHandler} where input statements can be streamed into
     */
    public final RDFHandler eval(final RDFHandler handler, final boolean deduplicate) {

        // Check parameters
        Objects.requireNonNull(handler);

        // Handle two cases, respectively with/without logging information emitted
        if (!RuleEngine.LOGGER.isDebugEnabled()) {

            // Logging disabled: delegate to doEval(), filtering out non-matchable quads
            return this.doEval(handler, deduplicate);

        } else {

            // Logging enabled: allocate counters to track quads in (processed/propagated) and out
            final AtomicInteger numProcessed = new AtomicInteger(0);
            final AtomicInteger numOut = new AtomicInteger(0);

            // Wrap sink handler to count out quads
            final RDFHandler sink = new AbstractRDFHandlerWrapper(handler) {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    super.handleStatement(statement);
                    numOut.incrementAndGet();
                }

            };

            // Delegate to doEval(), wrapping the returned handler to perform logging and filter
            // out non-matchable quads
            return new AbstractRDFHandlerWrapper(this.doEval(sink, deduplicate)) {

                private long ts;

                @Override
                public void startRDF() throws RDFHandlerException {
                    this.ts = System.currentTimeMillis();
                    numProcessed.set(0);
                    numOut.set(0);
                    RuleEngine.LOGGER.debug("Rule evaluation started: {} rule(s), stream input",
                            RuleEngine.this.ruleset.getRules().size());
                    super.startRDF();
                }

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {
                    super.handleStatement(stmt);
                    numProcessed.incrementAndGet();
                }

                @Override
                public void endRDF() throws RDFHandlerException {
                    super.endRDF();
                    RuleEngine.LOGGER.debug(
                            "Rule evaluation completed: {} input statements, "
                                    + "{} output statements , {} ms",
                            numProcessed.get(), numOut.get(),
                            System.currentTimeMillis() - this.ts);
                }

            };
        }
    }

    /**
     * Internal method called by {@link #eval(Collection)}. Its base implementation delegates to
     * {@link #doEval(RDFHandler, boolean)}.
     *
     * @param model
     *            the model to operate on
     */
    protected void doEval(final Collection<Statement> model) {

        // Delegate to doEval(RDFHandler), handling two cases for performance reasons
        if (!this.ruleset.isDeletePossible()
                && (model instanceof QuadModel || model instanceof Set<?>)) {

            // Optimized version that adds inferred statement back to the supplied model, relying
            // on the fact that no statement can be possibly deleted
            final List<Statement> inputStmts = new ArrayList<>(model);
            final RDFHandler handler = this.doEval(RDFHandlers
                    .decouple(RDFHandlers.wrap(Collections.synchronizedCollection(model))), false);
            try {
                handler.startRDF();
                for (final Statement stmt : inputStmts) {
                    handler.handleStatement(stmt);
                }
                handler.endRDF();
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            } finally {
                IO.closeQuietly(handler);
            }

        } else {

            // General implementation that stores resulting statement in a list, and then clears
            // the input model and loads those statement (this will also take into consideration
            // possible deletions)
            final List<Statement> outputStmts = new ArrayList<>();
            final RDFHandler handler = this.doEval(
                    RDFHandlers.decouple(
                            RDFHandlers.wrap(Collections.synchronizedCollection(outputStmts))),
                    true);
            try {
                handler.startRDF();
                for (final Statement stmt : model) {
                    handler.handleStatement(stmt);
                }
                handler.endRDF();
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            } finally {
                IO.closeQuietly(handler);
            }
            model.clear();
            for (final Statement stmt : outputStmts) {
                model.add(stmt);
            }
        }
    }

    /**
     * Internal method called by {@link #eval(RDFHandler, boolean)}. Its base implementation
     * delegates to {@link #doEval(Collection)}.
     *
     * @param handler
     *            the handler where to emit resulting statements
     * @param deduplicate
     *            true if output should not contain duplicate statements
     * @return an handler accepting input statements
     */
    protected RDFHandler doEval(final RDFHandler handler, final boolean deduplicate) {

        // Return an RDFHandler that delegates to doEval(QuadModel)
        return new AbstractRDFHandlerWrapper(handler) {

            private QuadModel model;

            @Override
            public void startRDF() throws RDFHandlerException {
                super.startRDF();
                this.model = QuadModel.create();
            }

            @Override
            public synchronized void handleStatement(final Statement stmt)
                    throws RDFHandlerException {
                this.model.add(stmt);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                RuleEngine.this.doEval(this.model);
                for (final Statement stmt : this.model) {
                    super.handleStatement(stmt);
                }
                this.model = null; // free memory
                super.endRDF();
            }

        };
    }

}
