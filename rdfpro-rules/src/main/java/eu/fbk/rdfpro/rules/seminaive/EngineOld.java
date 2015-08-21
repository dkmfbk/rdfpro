package eu.fbk.rdfpro.rules.seminaive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.rules.Rule;
import eu.fbk.rdfpro.rules.RuleEngine;
import eu.fbk.rdfpro.rules.Ruleset;
import eu.fbk.rdfpro.rules.model.QuadModel;
import eu.fbk.rdfpro.rules.util.StatementBuffer;
import eu.fbk.rdfpro.rules.util.StatementDeduplicator;

public class EngineOld extends RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineOld.class);

    public EngineOld(final Ruleset ruleset) {
        super(ruleset);
    }

    @Override
    protected void doEval(final Collection<Statement> model) {

        // Rule evaluation is done inside a specifically-created object as some state has to be
        // kept for the whole duration of the process
        if (model instanceof QuadModel) {
            new Evaluation((QuadModel) model).eval();
        } else {
            final QuadModel quadModel = QuadModel.create(model);
            new Evaluation((QuadModel) model).eval();
            if (this.ruleset.isDeletePossible() || !(model instanceof Set<?>)) {
                model.clear();
            }
            model.addAll(quadModel);
        }
    }

    private class Evaluation {

        private final QuadModel model;

        public Evaluation(final QuadModel model) {
            // Store the model to act on
            this.model = model;
        }

        public void eval() {

            // Scan rules (which are ordered by phase, fixpoint, id) and identify the rules for
            // each phase/fixpoint combination, delegating to evalPhase() their execution
            final List<Rule> rules = new ArrayList<>();
            for (final Rule rule : EngineOld.this.ruleset.getRules()) {
                if (!rules.isEmpty() && (rule.isFixpoint() != rules.get(0).isFixpoint() //
                        || rule.getPhase() != rules.get(0).getPhase())) {
                    evalPhase(rules);
                    rules.clear();
                }
                rules.add(rule);
            }
            if (!rules.isEmpty()) {
                evalPhase(rules);
            }
        }

        private void evalPhase(final List<Rule> rules) {

            // Retrieve phase index and fixpoint flag for the current phase
            final int phase = rules.get(0).getPhase();
            final boolean fixpoint = rules.get(0).isFixpoint();

            // Log beginning
            long ts = 0;
            if (LOGGER.isDebugEnabled()) {
                ts = System.currentTimeMillis();
                LOGGER.debug("Phase {}/{} started: {} quads", phase, fixpoint, this.model.size());
            }
            if (LOGGER.isTraceEnabled()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("Phase rules (").append(rules.size()).append("):");
                for (final Rule rule : rules) {
                    builder.append(" ").append(rule.getID().getLocalName());
                }
                LOGGER.trace(builder.toString());
            }

            // Handle three case
            if (!fixpoint) {
                // (1) One-shot evaluation
                evalRules(rules);

            } else if (!allSimple(rules)) {
                // (2) Naive fixpoint evaluation
                while (true) {
                    final boolean modified = evalRules(rules);
                    if (!modified) {
                        break; // fixpoint reached
                    }
                }

            } else {
                // (3) Semi-naive fixpoint evaluation
                QuadModel delta = null;
                while (true) {
                    delta = evalRules(rules, delta);
                    if (delta.isEmpty()) {
                        break; // fixpoint reached
                    }
                }
            }

            // Log completion
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Phase {}/{} completed: {} quads, {} ms", phase, fixpoint,
                        this.model.size(), System.currentTimeMillis() - ts);
            }
        }

        private boolean evalRules(final List<Rule> rules) {

            // Take a timestamp
            final long ts1 = System.currentTimeMillis();

            // Allocate delete and insert buffers (initially empty)
            final StatementBuffer deleteBuffer = new StatementBuffer();
            final StatementBuffer insertBuffer = new StatementBuffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            final int numRules = Rule.evaluate(rules, this.model, null, deduplicate(deleteBuffer),
                    deduplicate(insertBuffer));

            // Take another timestamp and measure buffer sizes after rule evaluation
            final long ts2 = System.currentTimeMillis();
            final int deleteBufferSize = deleteBuffer.size();
            final int insertBufferSize = insertBuffer.size();

            // Allocate a buffer where to accumulate statements actually deleted. This is
            // necessary only in case there are both deletions and insertions
            final StatementBuffer deleteDelta = deleteBuffer.isEmpty() || //
                    insertBuffer.isEmpty() ? null : new StatementBuffer();
            final RDFHandler deleteCallback = deleteDelta == null ? null : deleteDelta.get();

            // Apply the modifications and keep track of model sizes
            final int size0 = this.model.size();
            deleteBuffer.toModel(this.model, false, deleteCallback);
            final int size1 = this.model.size();
            insertBuffer.toModel(this.model, true, null);
            final int size2 = this.model.size();

            // Determine whether the model has changed w.r.t. its original state.
            boolean result;
            if (size0 != size2) {
                result = true;
            } else if (size0 == size1) {
                result = false;
            } else {
                assert deleteBuffer != null; // necessarily true
                result = insertBuffer.contains(deleteBuffer);
            }

            // Take a final timestamp and log relevant statistics if enabled
            final long ts3 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} rules (out of {}) rules evaluated in {} ms ({} ms query, "
                        + "{} ms modify), {} deletions ({} buffered), {} insertions "
                        + "({} buffered), {} quads in, {} quads out", numRules, rules.size(), ts3
                        - ts1, ts2 - ts1, ts3 - ts2, size1 - size0, deleteBufferSize, size2
                        - size1, insertBufferSize, size0, size2);
            }

            // Return true if the model has changed
            return result;
        }

        private QuadModel evalRules(final List<Rule> rules, @Nullable final QuadModel delta) {

            // Take a timestamp
            final long ts1 = System.currentTimeMillis();

            // Allocate insert buffer (initially empty)
            final StatementBuffer insertBuffer = new StatementBuffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            final int numVariants = Rule.evaluate(rules, this.model, delta, null,
                    deduplicate(insertBuffer));

            // Take another timestamp and measure insert buffer size after rule evaluation
            final long ts2 = System.currentTimeMillis();
            final int insertBufferSize = insertBuffer.size();

            // Insert the quads resulting from rule evaluation, building the delta model
            final StatementBuffer deltaBuffer = new StatementBuffer();
            final int size0 = this.model.size();
            insertBuffer.toModel(this.model, true, deltaBuffer.get());
            final int size1 = this.model.size();
            final long ts3 = System.currentTimeMillis();

            // Ensure proper indexing of delta model
            final QuadModel newDelta = this.model.filter(deltaBuffer);

            // Take a final timestamp and log relevant statistics if enabled
            final long ts4 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} rule variants (for {} rules) evaluated in "
                        + "{} ms ({} ms query, {} ms modify, {} ms delta), {} insertions "
                        + "({} buffered), {} quads in, {} quads out", numVariants, rules.size(),
                        ts4 - ts1, ts2 - ts1, ts3 - ts2, ts4 - ts3, size1 - size0,
                        insertBufferSize, size0, size1);
            }

            // Return true if the model has changed
            return newDelta;
        }

        private boolean allSimple(final Iterable<Rule> rules) {
            for (final Rule rule : rules) {
                if (!rule.isSimple()) {
                    return false;
                }
            }
            return true;
        }

        private Supplier<RDFHandler> deduplicate(final StatementBuffer buffer) {
            return () -> {
                return StatementDeduplicator.newPartialDeduplicator(1 * 1024, false, false)
                        .deduplicate(buffer.get());
            };
        }

    }

}
