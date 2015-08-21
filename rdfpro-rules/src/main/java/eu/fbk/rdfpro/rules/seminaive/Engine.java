package eu.fbk.rdfpro.rules.seminaive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.rules.Rule;
import eu.fbk.rdfpro.rules.RuleEngine;
import eu.fbk.rdfpro.rules.Ruleset;
import eu.fbk.rdfpro.rules.model.QuadModel;
import eu.fbk.rdfpro.rules.util.StatementBuffer;
import eu.fbk.rdfpro.rules.util.StatementDeduplicator;
import eu.fbk.rdfpro.rules.util.StatementMatcher;
import eu.fbk.rdfpro.rules.util.StatementTemplate;
import eu.fbk.rdfpro.util.Statements;

public class Engine extends RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(Engine.class);

    public Engine(final Ruleset ruleset) {
        super(ruleset);
    }

    @Override
    protected void doEval(final Collection<Statement> model) {
        // TODO Auto-generated method stub
        super.doEval(model);
    }

    @Override
    protected RDFHandler doEval(final RDFHandler handler) {
        // TODO Auto-generated method stub
        return super.doEval(handler);
    }

    private static void expand(final Statement stmt, final RDFHandler sink,
            final StatementDeduplicator deduplicator,
            @Nullable final StatementMatcher deleteMatcher,
            @Nullable final StatementMatcher insertMatcher, final boolean fixpoint)
            throws RDFHandlerException {

        // If statement was already seen, discard it
        if (!deduplicator.isNew(stmt)) {
            return;
        }

        // Otherwise, emit the statement only if it does not match a delete pattern
        if (deleteMatcher == null
                || !deleteMatcher.match(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                        stmt.getContext())) {
            sink.handleStatement(stmt);
        }

        // Apply insert part by looking up and applying insert templates. Recursion occurs in
        // case of fixpoint expansion; otherwise, inferred statements are directly emitted if
        // they might be new
        if (insertMatcher != null) {
            for (final StatementTemplate template : insertMatcher.map(stmt.getSubject(),
                    stmt.getPredicate(), stmt.getObject(), stmt.getContext(),
                    StatementTemplate.class)) {
                final Statement stmt2 = template.apply(stmt);
                if (stmt2 != null) {
                    if (fixpoint) {
                        expand(stmt2, sink, deduplicator, deleteMatcher, insertMatcher, true);
                    } else if (!deduplicator.isNew(stmt2)) {
                        sink.handleStatement(stmt2);
                    }
                }
            }
        }
    }

    private static Statement[] normalize(Statement[] statements,
            final Function<Object, Object> normalizer) {

        if (normalizer != null) {
            statements = statements.clone();
            for (int i = 0; i < statements.length; ++i) {
                final Statement s = statements[i];
                statements[i] = Statements.VALUE_FACTORY.createStatement(
                        (Resource) normalizer.apply(s.getSubject()),
                        (URI) normalizer.apply(s.getPredicate()),
                        (Value) normalizer.apply(s.getObject()),
                        (Resource) normalizer.apply(s.getContext()));
            }
        }
        return statements;
    }

    private static Supplier<RDFHandler> deduplicate(final StatementBuffer buffer) {
        return () -> {
            return StatementDeduplicator.newPartialDeduplicator(1 * 1024, false, false)
                    .deduplicate(buffer.get());
        };
    }

    private static abstract class Phase {

        private final boolean handlerSupported;

        private final boolean modelSupported;

        Phase(final boolean handlerSupported, final boolean modelSupported) {
            this.handlerSupported = handlerSupported;
            this.modelSupported = modelSupported;
        }

        public Phase normalize(final Function<Object, Object> normalizer) {
            return this;
        }

        public RDFHandler eval(final RDFHandler handler) {
            throw new Error();
        }

        public void eval(final QuadModel model) {
            throw new Error();
        }

        public final boolean isHandlerSupported() {
            return this.handlerSupported;
        }

        public final boolean isModelSupported() {
            return this.modelSupported;
        }

    }

    private static final class StreamPhase extends Phase {

        @Nullable
        private final StatementMatcher deleteMatcher;

        @Nullable
        private final StatementMatcher insertMatcher;

        private final Statement[] axioms;

        private final boolean fixpoint;

        private StreamPhase(@Nullable final StatementMatcher deleteMatcher,
                @Nullable final StatementMatcher insertMatcher, final Statement[] axioms,
                final boolean fixpoint) {

            // We only work in streaming
            super(true, false);

            // Store supplied structures
            this.deleteMatcher = deleteMatcher;
            this.insertMatcher = insertMatcher;
            this.axioms = axioms;
            this.fixpoint = fixpoint;
        }

        static StreamPhase create(final Iterable<Rule> rules) {

            // Allocate builders for the two matchers and an empty axiom list
            StatementMatcher.Builder db = null;
            StatementMatcher.Builder ib = null;
            final List<Statement> axioms = new ArrayList<>();
            boolean containsFixpointRule = false;
            boolean containsNonFixpointRule = false;

            // Populate matchers by iterating over supplied rules
            for (final Rule rule : rules) {
                assert rule.isSafe() && rule.isStreamable();
                containsFixpointRule |= rule.isFixpoint();
                containsNonFixpointRule |= !rule.isFixpoint();
                if (!rule.getWherePatterns().isEmpty()) {
                    final StatementPattern wp = rule.getWherePatterns().iterator().next();
                    for (final StatementPattern ip : rule.getInsertPatterns()) {
                        ib = ib != null ? ib : StatementMatcher.builder();
                        ib.addPattern(wp, new StatementTemplate(wp, ip));
                    }
                    if (!rule.getDeletePatterns().isEmpty()) {
                        db = db != null ? db : StatementMatcher.builder();
                        db.addPattern(wp);
                    }
                } else {
                    for (final StatementPattern ip : rule.getInsertPatterns()) {
                        final Value subj = ip.getSubjectVar().getValue();
                        final Value pred = ip.getPredicateVar().getValue();
                        final Value obj = ip.getObjectVar().getValue();
                        final Value ctx = ip.getContextVar() == null ? null : ip.getContextVar()
                                .getValue();
                        if (subj instanceof Resource && pred instanceof URI
                                && (ctx == null || ctx instanceof Resource)) {
                            axioms.add(Statements.VALUE_FACTORY.createStatement((Resource) subj,
                                    (URI) pred, obj, (Resource) ctx));
                        }
                    }
                }
            }
            assert containsFixpointRule ^ containsNonFixpointRule;

            // Create a new StreamPhase object, using null when there are no deletions or
            // insertions possible, and determining whether fixpoint evaluation should occur
            return new StreamPhase(db == null ? null : db.build(null), ib == null ? null
                    : ib.build(null), axioms.toArray(new Statement[axioms.size()]),
                    containsFixpointRule);
        }

        @Override
        public Phase normalize(final Function<Object, Object> normalizer) {

            // Normalize delete matchers and insert matchers with associated templates
            final Function<Object, Object> matcherNormalizer = (final Object object) -> {
                if (object instanceof StatementTemplate) {
                    return ((StatementTemplate) object).normalize(normalizer);
                } else {
                    return normalizer.apply(object);
                }
            };
            final StatementMatcher normalizedDeleteMatcher = this.deleteMatcher == null ? null
                    : this.deleteMatcher.normalize(matcherNormalizer);
            final StatementMatcher normalizedInsertMatcher = this.insertMatcher == null ? null
                    : this.insertMatcher.normalize(matcherNormalizer);

            // Normalize axioms
            final Statement[] normalizedAxioms = Engine.normalize(this.axioms, normalizer);

            // Return a normalized copy of this phase object
            return new StreamPhase(normalizedDeleteMatcher, normalizedInsertMatcher,
                    normalizedAxioms, this.fixpoint);
        }

        @Override
        public RDFHandler eval(final RDFHandler handler) {
            return new AbstractRDFHandlerWrapper(handler) {

                private final StatementDeduplicator deduplicator = StatementDeduplicator
                        .newPartialDeduplicator(6, false, true);

                @Override
                public void startRDF() throws RDFHandlerException {

                    // Delegate
                    super.startRDF();

                    // Emit axioms
                    for (final Statement axiom : StreamPhase.this.axioms) {
                        expand(axiom, this.handler, this.deduplicator,
                                StreamPhase.this.fixpoint ? StreamPhase.this.deleteMatcher : null,
                                StreamPhase.this.insertMatcher, StreamPhase.this.fixpoint);
                    }
                }

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {

                    // Delegate to recursive method expand(), marking the statement as explicit
                    expand(stmt, this.handler, this.deduplicator, StreamPhase.this.deleteMatcher,
                            StreamPhase.this.insertMatcher, StreamPhase.this.fixpoint);
                }

            };
        }
    }

    private static final class NaivePhase extends Phase {

        private final List<Rule> rules;

        private final boolean fixpoint;

        private NaivePhase(final List<Rule> rules, final boolean fixpoint) {
            super(false, true);
            this.rules = rules;
            this.fixpoint = fixpoint;
        }

        static NaivePhase create(final Iterable<Rule> rules) {
            return new NaivePhase(ImmutableList.copyOf(rules), rules.iterator().next()
                    .isFixpoint());
        }

        @Override
        public void eval(final QuadModel model) {

            if (!this.fixpoint) {
                // (1) One-shot evaluation
                evalRules(model);

            } else {
                // (2) Naive fixpoint evaluation
                while (true) {
                    final boolean modified = evalRules(model);
                    if (!modified) {
                        break; // fixpoint reached
                    }
                }
            }
        }

        private boolean evalRules(final QuadModel model) {

            // Take a timestamp
            final long ts1 = System.currentTimeMillis();

            // Allocate delete and insert buffers (initially empty)
            final StatementBuffer deleteBuffer = new StatementBuffer();
            final StatementBuffer insertBuffer = new StatementBuffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            final int numRules = Rule.evaluate(this.rules, model, null, deduplicate(deleteBuffer),
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

            // Apply the modifications resulting from rule evaluation, tracking the model sizes. A
            // side result of this operation is that the two buffers are deduplicated (this is
            // essential for determining if the model changed or not)
            final int size0 = model.size();
            deleteBuffer.toModel(model, false, deleteCallback);
            final int size1 = model.size();
            insertBuffer.toModel(model, true, null);
            final int size2 = model.size();

            // Determine whether the model has changed w.r.t. its original state. This is done by
            // first comparing the different model sizes and as a last resort by comparing the two
            // delete and insert buffers to see if they are equal (=> model unchanged)
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
                        + "({} buffered), {} quads in, {} quads out", numRules, this.rules.size(),
                        ts3 - ts1, ts2 - ts1, ts3 - ts2, size1 - size0, deleteBufferSize, size2
                                - size1, insertBufferSize, size0, size2);
            }

            // Return true if the model has changed
            return result;
        }

    }

    private static final class SemiNaivePhase extends Phase {

        private final List<Rule> allRules;

        private List<Rule> joinRules;

        private final StatementMatcher streamMatcher;

        private final StatementMatcher joinMatcher;

        private final Statement[] axioms;

        private final boolean fixpoint;

        private SemiNaivePhase(final List<Rule> rules, final StatementMatcher streamMatcher,
                final StatementMatcher modelMatcher, final Statement[] axioms,
                final boolean fixpoint) {

            super(streamMatcher != null, true);
            this.allRules = rules;
            this.streamMatcher = streamMatcher;
            this.joinMatcher = modelMatcher;
            this.axioms = axioms;
            this.fixpoint = fixpoint;
        }

        public static SemiNaivePhase create(final Iterable<Rule> rules) {
            // TODO
            return null;
        }

        @Override
        public Phase normalize(final Function<Object, Object> normalizer) {

            final StatementMatcher normStreamMatcher = this.streamMatcher.normalize(normalizer);
            final StatementMatcher normModelMatcher = this.joinMatcher.normalize(normalizer);
            final Statement[] normAxioms = Engine.normalize(this.axioms, normalizer);

            if (normStreamMatcher == this.streamMatcher && normModelMatcher == this.joinMatcher
                    && normAxioms == this.axioms) {
                return this;
            } else {
                return new SemiNaivePhase(this.allRules, normStreamMatcher, normModelMatcher,
                        normAxioms, this.fixpoint);
            }
        }

        @Override
        public RDFHandler eval(final RDFHandler handler) {
            return new AbstractRDFHandlerWrapper(handler) {

                private QuadModel joinModel;

                private StatementDeduplicator deduplicator;

                private RDFHandler sink;

                @Override
                public void startRDF() throws RDFHandlerException {

                    // Delegate
                    super.startRDF();

                    // Allocate a model for statements matching join rule WHERE patterns
                    this.joinModel = QuadModel.create();

                    // Allocate a deduplicator
                    this.deduplicator = StatementDeduplicator.newPartialDeduplicator(6, false,
                            true);

                    // Allocate a sink that accumulates statements matching join rules in the
                    // model, emitting other statements
                    this.sink = new AbstractRDFHandlerWrapper(this.handler) {

                        @Override
                        public void handleStatement(final Statement stmt)
                                throws RDFHandlerException {
                            if (SemiNaivePhase.this.joinMatcher.match(stmt)) {
                                joinModel.add(stmt);
                            } else {
                                this.handler.handleStatement(stmt);
                            }
                        }

                    };

                    // Emit axioms, accumulating statements matching join patterns in the model
                    for (final Statement axiom : SemiNaivePhase.this.axioms) {
                        expand(axiom, this.sink, this.deduplicator, null,
                                SemiNaivePhase.this.streamMatcher, SemiNaivePhase.this.fixpoint);
                    }
                }

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {

                    // Delegate to recursive method expand(), marking the statement as explicit
                    expand(stmt, this.sink, this.deduplicator, null,
                            SemiNaivePhase.this.streamMatcher, SemiNaivePhase.this.fixpoint);
                }

                @Override
                public void endRDF() throws RDFHandlerException {
                    // TODO Auto-generated method stub
                    super.endRDF();
                }

            };
        }

        @Override
        public void eval(final QuadModel model) {

            // Handle three case
            if (!this.fixpoint) {
                // (1) One-shot evaluation
                evalRules(model, null);

            } else {
                // (2) Semi-naive fixpoint evaluation
                QuadModel delta = null;
                while (true) {
                    delta = evalRules(model, delta);
                    if (delta.isEmpty()) {
                        break; // fixpoint reached
                    }
                }
            }
        }

        private QuadModel evalRules(final QuadModel model, @Nullable final QuadModel delta) {

            // Take a timestamp
            final long ts1 = System.currentTimeMillis();

            // Allocate insert buffer (initially empty)
            final StatementBuffer insertBuffer = new StatementBuffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            final int numVariants = Rule.evaluate(this.allRules, model, delta, null,
                    deduplicate(insertBuffer));

            // Take another timestamp and measure insert buffer size after rule evaluation
            final long ts2 = System.currentTimeMillis();
            final int insertBufferSize = insertBuffer.size();

            // Insert the quads resulting from rule evaluation.
            final StatementBuffer deltaBuffer = new StatementBuffer();
            final int size0 = model.size();
            insertBuffer.toModel(model, true, deltaBuffer.get());
            final int size1 = model.size();
            final long ts3 = System.currentTimeMillis();

            // Compute new delta model
            final QuadModel newDelta = model.filter(deltaBuffer);

            // Take a final timestamp and log relevant statistics if enabled
            final long ts4 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} rule variants (out of {} variants of {} rules) evaluated in "
                        + "{} ms ({} ms query, {} ms modify, {} ms delta), {} insertions "
                        + "({} buffered), {} quads in, {} quads out", numVariants,
                        this.allRules.size(), ts4 - ts1, ts2 - ts1, ts3 - ts2, ts4 - ts3, size1
                                - size0, insertBufferSize, size0, size1);
            }

            // Return true if the model has changed
            return newDelta;
        }

    }

}
