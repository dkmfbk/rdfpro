package eu.fbk.rdfpro.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.rules.util.QuadModel;
import eu.fbk.rdfpro.rules.util.StatementBuffer;
import eu.fbk.rdfpro.rules.util.StatementMatcher;
import eu.fbk.rdfpro.rules.util.StatementTemplate;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.StatementDeduplicator;
import eu.fbk.rdfpro.util.StatementDeduplicator.ComparisonMethod;
import eu.fbk.rdfpro.util.Statements;

public class QueryRuleEngine extends RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryRuleEngine.class);

    private static final int DEDUPLICATION_CACHE_SIZE = 16 * 1024;

    private static final boolean FORCE_DEDUPLICATION = Boolean.parseBoolean(Environment
            .getProperty("rdfpro.rules.deduplication", "false"));

    private static final boolean ENABLE_STREAMING = Boolean.parseBoolean(Environment.getProperty(
            "rdfpro.rules.streaming", "true"));

    private static final boolean ENABLE_SEMINAIVE = Boolean.parseBoolean(Environment.getProperty(
            "rdfpro.rules.seminaive", "true"));

    private final List<Phase> phases;

    private final boolean unique;

    public QueryRuleEngine(final Ruleset ruleset) {
        super(ruleset);
        this.phases = buildPhases(ruleset);

        boolean unique = false;
        for (final Phase phase : this.phases) {
            unique = phase.isHandlerOutputUnique(unique);
        }
        this.unique = unique;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("SN+ rule engine (");
        for (final Phase phase : this.phases) {
            if (phase instanceof StreamPhase) {
                builder.append('X');
            } else if (phase instanceof NaivePhase) {
                builder.append('N');
            } else if (phase instanceof SemiNaivePhase) {
                builder.append('S');
            } else {
                builder.append('?');
            }
        }
        builder.append(this.unique ? "*" : "");
        builder.append(')');
        return builder.toString();
    }

    @Override
    protected void doEval(final Collection<Statement> model) {

        final QuadModel quadModel = model instanceof QuadModel ? (QuadModel) model //
                : QuadModel.create(model);
        for (final Phase phase : this.phases) {
            phase.normalize(quadModel.getValueNormalizer()).eval(quadModel);
        }
        if (model != quadModel) {
            if (getRuleset().isDeletePossible() || !(model instanceof Set<?>)) {
                model.clear();
            }
            model.addAll(quadModel);
        }
    }

    @Override
    protected RDFHandler doEval(final RDFHandler handler, final boolean deduplicate) {

        // Determine the phase index range [i, j] (i,j included) where evaluation should be done
        // on a fully indexed model (i for necessity, up to j for necessity or convenience)
        int i = 0;
        int j = this.phases.size() - 1;
        while (i < this.phases.size() && this.phases.get(i).isHandlerSupported()) {
            ++i;
        }
        while (j > i && this.phases.get(j).isHandlerSupported()
                && !this.phases.get(j).isModelSupported()) {
            --j;
        }

        // Build the handler chain for [j+1, #phases-1]
        RDFHandler result = handler;
        for (int k = this.phases.size() - 1; k > j; --k) {
            result = this.phases.get(k).eval(result,
                    deduplicate && !this.unique && k == this.phases.size() - 1);
        }

        // Build the handler for interval [i, j], if non empty
        if (i <= j) {
            final List<Phase> modelPhases = this.phases.subList(i, j + 1);
            result = RDFHandlers.decouple(new AbstractRDFHandlerWrapper(result) {

                private long ts;

                private QuadModel model;

                @Override
                public void startRDF() throws RDFHandlerException {
                    super.startRDF();
                    this.ts = System.currentTimeMillis();
                    this.model = QuadModel.create();
                }

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {
                    this.model.add(stmt);
                }

                @Override
                public void endRDF() throws RDFHandlerException {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Model populated in {} ms, {} statements",
                                System.currentTimeMillis() - this.ts, this.model.size());
                    }
                    for (final Phase phase : modelPhases) {
                        phase.normalize(this.model.getValueNormalizer()).eval(this.model);
                    }
                    for (final Statement stmt : this.model) {
                        super.handleStatement(stmt);
                    }
                    super.endRDF();
                }

            }, 1);
        }

        // Build the handler chain for [0, i-1]
        for (int k = i - 1; k >= 0; --k) {
            result = this.phases.get(k).eval(result,
                    deduplicate && !this.unique && k == this.phases.size() - 1);
        }

        // Return the constructed handler chain
        return result;
    }

    private static void expand(final Statement stmt, final RDFHandler sink,
            final StatementDeduplicator deduplicator,
            @Nullable final StatementMatcher deleteMatcher,
            @Nullable final StatementMatcher insertMatcher, final boolean fixpoint,
            final boolean emitStmt) throws RDFHandlerException {

        // NOTE: this method has a HUGE impact on overall performances. In particular,
        // deduplication speed is critical for overall performances.

        // If statement was already processed, abort
        if (!deduplicator.add(stmt)) {
            return;
        }

        // Emit the supplied statements if authorized and if it does not match delete patterns
        if (emitStmt
                && (deleteMatcher == null || !deleteMatcher.match(stmt.getSubject(),
                        stmt.getPredicate(), stmt.getObject(), stmt.getContext()))) {
            sink.handleStatement(stmt);
        }

        // Abort if there is no way to infer new statements
        if (insertMatcher == null) {
            return;
        }

        // Otherwise handle two cases based on whether evaluation should be done till fixpoint
        if (!fixpoint) {
            // No fixpoint. Inferred statements are directly emitted if they might be new
            for (final StatementTemplate template : insertMatcher.map(stmt.getSubject(),
                    stmt.getPredicate(), stmt.getObject(), stmt.getContext(),
                    StatementTemplate.class)) {
                final Statement stmt2 = template.apply(stmt);
                if (stmt2 != null && deduplicator.add(stmt2)) {
                    sink.handleStatement(stmt2);
                }
            }

        } else {
            // Fixpoint. We proceed in a breadth-first way
            StatementDeduplicator totalDeduplicator = null;
            List<StatementTemplate> templates = null; // created on demand
            Statement[] stack = null;
            int index = 0;
            int capacity = 0;
            Statement s = stmt;
            while (true) {

                // Emit statement if it does not match delete patterns
                if (stack != null
                        && (deleteMatcher == null || !deleteMatcher.match(s.getSubject(),
                                s.getPredicate(), s.getObject(), s.getContext()))) {
                    sink.handleStatement(s);
                }

                // Apply insert part by looking up and applying insert templates.
                templates = insertMatcher.map(s.getSubject(), s.getPredicate(), s.getObject(),
                        s.getContext(), StatementTemplate.class, templates);
                for (final StatementTemplate template : templates) {
                    final Statement stmt2 = template.apply(s, deduplicator);
                    if (stmt2 != null) {
                        if (!deduplicator.isTotal()) {
                            if (totalDeduplicator == null) {
                                totalDeduplicator = StatementDeduplicator
                                        .newTotalDeduplicator(ComparisonMethod.EQUALS);
                                totalDeduplicator.add(stmt);
                            }
                            if (!totalDeduplicator.add(stmt2)) {
                                continue;
                            }
                        }
                        if (index == capacity) {
                            if (capacity == 0) {
                                capacity = 16;
                                stack = new Statement[capacity];
                            } else {
                                capacity *= 2;
                                stack = Arrays.copyOf(stack, capacity);
                            }
                        }
                        stack[index++] = stmt2;
                    }
                }

                // Terminate if there are no more statements to process
                if (index == 0) {
                    break;
                }

                // Otherwise, pick up the next statement to process from the stack
                s = stack[--index];

                // Restore cached templates list
                templates.clear();
            }
        }
    }

    private static Statement[] normalize(Statement[] statements,
            final Function<Value, Value> normalizer) {

        if (normalizer != null) {
            statements = statements.clone();
            for (int i = 0; i < statements.length; ++i) {
                final Statement s = statements[i];
                statements[i] = Statements.VALUE_FACTORY.createStatement(
                        (Resource) normalizer.apply(s.getSubject()),
                        (URI) normalizer.apply(s.getPredicate()), //
                        normalizer.apply(s.getObject()),
                        (Resource) normalizer.apply(s.getContext()));
            }
        }
        return statements;
    }

    private static List<Phase> buildPhases(final Ruleset ruleset) {

        // Scan rules (which are ordered by phase, fixpoint, id) and identify the rules for
        // each phase/fixpoint combination, instantiating the corresponding phase object
        final List<Phase> phases = new ArrayList<>();
        final List<Rule> rules = new ArrayList<>();
        for (final Rule rule : ruleset.getRules()) {
            if (!rules.isEmpty() && (rule.isFixpoint() != rules.get(0).isFixpoint() //
                    || rule.getPhase() != rules.get(0).getPhase())) {
                phases.add(buildPhase(rules));
                rules.clear();
            }
            rules.add(rule);
        }
        if (!rules.isEmpty()) {
            phases.add(buildPhase(rules));
        }
        return phases;
    }

    private static Phase buildPhase(final List<Rule> rules) {

        // Determine whether all rules are (i) simple, (ii) streamable, (iii) insert-only
        boolean simple = true;
        boolean insertOnly = true;
        boolean streamable = true;
        for (final Rule rule : rules) {
            simple &= rule.isSimple();
            insertOnly &= rule.getDeleteExpr() == null;
            streamable &= rule.isStreamable();
        }

        // Select the type of phase based on rule properties
        Phase phase;
        if (streamable && ENABLE_STREAMING) {
            phase = StreamPhase.create(rules);
        } else if (simple && insertOnly && ENABLE_SEMINAIVE) {
            phase = SemiNaivePhase.create(rules);
        } else {
            phase = NaivePhase.create(rules);
        }

        // Return the Phase object built
        return phase;
    }

    private static abstract class Phase {

        private final boolean handlerSupported;

        private final boolean modelSupported;

        Phase(final boolean handlerSupported, final boolean modelSupported) {
            this.handlerSupported = handlerSupported;
            this.modelSupported = modelSupported;
        }

        public Phase normalize(final Function<Value, Value> normalizer) {
            return this;
        }

        public RDFHandler eval(final RDFHandler handler, final boolean deduplicate) {
            throw new Error();
        }

        public void eval(final QuadModel model) {
            throw new Error();
        }

        public boolean isHandlerOutputUnique(final boolean inputUnique) {
            return true;
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
                        ib.addExpr(rule.getWhereExpr(), new StatementTemplate(ip, wp));
                    }
                    if (!rule.getDeletePatterns().isEmpty()) {
                        db = db != null ? db : StatementMatcher.builder();
                        db.addExpr(rule.getWhereExpr());
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

            // Build matchers
            final StatementMatcher dm = db == null ? null : db.build(null);
            final StatementMatcher im = ib == null ? null : ib.build(null);

            // Log result
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "Configured StreamPhase: {} rules; {}; {} axioms; {} delete matcher; "
                                + "{} insert matcher", Iterables.size(rules),
                        containsFixpointRule ? "fixpoint" : "non fixpoint", axioms.size(), dm, im);
            }

            // Create a new StreamPhase object, using null when there are no deletions or
            // insertions possible, and determining whether fixpoint evaluation should occur
            return new StreamPhase(dm, im, axioms.toArray(new Statement[axioms.size()]),
                    containsFixpointRule);
        }

        @Override
        public boolean isHandlerOutputUnique(final boolean inputUnique) {
            return inputUnique && this.insertMatcher == null;
        }

        @Override
        public Phase normalize(final Function<Value, Value> normalizer) {

            // Normalize delete matchers and insert matchers with associated templates
            final StatementMatcher normalizedDeleteMatcher = this.deleteMatcher == null ? null
                    : this.deleteMatcher.normalize(normalizer);
            final StatementMatcher normalizedInsertMatcher = this.insertMatcher == null ? null
                    : this.insertMatcher.normalize(normalizer);

            // Normalize axioms
            final Statement[] normalizedAxioms = QueryRuleEngine
                    .normalize(this.axioms, normalizer);

            // Return a normalized copy of this phase object
            return new StreamPhase(normalizedDeleteMatcher, normalizedInsertMatcher,
                    normalizedAxioms, this.fixpoint);
        }

        @Override
        public RDFHandler eval(final RDFHandler handler, final boolean deduplicate) {

            return new AbstractRDFHandlerWrapper(handler) {

                private StatementDeduplicator deduplicator;

                @Override
                public void startRDF() throws RDFHandlerException {

                    // Delegate
                    super.startRDF();

                    // Initialize deduplicator
                    if (deduplicate || FORCE_DEDUPLICATION) {
                        this.deduplicator = StatementDeduplicator
                                .newTotalDeduplicator(ComparisonMethod.HASH);
                    } else {
                        this.deduplicator = StatementDeduplicator.newPartialDeduplicator(
                                ComparisonMethod.EQUALS, DEDUPLICATION_CACHE_SIZE);
                    }

                    // Emit axioms
                    for (final Statement axiom : StreamPhase.this.axioms) {
                        expand(axiom, this.handler, this.deduplicator,
                                StreamPhase.this.fixpoint ? StreamPhase.this.deleteMatcher : null,
                                StreamPhase.this.insertMatcher, StreamPhase.this.fixpoint, true);
                    }
                }

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {

                    // Delegate to recursive method expand(), marking the statement as explicit
                    expand(stmt, this.handler, this.deduplicator, StreamPhase.this.deleteMatcher,
                            StreamPhase.this.insertMatcher, StreamPhase.this.fixpoint, true);
                }

            };
        }
    }

    private static final class NaivePhase extends Phase {

        private final List<Rule> rules;

        private final boolean fixpoint;

        private final boolean canDelete;

        private final boolean canInsert;

        private NaivePhase(final List<Rule> rules, final boolean fixpoint,
                final boolean canDelete, final boolean canInsert) {
            super(false, true);
            this.rules = rules;
            this.fixpoint = fixpoint;
            this.canDelete = canDelete;
            this.canInsert = canInsert;
        }

        static NaivePhase create(final Iterable<Rule> rules) {

            // Extract list of rules and fixpoint mode
            final List<Rule> ruleList = ImmutableList.copyOf(rules);
            final boolean fixpoint = ruleList.get(0).isFixpoint();

            // Log result
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Configured NaivePhase: {} rules; {}", ruleList.size(),
                        fixpoint ? "fixpoint" : "non fixpoint");
            }

            // Determine whether deletions and/or insertions are possible
            boolean canDelete = false;
            boolean canInsert = false;
            for (final Rule rule : rules) {
                canDelete |= rule.getDeleteExpr() != null;
                canInsert |= rule.getInsertExpr() != null;
            }

            // Build the naive phase
            return new NaivePhase(ruleList, fixpoint && canInsert, canDelete, canInsert);
        }

        private StatementDeduplicator newDeduplicator() {
            return FORCE_DEDUPLICATION ? StatementDeduplicator
                    .newTotalDeduplicator(ComparisonMethod.HASH) : StatementDeduplicator
                    .newPartialDeduplicator(ComparisonMethod.EQUALS, DEDUPLICATION_CACHE_SIZE);
        }

        @Override
        public void eval(final QuadModel model) {

            StatementDeduplicator deleteDeduplicator = this.canDelete ? newDeduplicator() : null;
            StatementDeduplicator insertDeduplicator = this.canInsert ? newDeduplicator() : null;

            if (!this.fixpoint) {
                // (1) One-shot evaluation
                evalRules(deleteDeduplicator, insertDeduplicator, model);

            } else {
                // (2) Naive fixpoint evaluation
                while (true) {
                    final boolean modified = evalRules(deleteDeduplicator, insertDeduplicator,
                            model);
                    if (!modified) {
                        break; // fixpoint reached
                    }
                    if (this.canInsert && this.canDelete) {
                        deleteDeduplicator = newDeduplicator();
                        insertDeduplicator = newDeduplicator();
                    }
                }
            }
        }

        private boolean evalRules(final StatementDeduplicator deleteDeduplicator,
                final StatementDeduplicator insertDeduplicator, final QuadModel model) {

            // Take a timestamp
            final long ts1 = System.currentTimeMillis();

            // Allocate delete and insert buffers (initially empty)
            final StatementBuffer deleteBuffer = new StatementBuffer();
            final StatementBuffer insertBuffer = new StatementBuffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            final int numRules = Rule.evaluate(this.rules, model, null, () -> {
                return deleteDeduplicator.deduplicate(deleteBuffer.get(), true);
            }, () -> {
                return insertDeduplicator.deduplicate(insertBuffer.get(), true);
            });

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

        private final List<Rule> joinRules;

        private final StatementMatcher streamMatcher;

        private final StatementMatcher joinMatcher;

        private final Statement[] axioms;

        private final boolean fixpoint;

        private SemiNaivePhase(final List<Rule> rules, final List<Rule> joinRules,
                final StatementMatcher streamMatcher, final StatementMatcher joinMatcher,
                final Statement[] axioms, final boolean fixpoint) {

            super(!joinMatcher.matchAll(), true);
            this.allRules = rules;
            this.joinRules = joinRules;
            this.streamMatcher = streamMatcher;
            this.joinMatcher = joinMatcher;
            this.axioms = axioms;
            this.fixpoint = fixpoint;
        }

        public static SemiNaivePhase create(final Iterable<Rule> rules) {

            // Extract list of rules and fixpoint mode
            final List<Rule> allRules = ImmutableList.copyOf(rules);
            final boolean fixpoint = allRules.get(0).isFixpoint();

            // Allocate builders for the two matchers and an empty axiom list
            final List<Rule> joinRules = Lists.newArrayList();
            final List<Statement> axioms = new ArrayList<>();
            final StatementMatcher.Builder joinBuilder = StatementMatcher.builder();
            final StatementMatcher.Builder streamBuilder = StatementMatcher.builder();

            // Scan rules to (i) identify join rules; (ii) collect axioms from stream rules
            // with empty WHERE part; and (iii) build the join and stream matchers
            for (final Rule rule : allRules) {
                if (!rule.isStreamable()) {
                    joinRules.add(rule);
                    for (final StatementPattern wp : rule.getWherePatterns()) {
                        joinBuilder.addPattern(wp, null);
                    }
                } else if (!rule.getWherePatterns().isEmpty()) {
                    final StatementPattern wp = rule.getWherePatterns().iterator().next();
                    for (final StatementPattern ip : rule.getInsertPatterns()) {
                        streamBuilder.addExpr(rule.getWhereExpr(), new StatementTemplate(ip, wp));
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
            final StatementMatcher joinMatcher = joinBuilder.build(null);
            final StatementMatcher streamMatcher = streamBuilder.build(null);

            // Log result
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Configured SemiNaivePhase: {} rules ({} join); {}; {} axioms; "
                        + "{} join matcher ({}); {} stream matcher", allRules.size(),
                        joinRules.size(), fixpoint ? "fixpoint" : "non fixpoint", axioms.size(),
                        joinMatcher, joinMatcher.matchAll() ? "match all" : "no match all",
                        streamMatcher);
            }

            // Create and return the SemiNaivePhase object for the rules specified
            return new SemiNaivePhase(allRules, ImmutableList.copyOf(joinRules), streamMatcher,
                    joinMatcher, axioms.toArray(new Statement[axioms.size()]), fixpoint);
        }

        @Override
        public boolean isHandlerOutputUnique(final boolean inputUnique) {
            return this.fixpoint && this.joinMatcher.matchAll();
        }

        @Override
        public Phase normalize(final Function<Value, Value> normalizer) {

            final StatementMatcher normStreamMatcher = this.streamMatcher.normalize(normalizer);
            final StatementMatcher normModelMatcher = this.joinMatcher.normalize(normalizer);
            final Statement[] normAxioms = QueryRuleEngine.normalize(this.axioms, normalizer);

            Phase result = this;
            if (normStreamMatcher != this.streamMatcher || normModelMatcher != this.joinMatcher
                    || normAxioms != this.axioms) {
                result = new SemiNaivePhase(this.allRules, this.joinRules, normStreamMatcher,
                        normModelMatcher, normAxioms, this.fixpoint);
            }
            return result;
        }

        @SuppressWarnings("resource")
        @Override
        public RDFHandler eval(final RDFHandler handler, final boolean deduplicate) {

            // Instantiate and return a wrapper, depending on whether fixpoint evaluation is used
            return this.fixpoint ? new FixpointHandler(handler, deduplicate)
                    : new NonFixpointHandler(handler, deduplicate);
        }

        @Override
        public void eval(final QuadModel model) {

            // Allocate a partial deduplicator to use during all the phase evaluation
            final StatementDeduplicator deduplicator;
            if (FORCE_DEDUPLICATION) {
                deduplicator = StatementDeduplicator.newTotalDeduplicator(ComparisonMethod.HASH);
            } else {
                deduplicator = StatementDeduplicator.newPartialDeduplicator(
                        ComparisonMethod.EQUALS, DEDUPLICATION_CACHE_SIZE);
            }

            // Handle three case
            if (!this.fixpoint) {
                // (1) Single iteration of both join and stream rules
                evalJoinStreamIteration(deduplicator, model);

            } else {
                // (2) Semi-naive fixpoint evaluation. Expand the model evaluating stream
                // rules first, then evaluate join rules + stream rules in fixpoint
                if (this.joinRules.size() < this.allRules.size()) {
                    evalStreamFixpoint(deduplicator, model);
                }
                QuadModel delta = null;
                while (true) {
                    delta = evalJoinIterationStreamFixpoint(deduplicator, model, delta, null);
                    if (delta.isEmpty()) {
                        break; // fixpoint reached
                    }
                }
            }
        }

        private void evalJoinStreamIteration(final StatementDeduplicator deduplicator,
                final QuadModel model) {

            // Take a timestamp before evaluating rules
            final long ts0 = System.currentTimeMillis();

            // Allocate insert buffer (initially empty)
            final StatementBuffer buffer = new StatementBuffer();

            // Evaluate stream rules, including axioms, single iteration (no fixpoint)
            buffer.addAll(Arrays.asList(this.axioms));
            applyStreamRules(deduplicator, model, buffer, false);

            // Evaluate join rules, single iteration (no fixpoint)
            final int numVariants = Rule.evaluate(SemiNaivePhase.this.joinRules, model, null,
                    null, () -> {
                        return deduplicator.deduplicate(buffer.get(), true);
                    });

            // Take a timestamp after evaluating rules
            final long ts1 = System.currentTimeMillis();

            // Apply changes to the model
            final int size0 = model.size();
            buffer.toModel(model, true, null);
            final int size1 = model.size();

            // Take a timestamp after modifying the model
            final long ts2 = System.currentTimeMillis();

            // Log evaluation statistics
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Iteration of {} join rules ({} variants) and {} stream rules "
                        + "performed in {} ms ({} ms evaluation, {} ms model update), "
                        + "{} insertions ({} buffered), {} quads in, {} quads out",
                        this.joinRules.size(), numVariants,
                        this.allRules.size() - this.joinRules.size(), ts2 - ts0, ts1 - ts0, ts2
                                - ts1, size1 - size0, buffer.size(), size0, size1);
            }
        }

        private QuadModel evalJoinIterationStreamFixpoint(
                final StatementDeduplicator deduplicator, final QuadModel model,
                @Nullable final QuadModel delta, @Nullable final RDFHandler unmatcheableSink) {

            // Take a timestamp
            final long ts0 = System.currentTimeMillis();

            // Allocate insert buffer (initially empty)
            final StatementBuffer buffer = new StatementBuffer();

            // Build a supplier of RDFHandlers to be used for handling inferred statements.
            // Produced handlers will expand statements applying streamable rules with partial
            // result deduplication. Expanded statements are either emitted (if not further
            // processable) or accumulated in a buffer
            final Supplier<RDFHandler> supplier = () -> {
                RDFHandler handler = buffer.get();
                if (unmatcheableSink != null) {
                    handler = new AbstractRDFHandlerWrapper(handler) {

                        @Override
                        public void handleStatement(final Statement stmt)
                                throws RDFHandlerException {
                            if (SemiNaivePhase.this.joinMatcher.match(stmt)) {
                                super.handleStatement(stmt);
                            } else {
                                unmatcheableSink.handleStatement(stmt);
                            }
                        }

                    };
                }
                handler = new AbstractRDFHandlerWrapper(handler) {

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        expand(stmt, this.handler, deduplicator, null,
                                SemiNaivePhase.this.streamMatcher, true, true);
                    }

                };
                return handler;
            };

            // Evaluate join rules in parallel using the supplier created before
            final int numVariants = Rule.evaluate(this.joinRules, model, delta, null, supplier);

            // Take another timestamp and measure size of join buffer after evaluation
            final long ts1 = System.currentTimeMillis();
            final int joinBufferSize = buffer.size();

            // Insert the quads resulting from rule evaluation.
            final StatementBuffer deltaBuffer = new StatementBuffer();
            final int size0 = model.size();
            buffer.toModel(model, true, deltaBuffer.get());
            final int size1 = model.size();
            final long ts2 = System.currentTimeMillis();

            // Compute new delta model
            final QuadModel newDelta = model.filter(deltaBuffer);

            // Take a final timestamp and log relevant statistics if enabled
            final long ts3 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                final int numJoinRules = this.joinRules.size();
                final int numStreamRules = this.allRules.size() - this.joinRules.size();
                LOGGER.debug("Iteration of {} join rules ({} variants) and fixpoint of {} stream "
                        + "rules evaluated in {} ms ({} ms evaluation, {} ms model update, {} ms "
                        + "delta), {} insertions ({} buffered), {} quads in, {} quads out",
                        numJoinRules, numVariants, numStreamRules, ts3 - ts0, ts1 - ts0,
                        ts2 - ts1, ts3 - ts2, size1 - size0, joinBufferSize, size0, size1);
            }

            // Return the new delta model
            return newDelta;
        }

        private void evalStreamFixpoint(final StatementDeduplicator deduplicator,
                final QuadModel model) {

            // Take a timestamp for tracking execution time
            final long ts0 = System.currentTimeMillis();

            // Allocate a buffer where to accumulate the result of rule evaluation
            final StatementBuffer buffer = new StatementBuffer();
            buffer.addAll(Arrays.asList(this.axioms));
            applyStreamRules(deduplicator, Iterables.concat(Arrays.asList(this.axioms), model),
                    buffer, true);

            // Take a timestamp before modifying the model
            final long ts1 = System.currentTimeMillis();

            // Apply changes to the model
            final int size0 = model.size();
            buffer.toModel(model, true, null);
            final int size1 = model.size();

            // Take a final timestamp
            final long ts2 = System.currentTimeMillis();

            // Log statistics
            if (LOGGER.isDebugEnabled()) {
                final int numStreamRules = this.allRules.size() - this.joinRules.size();
                LOGGER.debug("Fixpoint of {} stream rules evaluated in {} ms ({} ms evaluation, "
                        + "{} ms model update), {} insertions ({} buffered), {} quads in, "
                        + "{} quads out", numStreamRules, ts2 - ts0, ts1 - ts0, ts2 - ts1, size1
                        - size0, buffer.size(), size0, size1);
            }
        }

        private void applyStreamRules(final StatementDeduplicator deduplicator,
                final Iterable<Statement> statements, final Supplier<RDFHandler> sink,
                final boolean fixpoint) {

            // Prepare a tree of RDFHandlers to process model statements. The root handler
            // dispatches statements in a round robin way to child handler sequences, each one
            // delegating work to another thread that perform expansion and populates the buffer
            final RDFHandler[] sinks = new RDFHandler[Environment.getCores()];
            for (int i = 0; i < sinks.length; ++i) {
                sinks[i] = RDFHandlers.decouple(new AbstractRDFHandlerWrapper(sink.get()) {

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        expand(stmt, this.handler, deduplicator, null,
                                SemiNaivePhase.this.streamMatcher, fixpoint, false);
                    }

                }, 1);
            }
            final RDFHandler handler = RDFHandlers.dispatchRoundRobin(64, sinks);

            // Evaluate rules
            try {
                RDFSources.wrap(statements).emit(handler, 1);
            } catch (final RDFHandlerException ex) {
                Throwables.propagate(ex);
            }
        }

        private final class FixpointHandler extends AbstractRDFHandlerWrapper {

            private final boolean deduplicate;

            private QuadModel joinModel;

            private StatementDeduplicator deduplicator;

            private RDFHandler sink;

            FixpointHandler(final RDFHandler handler, final boolean deduplicate) {
                super(handler);
                this.deduplicate = deduplicate && SemiNaivePhase.this.fixpoint
                        && SemiNaivePhase.this.joinMatcher.matchAll();
            }

            @Override
            public void startRDF() throws RDFHandlerException {

                // Delegate
                super.startRDF();

                // Allocate a model for statements matching WHERE patterns of join rule
                this.joinModel = QuadModel.create();

                // Allocate a deduplicator
                this.deduplicator = this.deduplicate || FORCE_DEDUPLICATION ? StatementDeduplicator
                        .newTotalDeduplicator(ComparisonMethod.HASH) : StatementDeduplicator
                        .newPartialDeduplicator(ComparisonMethod.EQUALS, DEDUPLICATION_CACHE_SIZE);

                // Setup the sink where to emit output of stream rules
                this.sink = new AbstractRDFHandlerWrapper(this.handler) {

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        if (SemiNaivePhase.this.joinMatcher.match(stmt)) {
                            synchronized (FixpointHandler.this.joinModel) {
                                FixpointHandler.this.joinModel.add(stmt);
                            }
                        } else {
                            this.handler.handleStatement(stmt);
                        }
                    }

                };

                // Emit axioms
                for (final Statement axiom : SemiNaivePhase.this.axioms) {
                    handleStatement(axiom);
                }
            }

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {

                // Apply stream rules with output deduplication. Inferred statements are either
                // emitted (if not further processable) or accumulated in a model
                expand(stmt, this.sink, this.deduplicator, null,
                        SemiNaivePhase.this.streamMatcher, true, true);
            }

            @Override
            public void endRDF() throws RDFHandlerException {

                // Semi-naive fixpoint evaluation
                QuadModel delta = null;
                while (true) {
                    delta = evalJoinIterationStreamFixpoint(this.deduplicator, this.joinModel,
                            delta, this.handler);
                    if (delta.isEmpty()) {
                        break; // fixpoint reached
                    }
                }

                // Emit closed statements in the join model
                final RDFHandler sink = RDFHandlers.decouple(RDFHandlers.ignoreMethods(
                        this.handler, RDFHandlers.METHOD_START_RDF | RDFHandlers.METHOD_END_RDF
                                | RDFHandlers.METHOD_CLOSE));
                RDFSources.wrap(this.joinModel).emit(sink, 1);

                // Release memory
                this.joinModel = null;
                this.deduplicator = null;
                this.sink = null;

                // Signal completion
                super.endRDF();
            }

        }

        private final class NonFixpointHandler extends AbstractRDFHandlerWrapper {

            private final boolean deduplicate;

            private QuadModel joinModel;

            private StatementDeduplicator deduplicator;

            NonFixpointHandler(final RDFHandler handler, final boolean deduplicate) {
                super(handler);
                this.deduplicate = deduplicate;
            }

            @Override
            public void startRDF() throws RDFHandlerException {

                // Delegate
                super.startRDF();

                // Allocate model and deduplicator
                this.joinModel = QuadModel.create();
                this.deduplicator = this.deduplicate || FORCE_DEDUPLICATION ? StatementDeduplicator
                        .newTotalDeduplicator(ComparisonMethod.HASH) : StatementDeduplicator
                        .newPartialDeduplicator(ComparisonMethod.EQUALS, DEDUPLICATION_CACHE_SIZE);

                // Emit axioms
                for (final Statement axiom : SemiNaivePhase.this.axioms) {
                    this.handler.handleStatement(axiom);
                }
            }

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {

                // Apply stream rules to all incoming statements
                expand(stmt, this.handler, this.deduplicator, null,
                        SemiNaivePhase.this.streamMatcher, false, true);

                // Statements matching WHERE part of join rules are accumulated in a model
                if (SemiNaivePhase.this.joinMatcher.match(stmt)) {
                    synchronized (this.joinModel) {
                        this.joinModel.add(stmt);
                    }
                }
            }

            @Override
            public void endRDF() throws RDFHandlerException {

                // Apply join rules on accumulated statements, emitting inferred statements
                Rule.evaluate(SemiNaivePhase.this.joinRules, this.joinModel, null, null, () -> {
                    return this.deduplicator.deduplicate(this.handler, true);
                });

                // Notify completion
                super.endRDF();
            }

        }

    }

}
