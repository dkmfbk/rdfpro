package eu.fbk.rdfpro.rules.seminaive;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.PrimitiveSink;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.impl.EmptyBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.rules.api.Rule;
import eu.fbk.rdfpro.rules.api.RuleEngine;
import eu.fbk.rdfpro.rules.api.Ruleset;
import eu.fbk.rdfpro.rules.model.QuadModel;
import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.rules.util.Iterators;
import eu.fbk.rdfpro.rules.util.Sorting;
import eu.fbk.rdfpro.rules.util.Sorting.ArrayComparator;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

public class SemiNaiveRuleEngine extends RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(SemiNaiveRuleEngine.class);

    private final Map<URI, Collector> collectors;

    public SemiNaiveRuleEngine(final Ruleset ruleset) {

        // Initialize parent class
        super(ruleset);

        // Build a collector for each rule in the ruleset
        final ImmutableMap.Builder<URI, Collector> builder = ImmutableMap.builder();
        for (final Rule rule : ruleset.getRules()) {
            builder.put(rule.getID(), Collector.create(rule));
        }
        this.collectors = builder.build();
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

        private final Map<URI, Collector> collectors;

        public Evaluation(final QuadModel model) {

            // Store the model to act on
            this.model = model;

            // Initialize an empty map where to keep collectors 'optimized' for this evaluation
            this.collectors = Maps.newHashMap();
        }

        public void eval() {

            // Scan rules (which are ordered by phase, fixpoint, id) and identify the rules for
            // each phase/fixpoint combination, delegating to evalPhase() their execution
            final List<Rule> rules = new ArrayList<>();
            for (final Rule rule : SemiNaiveRuleEngine.this.ruleset.getRules()) {
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
            final Buffer deleteBuffer = new Buffer();
            final Buffer insertBuffer = new Buffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            RuleEvaluation re;
            final List<RuleEvaluation> ruleEvaluations = new ArrayList<>();
            for (final Rule rule : rules) {
                re = new RuleEvaluation(rule, deleteBuffer, insertBuffer, null, null);
                if (re.isActivable()) {
                    ruleEvaluations.add(re);
                }
            }
            Collections.sort(ruleEvaluations);
            Environment.run(ruleEvaluations);

            // Take another timestamp and measure buffer sizes after rule evaluation
            final long ts2 = System.currentTimeMillis();
            final int deleteBufferSize = deleteBuffer.size();
            final int insertBufferSize = insertBuffer.size();

            // Apply the modifications resulting from rule evaluation, tracking the model sizes. A
            // side result of this operation is that the two buffers are deduplicated (this is
            // essential for determining if the model changed or not)
            final int size0 = this.model.size();
            deleteBuffer.apply(this.model, false);
            final int size1 = this.model.size();
            insertBuffer.apply(this.model, true);
            final int size2 = this.model.size();

            // Determine whether the model has changed w.r.t. its original state. This is done by
            // first comparing the different model sizes and as a last resort by comparing the two
            // delete and insert buffers to see if they are equal (=> model unchanged)
            boolean result;
            if (size0 != size2) {
                result = true;
            } else if (size0 == size1) {
                result = false;
            } else {
                result = deleteBuffer.contains(insertBuffer);
            }

            // Take a final timestamp and log relevant statistics if enabled
            final long ts3 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} rules (out of {}) rules evaluated in {} ms ({} ms query, "
                        + "{} ms modify), {} deletions ({} buffered), {} insertions "
                        + "({} buffered), {} quads in, {} quads out", ruleEvaluations.size(),
                        rules.size(), ts3 - ts1, ts2 - ts1, ts3 - ts2, size1 - size0,
                        deleteBufferSize, size2 - size1, insertBufferSize, size0, size2);
            }

            // Return true if the model has changed
            return result;
        }

        private QuadModel evalRules(final List<Rule> rules, @Nullable final QuadModel delta) {

            // Take a timestamp
            final long ts1 = System.currentTimeMillis();

            // Allocate insert buffer (initially empty)
            final Buffer insertBuffer = new Buffer();

            // Evaluate all rules in parallel, collecting produced quads in the two buffers
            int numVariants = 0;
            RuleEvaluation re;
            final List<RuleEvaluation> ruleEvaluations = new ArrayList<>();
            for (final Rule rule : rules) {
                if (delta == null || rule.getWhereExpr() == null) {
                    ++numVariants;
                    re = new RuleEvaluation(rule, null, insertBuffer, null, null);
                    if (re.isActivable()) {
                        ruleEvaluations.add(re);
                    }
                } else {
                    for (final StatementPattern pattern : rule.getWherePatterns()) {
                        ++numVariants;
                        re = new RuleEvaluation(rule, null, insertBuffer, pattern, delta);
                        if (re.isActivable()) {
                            ruleEvaluations.add(re);
                        }
                    }
                }
            }
            Collections.sort(ruleEvaluations);
            Environment.run(ruleEvaluations);

            // Take another timestamp and measure insert buffer size after rule evaluation
            final long ts2 = System.currentTimeMillis();
            final int insertBufferSize = insertBuffer.size();

            // Insert the quads resulting from rule evaluation. A side result of this operation is
            // that the insert buffer is deduplicated
            final int size0 = this.model.size();
            insertBuffer.apply(this.model, true);
            final int size1 = this.model.size();
            final long ts3 = System.currentTimeMillis();

            // Compute new delta model
            final QuadModel newDelta = insertBuffer.toDeltaModel(this.model);

            // Take a final timestamp and log relevant statistics if enabled
            final long ts4 = System.currentTimeMillis();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} rule variants (out of {} variants of {} rules) evaluated in "
                        + "{} ms ({} ms query, {} ms modify, {} ms delta), {} insertions "
                        + "({} buffered), {} quads in, {} quads out", ruleEvaluations.size(),
                        numVariants, rules.size(), ts4 - ts1, ts2 - ts1, ts3 - ts2, ts4 - ts3,
                        size1 - size0, insertBufferSize, size0, size1);
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

        private final class RuleEvaluation implements Runnable, Comparable<RuleEvaluation> {

            private final Rule rule;

            private final Buffer deleteBuffer;

            private final Buffer insertBuffer;

            @Nullable
            private final StatementPattern deltaPattern;

            @Nullable
            private final QuadModel deltaModel;

            private final EvaluationStatistics statistics;

            private final double cardinality;

            RuleEvaluation(final Rule rule, final Buffer deleteBuffer, final Buffer insertBuffer,
                    @Nullable final StatementPattern deltaPattern,
                    @Nullable final QuadModel deltaModel) {
                this.rule = rule;
                this.deleteBuffer = deleteBuffer;
                this.insertBuffer = insertBuffer;
                this.deltaPattern = deltaPattern;
                this.deltaModel = deltaModel;
                this.statistics = deltaModel == null || deltaPattern == null ? Evaluation.this.model
                        .getEvaluationStatistics() : newSemiNaiveEvaluationStatistics(deltaModel,
                        deltaPattern);
                this.cardinality = rule.getWhereExpr() == null ? 1.0 : this.statistics
                        .getCardinality(rule.getWhereExpr());
            }

            public boolean isActivable() {
                return this.cardinality != 0.0;
            }

            @Override
            public int compareTo(final RuleEvaluation other) {
                return -Double.compare(this.cardinality, other.cardinality);
            }

            @Override
            public void run() {

                // Aliases of Evaluation fields
                final QuadModel model = Evaluation.this.model;
                final Map<URI, Collector> collectors = Evaluation.this.collectors;

                // Take a timestamp to measure rule evaluation time
                final long ts = System.currentTimeMillis();

                // Define delete and insert buffers (will be created if necessary)
                Buffer.Appender deleteAppender = null;
                Buffer.Appender insertAppender = null;

                // Define counter for # activations
                int numActivations = 0;

                // Start evaluating the rule
                Iterator<BindingSet> iterator;
                if (this.cardinality == 0.0) {
                    iterator = Collections.emptyIterator();
                } else if (this.cardinality == 0.0) {
                    iterator = Collections.emptyIterator();
                } else if (this.rule.getWhereExpr() == null) {
                    iterator = Collections.singleton(EmptyBindingSet.getInstance()).iterator();
                } else if (this.deltaPattern == null) {
                    iterator = model.evaluate(this.rule.getWhereExpr(), null, null);
                } else {
                    iterator = Algebra.evaluateTupleExpr(this.rule.getWhereExpr(), null, null, //
                            newSemiNaiveEvaluationStrategy(this.deltaModel, this.deltaPattern, //
                                    null), this.statistics, model.getValueNormalizer());
                }

                try {
                    // Proceed only if there is some query result to process
                    if (iterator.hasNext()) {

                        // Allocate an appender for the delete buffer, if necessary
                        if (this.rule.getDeleteExpr() != null) {
                            deleteAppender = this.deleteBuffer.appender();
                            deleteAppender.startRDF();
                        }

                        // Allocate an appender for the insert buffer, if necessary
                        if (this.rule.getInsertExpr() != null) {
                            insertAppender = this.insertBuffer.appender();
                            insertAppender.startRDF();
                        }

                        // Retrieve the optimized rule collector, creating it the first time
                        Collector collector;
                        synchronized (collectors) {
                            collector = collectors.get(this.rule.getID());
                            if (collector == null) {
                                collector = SemiNaiveRuleEngine.this.collectors.get(
                                        this.rule.getID()).normalize(model);
                                collectors.put(this.rule.getID(), collector);
                            }
                        }

                        // Scan the bindings returned by the WHERE part, using the collector to
                        // compute deleted/inserted quads
                        while (iterator.hasNext()) {
                            ++numActivations;
                            final BindingSet bindings = iterator.next();
                            collector.collect(bindings, model, deleteAppender, insertAppender);
                        }

                        // Flush the delete appender, if any
                        if (deleteAppender != null) {
                            deleteAppender.endRDF();
                        }

                        // Flush the insert appender, if any
                        if (insertAppender != null) {
                            insertAppender.endRDF();
                        }
                    }

                } finally {
                    // Ensure to close the iterator (if it needs to be closed)
                    IO.closeQuietly(iterator);
                }

                // Log relevant rule evaluation statistics
                if (LOGGER.isTraceEnabled()) {
                    final String patternString = this.deltaPattern == null ? ""
                            : " (delta pattern " + Algebra.format(this.deltaPattern) + ")";
                    LOGGER.trace("Rule {}{} evaluated in {} ms: {} activations, "
                            + "{} quads to delete, {} quads to insert", this.rule.getID()
                            .getLocalName(), patternString, System.currentTimeMillis() - ts,
                            numActivations, deleteAppender == null ? 0 : deleteAppender.getSize(),
                            insertAppender == null ? 0 : insertAppender.getSize());
                }
            }

            private EvaluationStrategy newSemiNaiveEvaluationStrategy(final QuadModel deltaModel,
                    final StatementPattern deltaPattern, @Nullable final Dataset dataset) {

                final AtomicReference<TripleSource> selectedSource = new AtomicReference<>();

                final TripleSource baseSource = Evaluation.this.model.getTripleSource();
                final TripleSource deltaSource = deltaModel.getTripleSource();
                final TripleSource semiNaiveSource = new TripleSource() {

                    @Override
                    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                            final Resource subj, final URI pred, final Value obj,
                            final Resource... contexts) throws QueryEvaluationException {
                        return selectedSource.get().getStatements(subj, pred, obj, contexts);
                    }

                    @Override
                    public ValueFactory getValueFactory() {
                        return baseSource.getValueFactory();
                    }

                };

                return new EvaluationStrategyImpl(semiNaiveSource, dataset,
                        Algebra.getFederatedServiceResolver()) {

                    @Nullable
                    private StatementPattern normalizedDeltaPattern = null;

                    @Override
                    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                            final StatementPattern pattern, final BindingSet bindings)
                            throws QueryEvaluationException {

                        if (this.normalizedDeltaPattern == null) {
                            if (pattern.equals(deltaPattern)) {
                                this.normalizedDeltaPattern = pattern;
                            }
                        }
                        if (this.normalizedDeltaPattern == pattern) {
                            selectedSource.set(deltaSource);
                        } else {
                            selectedSource.set(baseSource);
                        }
                        return super.evaluate(pattern, bindings);
                    }
                };
            }

            private EvaluationStatistics newSemiNaiveEvaluationStatistics(
                    final QuadModel deltaModel, final StatementPattern deltaPattern) {

                return new EvaluationStatistics() {

                    @Override
                    protected CardinalityCalculator createCardinalityCalculator() {
                        return new CardinalityCalculator() {

                            @Override
                            public final double getCardinality(final StatementPattern pattern) {
                                return (pattern.equals(deltaPattern) ? deltaModel
                                        .getEvaluationStatistics() : Evaluation.this.model
                                        .getEvaluationStatistics()).getCardinality(pattern);
                            }

                        };
                    }

                };
            }

        }

    }

    private static final class Collector {

        private static final int[] EMPTY_INDEXES = new int[0];

        private static final String[] EMPTY_VARS = new String[0];

        private static final Value[] EMPTY_CONSTANTS = new Value[0];

        private final int[] deleteIndexes;

        private final int[] insertIndexes;

        private final String[] commonVars;

        private final Value[] constants;

        static Collector create(final Rule rule) {

            // Retrieve the list of variables common to the WHERE and DELETE/INSERT expressions
            final List<String> commonVars = rule.getCommonVariables();
            final String[] commonVarsArray = commonVars.isEmpty() ? EMPTY_VARS : commonVars
                    .toArray(new String[commonVars.size()]);

            // Compute the mappings (indexes+constants) required for translating bindings to quads
            final List<Value> constants = new ArrayList<>();
            final int[] deleteIndexes = createHelper(rule.getDeleteExpr(), commonVars, constants);
            final int[] insertIndexes = createHelper(rule.getInsertExpr(), commonVars, constants);
            final Value[] constantsArray = constants.isEmpty() ? EMPTY_CONSTANTS : constants
                    .toArray(new Value[constants.size()]);

            // Log results
            if (LOGGER.isTraceEnabled()) {
                final StringBuilder builder = new StringBuilder();
                for (final Value constant : constants) {
                    builder.append(builder.length() == 0 ? "[" : ", ");
                    builder.append(Statements.formatValue(constant, Namespaces.DEFAULT));
                }
                builder.append("]");
                LOGGER.trace("Collector for rule {}: vars={}, constants={}, delete indexes={}, "
                        + "insert indexes={}", rule.getID().getLocalName(), commonVars, builder,
                        deleteIndexes, insertIndexes);
            }

            // Instantiate a collector with the data structures computed above
            return new Collector(deleteIndexes, insertIndexes, commonVarsArray, constantsArray);
        }

        private static int[] createHelper(@Nullable final TupleExpr expr,
                final List<String> commonVars, final List<Value> constants) {

            // Return an empty index array if there is no expression (-> no mapping necessary)
            if (expr == null) {
                return EMPTY_INDEXES;
            }

            // Otherwise, extracts all the statement patterns in the expression
            final List<StatementPattern> patterns = Algebra.extractNodes(expr,
                    StatementPattern.class, null, null);

            // Build an index array with 4 slots for each pattern. Each slot contains either: the
            // index (i + 1) of the variable in commonVars corresponding to that quad component,
            // or the index -(i+1) of the constant in 'constants' corresponding to that component,
            // or 0 to denote the default context constant (sesame:nil)
            final int[] indexes = new int[4 * patterns.size()];
            for (int i = 0; i < patterns.size(); ++i) {
                final List<Var> patternVars = patterns.get(i).getVarList();
                for (int j = 0; j < patternVars.size(); ++j) {
                    final Var var = patternVars.get(j);
                    if (var.getValue() != null) {
                        int index = constants.indexOf(var.getValue());
                        if (index < 0) {
                            index = constants.size();
                            constants.add(var.getValue());
                        }
                        indexes[i * 4 + j] = -index - 1;
                    } else {
                        final int index = commonVars.indexOf(var.getName());
                        if (index < 0) {
                            throw new Error("Var " + var.getName() + " not among common vars "
                                    + commonVars);
                        }
                        indexes[i * 4 + j] = index + 1;
                    }
                }
            }
            return indexes;
        }

        private Collector(final int[] deleteIndexes, final int[] insertIndexes,
                final String[] commonVars, final Value[] constants) {

            // Store all the supplied parameters
            this.deleteIndexes = deleteIndexes;
            this.insertIndexes = insertIndexes;
            this.commonVars = commonVars;
            this.constants = constants;
        }

        private Value resolve(final int index, final Value[] commonValues) {
            return index > 0 ? commonValues[index - 1] : index == 0 ? null
                    : this.constants[-index - 1];
        }

        void collect(final BindingSet bindings, final QuadModel model,
                @Nullable final Buffer.Appender deleteAppender,
                @Nullable final Buffer.Appender insertAppender) {

            // Transform the bindings var=value map to a value array, using the same variable
            // order of commonVars
            final Value[] commonValues = new Value[this.commonVars.length];
            for (int i = 0; i < this.commonVars.length; ++i) {
                commonValues[i] = bindings.getValue(this.commonVars[i]);
            }

            // Generate and send to the delete Appender the quads that need to be removed. In case
            // of quads in the default context, we need to explode them including all the quads
            // with same SPO and different context (due to SESAME semantics 'default context =
            // merge of all other contexts').
            if (deleteAppender != null) {
                for (int i = 0; i < this.deleteIndexes.length; i += 4) {
                    final Value subj = resolve(this.deleteIndexes[i], commonValues);
                    final Value pred = resolve(this.deleteIndexes[i + 1], commonValues);
                    final Value obj = resolve(this.deleteIndexes[i + 2], commonValues);
                    final Value ctx = resolve(this.deleteIndexes[i + 3], commonValues);
                    if (subj instanceof Resource && pred instanceof URI && obj instanceof Value) {
                        if (ctx instanceof Resource) {
                            deleteAppender.handleStatement((Resource) subj, (URI) pred, obj,
                                    (Resource) ctx);
                        } else if (ctx == null) {
                            for (final Statement stmt : model.filter((Resource) subj, (URI) pred,
                                    obj)) {
                                deleteAppender.handleStatement((Resource) subj, (URI) pred, obj,
                                        stmt.getContext());
                            }
                        }
                    }
                }
            }

            // Generate and send to the insert Appender the quads that need to be inserted
            if (insertAppender != null) {
                for (int i = 0; i < this.insertIndexes.length; i += 4) {
                    final Value subj = resolve(this.insertIndexes[i], commonValues);
                    final Value pred = resolve(this.insertIndexes[i + 1], commonValues);
                    final Value obj = resolve(this.insertIndexes[i + 2], commonValues);
                    final Value ctx = resolve(this.insertIndexes[i + 3], commonValues);
                    if (subj instanceof Resource && pred instanceof URI && obj instanceof Value
                            && (ctx == null || ctx instanceof Resource)) {
                        insertAppender.handleStatement((Resource) subj, (URI) pred, obj,
                                (Resource) ctx);
                    }
                }
            }
        }

        Collector normalize(final QuadModel model) {

            // Replace each Value constant in the constants array with the corresponding Value
            // instance already stored in the quad model, if any. This may enable using identity
            // comparison of values instead of string comparison (faster!)
            int numReplacements = 0;
            final Value[] normalizedConstants = new Value[this.constants.length];
            for (int i = 0; i < this.constants.length; ++i) {
                normalizedConstants[i] = model.normalize(this.constants[i]);
                numReplacements += normalizedConstants[i] == this.constants[i] ? 0 : 1;
            }
            LOGGER.trace("{} constant values replaced during collector normalization",
                    numReplacements);

            // Return the collector with the same parameters except the normalized constant array
            return new Collector(this.deleteIndexes, this.insertIndexes, this.commonVars,
                    normalizedConstants);
        }

    }

    private static final class Buffer {

        private static final int BLOCK_SIZE = 4 * 1024; // 1K quads, 4K values, 16K bytes

        private static final int CACHE_SIZE = 4 * 1024; // 1K quads, 4K values, 16K bytes

        private final List<Value[]> blocks;

        private int offset;

        Buffer() {
            this.blocks = Lists.newArrayList();
            this.offset = BLOCK_SIZE;
        }

        synchronized int size() {
            // Compute and return the number of *statements* in the buffer
            return this.blocks.isEmpty() ? 0
                    : ((this.blocks.size() - 1) * BLOCK_SIZE + this.offset) / 4;
        }

        synchronized void apply(final QuadModel model, final boolean add) {

            // Abort if the buffer is empty
            if (this.blocks.isEmpty()) {
                return;
            }

            // Otherwise, allocate variables to keep track of the position (inside this buffer)
            // where to write back deduplicated quads
            int writeIndex = 0;
            int writeOffset = BLOCK_SIZE;
            Value[] writeBlock = null;

            // Iterate over the blocks of this buffer, adding/deleting stored quads to/from the
            // model specified. Quads that are succesfully added/deleted are moved at the
            // beginning of the buffer, while the other are discarded. As a consequence, the
            // buffer will contain all distinct quads that representing the delta added to /
            // removed from the model
            for (int readIndex = 0; readIndex < this.blocks.size(); ++readIndex) {
                final Value[] readBlock = this.blocks.get(readIndex);
                final int maxReadOffset = readIndex < this.blocks.size() - 1 ? BLOCK_SIZE
                        : this.offset;
                for (int readOffset = 0; readOffset < maxReadOffset; readOffset += 4) {
                    Resource subj = (Resource) readBlock[readOffset];
                    URI pred = (URI) readBlock[readOffset + 1];
                    Value obj = readBlock[readOffset + 2];
                    Resource ctx = (Resource) readBlock[readOffset + 3];
                    final boolean modified;
                    if (add) {
                        subj = model.normalize(subj);
                        pred = model.normalize(pred);
                        obj = model.normalize(obj);
                        ctx = model.normalize(ctx);
                        modified = model.add(subj, pred, obj, ctx);
                    } else {
                        modified = model.remove(subj, pred, obj, ctx);
                    }
                    if (modified) {
                        if (writeOffset == BLOCK_SIZE) {
                            writeBlock = this.blocks.get(writeIndex++);
                            writeOffset = 0;
                        }
                        writeBlock[writeOffset++] = subj;
                        writeBlock[writeOffset++] = pred;
                        writeBlock[writeOffset++] = obj;
                        writeBlock[writeOffset++] = ctx;
                    }
                }
            }

            // Drop unused blocks at the end of the buffer, adjusting offsets
            if (writeIndex == 0) {
                this.blocks.clear();
                this.offset = 0;
            } else {
                while (this.blocks.size() > writeIndex) {
                    this.blocks.remove(this.blocks.size() - 1);
                }
                this.offset = writeOffset;
            }
        }

        QuadModel toDeltaModel(final QuadModel model) {

            final DeltaModel2 deltaModel = new DeltaModel2(model, size());
            for (int index = 0; index < this.blocks.size(); ++index) {
                final Value[] block = this.blocks.get(index);
                final int maxOffset = index < this.blocks.size() - 1 ? BLOCK_SIZE : this.offset;
                for (int offset = 0; offset < maxOffset; offset += 4) {
                    deltaModel.include((Resource) block[offset], (URI) block[offset + 1],
                            block[offset + 2], (Resource) block[offset + 3]);
                }
            }
            return deltaModel;
        }

        boolean contains(final Buffer buffer) {

            // Build an open-addressing hash table with pointers to statements in the buffer
            final int size = size();
            final int[] buckets = new int[Integer.highestOneBit(size) * 4 - 1];
            int pointer = 0;
            for (int index = 0; index < this.blocks.size(); ++index) {
                final Value[] block = this.blocks.get(index);
                final int maxOffset = index < this.blocks.size() - 1 ? BLOCK_SIZE : this.offset;
                for (int offset = 0; offset < maxOffset; offset += 4) {
                    final int hash = hash(block[offset], block[offset + 1], block[offset + 2],
                            block[offset + 3]);
                    int slot = (hash & 0x7FFFFFFF) % buckets.length;
                    while (buckets[slot] != 0) {
                        slot = (slot + 1) % buckets.length;
                    }
                    buckets[slot] = pointer;
                    pointer += 4;
                }
            }

            // Lookup the statements of the other buffer in the table. Halt on statement not found
            for (int index = 0; index < buffer.blocks.size(); ++index) {
                final Value[] block = buffer.blocks.get(index);
                final int maxOffset = index < buffer.size() - 1 ? BLOCK_SIZE : buffer.offset;
                outer: for (int offset = 0; offset < maxOffset; offset += 4) {
                    final int hash = hash(block[offset], block[offset + 1], block[offset + 2],
                            block[offset + 3]);
                    int slot = (hash & 0x7FFFFFFF) % buckets.length;
                    while (true) {
                        if (buckets[slot] == 0) {
                            return false;
                        } else {
                            pointer = buckets[slot];
                            final int thisIndex = pointer / BLOCK_SIZE;
                            final int thisOffset = pointer % BLOCK_SIZE;
                            final Value[] thisBlock = this.blocks.get(thisIndex);
                            if (thisBlock[thisOffset].equals(block[offset])
                                    && thisBlock[thisOffset + 1].equals(block[offset + 1])
                                    && thisBlock[thisOffset + 2].equals(block[offset + 2])
                                    && Objects.equals(thisBlock[thisOffset + 3], //
                                            block[offset + 3])) {
                                continue outer;
                            }
                        }
                        slot = (slot + 1) % buckets.length;
                    }
                }
            }
            return true;
        }

        Appender appender() {
            // Return a new Appender writing back to this buffer
            return new Appender();
        }

        private synchronized Value[] append(final Value[] block, final int blockLength) {

            // Handle two cases
            if (blockLength == block.length) {

                // (1) A full block is being added. Don't copy, just insert the block in the list
                if (this.offset >= BLOCK_SIZE) {
                    this.blocks.add(block);
                } else {
                    final Value[] last = this.blocks.remove(this.blocks.size() - 1);
                    this.blocks.add(block);
                    this.blocks.add(last);
                }
                return new Value[BLOCK_SIZE];

            } else {

                // (2) A partial block is being added. Copy the content of the block specified
                // into buffer blocks, possibly allocating new blocks if necessary.
                int offset = 0;
                while (offset < blockLength) {
                    Value[] thisBlock;
                    if (this.offset < BLOCK_SIZE) {
                        thisBlock = this.blocks.get(this.blocks.size() - 1);
                    } else {
                        thisBlock = new Value[BLOCK_SIZE];
                        this.blocks.add(thisBlock);
                        this.offset = 0;
                    }
                    final int length = Math.min(blockLength - offset, BLOCK_SIZE - this.offset);
                    System.arraycopy(block, offset, thisBlock, this.offset, length);
                    offset += length;
                    this.offset += length;
                }
                return block;
            }
        }

        private static int hash(final Value subj, final Value pred, final Value obj,
                final Value ctx) {
            // Return an hash code depending on all four SPOC components
            return 6661 * subj.hashCode() + 961 * pred.hashCode() + 31 * obj.hashCode()
                    + (ctx == null ? 0 : ctx.hashCode());
        }

        final class Appender extends AbstractRDFHandler {

            private Value[] cache;

            private Value[] block;

            private int offset;

            private int size;

            private Appender() {
                this.cache = null;
                this.block = null;
                this.offset = 0;
                this.size = 0;
            }

            public int getSize() {
                return this.size;
            }

            @Override
            public void startRDF() {
                // Allocate a local block
                this.cache = new Value[CACHE_SIZE];
                this.block = new Value[BLOCK_SIZE];
                this.offset = 0;
            }

            @Override
            public void handleStatement(final Statement stmt) {
                // Delegate (this method is here so to make Appender compatible with RDFHandler)
                this.handleStatement(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                        stmt.getContext());
            }

            public void handleStatement(final Resource subj, final URI pred, final Value obj,
                    @Nullable final Resource ctx) {

                // Increment the number of statements collected
                ++this.size;

                // Abort in case the quad is contained in the cache with recently added quads
                final int hash = 6661 * System.identityHashCode(subj) + 961
                        * System.identityHashCode(pred) + 31 * System.identityHashCode(obj)
                        + (ctx == null ? 0 : System.identityHashCode(ctx));
                final int index = (hash & 0x7FFFFFFF) % (this.cache.length / 4) * 4;
                if (this.cache[index] == subj && this.cache[index + 1] == pred
                        && this.cache[index + 2] == obj && this.cache[index + 3] == ctx) {
                    return;
                }

                // Otherwise, update the cache
                this.cache[index] = subj;
                this.cache[index + 1] = pred;
                this.cache[index + 2] = obj;
                this.cache[index + 3] = ctx;

                // Append the SPOC components to the local block
                this.block[this.offset++] = subj;
                this.block[this.offset++] = pred;
                this.block[this.offset++] = obj;
                this.block[this.offset++] = ctx;

                // If the local block is full, copy its content to the buffer (this requires
                // synchronization)
                if (this.offset == this.block.length) {
                    this.block = append(this.block, this.block.length);
                    this.offset = 0;
                }

            }

            @Override
            public void endRDF() {

                // Flush the content of the local block to the buffer, if necessary, and release
                // the block to free memory
                if (this.offset > 0) {
                    this.block = append(this.block, this.offset);
                    this.offset = 0;
                }
                this.block = null;
            }

        }

    }

    private static final class DeltaModel extends QuadModel {

        private static final Funnel<Value> VALUE_FUNNEL = new Funnel<Value>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void funnel(final Value value, final PrimitiveSink sink) {
                sink.putInt(hash(value));
            }

        };

        private static final long serialVersionUID = 1;

        private final QuadModel model;

        private final int size;

        private final BloomFilter<Integer> statementFilter;

        private final BloomFilter<Value> subjFilter;

        private final BloomFilter<Value> predFilter;

        private final BloomFilter<Value> objFilter;

        private final BloomFilter<Value> ctxFilter;

        private final Value[] data;

        private int index;

        DeltaModel(final QuadModel model, final int size) {

            // Store model and size parameters
            this.model = model;
            this.size = size;

            // Create bloom filters, expecting 'size' insertions
            this.statementFilter = BloomFilter.create(Funnels.integerFunnel(), size * 2);
            this.subjFilter = BloomFilter.create(VALUE_FUNNEL, size);
            this.predFilter = BloomFilter.create(VALUE_FUNNEL, size / 16);
            this.objFilter = BloomFilter.create(VALUE_FUNNEL, size);
            this.ctxFilter = BloomFilter.create(VALUE_FUNNEL, size / 16);

            // Initialize storage for delta statements
            this.data = new Value[4 * size];
            this.index = 0;
        }

        void include(final Resource subj, final URI pred, final Value obj, final Resource ctx) {

            // Store statement
            this.data[this.index++] = subj;
            this.data[this.index++] = pred;
            this.data[this.index++] = obj;
            this.data[this.index++] = ctx;

            // Update bloom filters
            this.statementFilter.put(hash(subj, pred, obj, ctx));
            this.subjFilter.put(subj);
            this.predFilter.put(pred);
            this.objFilter.put(obj);
            this.ctxFilter.put(ctx);
        }

        @Override
        protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, @Nullable final Resource ctx) {

            // Return 0 if view is empty or pattern is not compatible with SPOC bloom filters
            if (this.size == 0 || subj != null && !this.subjFilter.mightContain(subj) //
                    || pred != null && !this.predFilter.mightContain(pred) //
                    || obj != null && !this.objFilter.mightContain(obj) //
                    || ctx != null && !this.ctxFilter.mightContain(ctx)) {
                return 0;
            }

            // Otherwise, simply delegate to the wrapped model, limiting the result to this size
            return Math.min(this.size, this.model.sizeEstimate(subj, pred, obj,
                    ctx == null ? CTX_ANY : new Resource[] { ctx }));
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable final Resource subj,
                @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {

            // In case of a wildcard <?s ?p ?o ?c> returns all the statements in the delta
            if (subj == null && pred == null && obj == null && ctxs.length == 0) {
                return new Iterator<Statement>() {

                    private int offset = 0;

                    @Override
                    public boolean hasNext() {
                        return this.offset < DeltaModel.this.data.length;
                    }

                    @Override
                    public Statement next() {
                        final Resource subj = (Resource) DeltaModel.this.data[this.offset++];
                        final URI pred = (URI) DeltaModel.this.data[this.offset++];
                        final Value obj = DeltaModel.this.data[this.offset++];
                        final Resource ctx = (Resource) DeltaModel.this.data[this.offset++];
                        return ctx == null ? Statements.VALUE_FACTORY.createStatement(subj, pred,
                                obj) : Statements.VALUE_FACTORY.createStatement(subj, pred, obj,
                                ctx);
                    }

                };
            }

            // Return empty iterator if view is empty or pattern not compatible with SPO filters
            if (this.size == 0 || subj != null && !this.subjFilter.mightContain(subj) //
                    || pred != null && !this.predFilter.mightContain(pred) //
                    || obj != null && !this.objFilter.mightContain(obj)) {
                return Collections.emptyIterator();
            }

            // In case one or more context constants were specified, check at least one is in the
            // context bloom filter and return an empty iterator otherwise
            if (ctxs.length > 0) {
                boolean mightContainContext = false;
                for (final Resource ctx : ctxs) {
                    if (this.ctxFilter.mightContain(ctx)) {
                        mightContainContext = true;
                        break;
                    }
                }
                if (!mightContainContext) {
                    return Collections.emptyIterator();
                }
            }

            // Handle two cases
            final int estimate = this.model.sizeEstimate(subj, pred, obj, ctxs);
            if (estimate < this.size) {

                // (1) Estimated size (in wrapped model) less than delta size: delegate to the
                // model, filtering its output to ensure that returned statements belong to delta
                return Iterators.filter(
                        this.model.iterator(subj, pred, obj, ctxs),
                        (final Statement stmt) -> {
                            return this.statementFilter.mightContain(hash(stmt.getSubject(),
                                    stmt.getPredicate(), stmt.getObject(), stmt.getContext()));
                        });

            } else {

                // (2) Estimated size greater than delta size: it is better to iterate over the
                // statements in the delta (as stored in single array) and filter them
                return Iterators.filter(doIterator(null, null, null, CTX_ANY), (
                        final Statement stmt) -> {
                    if (subj != null && !subj.equals(stmt.getSubject()) //
                            || pred != null && !pred.equals(stmt.getPredicate()) //
                            || obj != null && !obj.equals(stmt.getObject())) {
                        return false;
                    }
                    if (ctxs.length == 0) {
                        return true;
                    }
                    for (final Resource ctx : ctxs) {
                        if (Objects.equals(ctx, stmt.getContext())) {
                            return true;
                        }
                    }
                    return false;
                });
            }
        }

        @Override
        protected Set<Namespace> doGetNamespaces() {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected Namespace doGetNamespace(final String prefix) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected Namespace doSetNamespace(final String prefix, final String name) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected int doSize(final Resource subj, final URI pred, final Value obj,
                final Resource[] ctxs) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected boolean doAdd(final Resource subj, final URI pred, final Value obj,
                final Resource[] ctxs) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected boolean doRemove(final Resource subj, final URI pred, final Value obj,
                final Resource[] ctxs) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected Value doNormalize(final Value value) {
            throw new UnsupportedOperationException(); // not invoked
        }

        private static int hash(final Value subj, final Value pred, final Value obj,
                final Value ctx) {
            return 6661 * hash(subj) + 961 * hash(pred) + 31 * hash(obj) + hash(ctx);
        }

        private static int hash(@Nullable final Value value) {
            return value == null ? 0 : value.hashCode();
        }

    }

    private static final class DeltaModel2 extends QuadModel {

        private static final long serialVersionUID = 1;

        private static final int PROPERTY_MASK_SIZE = 8 * 8 * 1024 - 1;

        private static final int TYPE_MASK_SIZE = 8 * 8 * 1024 - 1;

        private static final ArrayComparator<Value> VALUE_ARRAY_COMPARATOR = new ArrayComparator<Value>() {

            @Override
            public int size() {
                return 4;
            }

            @Override
            public int compare(final Value[] leftArray, final int leftIndex,
                    final Value[] rightArray, final int rightIndex) {
                // POSC order
                int result = hash(leftArray[leftIndex + 1]) - hash(rightArray[rightIndex + 1]);
                if (result == 0) {
                    result = hash(leftArray[leftIndex + 2]) - hash(rightArray[rightIndex + 2]);
                    if (result == 0) {
                        result = hash(leftArray[leftIndex]) - hash(rightArray[rightIndex]);
                        if (result == 0) {
                            result = hash(leftArray[leftIndex + 3])
                                    - hash(rightArray[rightIndex + 3]);
                        }
                    }
                }
                return result;
            }

        };

        private final QuadModel model;

        private final int size;

        private final BitSet propertyMask;

        private final BitSet typeMask;

        private final int typeHash;

        private final Value[] data;

        private int index;

        DeltaModel2(final QuadModel model, final int size) {

            // Store model and size parameters
            this.model = model;
            this.size = size;

            // Initialize bitmasks for properties and types
            this.propertyMask = new BitSet(PROPERTY_MASK_SIZE);
            this.typeMask = new BitSet(TYPE_MASK_SIZE);
            this.typeHash = hash(model.normalize(RDF.TYPE));

            // Initialize storage for delta statements
            this.data = new Value[4 * size];
            this.index = 0;
        }

        void include(final Resource subj, final URI pred, final Value obj, final Resource ctx) {

            // Store statement
            this.data[this.index++] = subj;
            this.data[this.index++] = pred;
            this.data[this.index++] = obj;
            this.data[this.index++] = ctx;

            // Update masks
            final int predHash = hash(pred);
            this.propertyMask.set(predHash % PROPERTY_MASK_SIZE);
            if (predHash == this.typeHash) {
                final int objHash = hash(obj);
                this.typeMask.set(objHash % TYPE_MASK_SIZE);
            }

            // Sort statements at the end
            if (this.index == this.data.length) {
                Sorting.sort(VALUE_ARRAY_COMPARATOR, this.data);
            }
        }

        @Override
        protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, @Nullable final Resource ctx) {

            // Return 0 if view is empty
            if (this.size == 0) {
                return 0;
            }

            // Check masks, if possible
            if (pred != null) {
                final int predHash = hash(pred);
                if (!this.propertyMask.get(predHash % PROPERTY_MASK_SIZE)) {
                    return 0;
                }
                if (obj != null && predHash == this.typeHash) {
                    final int objHash = hash(obj);
                    if (!this.typeMask.get(objHash % TYPE_MASK_SIZE)) {
                        return 0;
                    }
                }
            }

            // Otherwise, simply delegate to the wrapped model, limiting the result to this size
            return Math.min(this.size, this.model.sizeEstimate(subj, pred, obj,
                    ctx == null ? CTX_ANY : new Resource[] { ctx }));
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable final Resource subj,
                @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {

            // In case of a wildcard <?s ?p ?o ?c> returns all the statements in the delta
            if (subj == null && pred == null && obj == null && ctxs.length == 0) {
                return new Iterator<Statement>() {

                    private int offset = 0;

                    @Override
                    public boolean hasNext() {
                        return this.offset < DeltaModel2.this.data.length;
                    }

                    @Override
                    public Statement next() {
                        final Statement stmt = statementAt(this.offset);
                        this.offset += 4;
                        return stmt;
                    }

                };
            }

            // Compute prefix
            final Value[] prefix = prefixFor(subj, pred, obj);

            // Delegate to model without filtering (thus going towards a naive approach) in case
            // there is no way to exploit the order of quads and their number in the model is less
            // than the delta size
            if (prefix.length == 0) {
                final int estimate = this.model.sizeEstimate(subj, pred, obj, ctxs);
                if (estimate < this.size) {
                    return this.model.iterator(subj, pred, obj, ctxs);
                }
            }

            // Compute start index in the value array
            final int startIndex = indexOf(prefix, 0, this.data.length);
            if (startIndex < 0) {
                return Collections.emptyIterator();
            }

            // Otherwise, iterate starting at computed index, stopping when prefix does not match
            return new Iterator<Statement>() {

                private int offset = startIndex;

                private Statement next = null;

                @Override
                public boolean hasNext() {
                    if (this.next != null) {
                        return true;
                    }
                    final Value[] data = DeltaModel2.this.data;
                    while (this.offset < data.length) {
                        if (prefixCompare(prefix, this.offset) != 0) {
                            this.offset = data.length;
                            return false;
                        }
                        if ((subj == null || subj.equals(data[this.offset]))
                                && (pred == null || pred.equals(data[this.offset + 1]))
                                && (obj == null || obj.equals(data[this.offset + 2]))) {
                            if (ctxs.length == 0) {
                                this.next = statementAt(this.offset);
                                this.offset += 4;
                                return true;
                            }
                            final Resource c = (Resource) data[this.offset + 3];
                            for (final Resource ctx : ctxs) {
                                if (Objects.equals(c, ctx)) {
                                    this.next = statementAt(this.offset);
                                    this.offset += 4;
                                    return true;
                                }
                            }
                        }
                        this.offset += 4;
                    }
                    return false;
                }

                @Override
                public Statement next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    final Statement stmt = this.next;
                    this.next = null;
                    return stmt;
                }

            };
        }

        @Override
        protected Set<Namespace> doGetNamespaces() {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected Namespace doGetNamespace(final String prefix) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected Namespace doSetNamespace(final String prefix, final String name) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected int doSize(final Resource subj, final URI pred, final Value obj,
                final Resource[] ctxs) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected boolean doAdd(final Resource subj, final URI pred, final Value obj,
                final Resource[] ctxs) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected boolean doRemove(final Resource subj, final URI pred, final Value obj,
                final Resource[] ctxs) {
            throw new UnsupportedOperationException(); // not invoked
        }

        @Override
        protected Value doNormalize(final Value value) {
            throw new UnsupportedOperationException(); // not invoked
        }

        private Value[] prefixFor(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj) {

            // Compute prefix length
            int prefixLength = 0;
            if (pred != null) {
                ++prefixLength;
                if (obj != null) {
                    ++prefixLength;
                    if (subj != null) {
                        ++prefixLength;
                    }
                }
            }

            // Compute prefix
            final Value[] prefix = new Value[prefixLength];
            if (prefixLength >= 1) {
                prefix[0] = pred;
            }
            if (prefixLength >= 2) {
                prefix[1] = obj;
            }
            if (prefixLength >= 3) {
                prefix[2] = subj;
            }
            return prefix;
        }

        private int prefixCompare(final Value[] prefix, final int offset) {
            int result = 0;
            if (prefix.length >= 1) {
                result = hash(this.data[offset + 1]) - hash(prefix[0]);
                if (result == 0 && prefix.length >= 2) {
                    result = hash(this.data[offset + 2]) - hash(prefix[1]);
                    if (result == 0 && prefix.length >= 3) {
                        result = hash(this.data[offset]) - hash(prefix[2]);
                    }
                }
            }
            return result;
        }

        private Statement statementAt(final int offset) {
            final Resource subj = (Resource) DeltaModel2.this.data[offset];
            final URI pred = (URI) DeltaModel2.this.data[offset + 1];
            final Value obj = DeltaModel2.this.data[offset + 2];
            final Resource ctx = (Resource) DeltaModel2.this.data[offset + 3];
            return ctx == null ? Statements.VALUE_FACTORY.createStatement(subj, pred, obj)
                    : Statements.VALUE_FACTORY.createStatement(subj, pred, obj, ctx);
        }

        private int indexOf(final Value[] prefix, final int lo, final int hi) {
            if (lo >= hi) {
                return -1;
            }
            if (prefix.length == 0) {
                return lo;
            }
            final int mid = (lo >>> 2) + (hi >>> 2) >>> 1 << 2;
            final int c = prefixCompare(prefix, mid);
            if (c < 0) {
                return indexOf(prefix, mid + 4, hi);
            } else if (c > 0) {
                return indexOf(prefix, lo, mid);
            } else if (mid > lo) {
                final int index = indexOf(prefix, lo, mid);
                return index >= 0 ? index : mid;
            }
            return mid;
        }

        private static int hash(@Nullable final Value value) {
            return value == null ? 0 : value.hashCode() & 0x7FFFFFFF;
        }

    }

}
