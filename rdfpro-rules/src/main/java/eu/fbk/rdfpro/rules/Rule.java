package eu.fbk.rdfpro.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStatistics;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.queryrender.sparql.SparqlTupleExprRenderer;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;

import eu.fbk.rdfpro.rules.model.QuadModel;
import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.rules.util.SPARQLRenderer;
import eu.fbk.rdfpro.rules.util.StatementHandler;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

/**
 * Rule definition.
 */
public final class Rule implements Comparable<Rule> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Rule.class);

    private static final AtomicLong ID_COUNTER = new AtomicLong(0L);

    private final URI id;

    private final boolean fixpoint;

    private final int phase;

    @Nullable
    private final TupleExpr deleteExpr;

    @Nullable
    private final TupleExpr insertExpr;

    @Nullable
    private final TupleExpr whereExpr;

    @Nullable
    private transient List<String> commonVariables;

    @Nullable
    private transient Set<StatementPattern> deletePatterns;

    @Nullable
    private transient Set<StatementPattern> insertPatterns;

    @Nullable
    private transient Set<StatementPattern> wherePatterns;

    @Nullable
    private transient Collector collector;

    private transient byte simple; // 0 = not computed, 1 = true, -1 = false

    private transient byte streamable; // 0 = not computed, 1 = true, -1 = false

    private transient byte safe; // 0 = not computed, 1 = true, -1 = false

    private transient byte specific; // 0 = not computed, 1 = true, -1 = false

    /**
     * Creates a new rule.
     *
     * @param id
     *            the rule ID, not null
     * @param fixpoint
     *            true, if the rule should be evaluated with a fixpoint semantics
     * @param phase
     *            the evaluation phase associated to this rule (defaults to zero)
     * @param deleteExpr
     *            the optional DELETE expression; if present, must be a BGP
     * @param insertExpr
     *            the optional INSERT expression; if present, must be a BGP
     * @param whereExpr
     *            the optional WHERE expression (absent in case of rules asserting axiomatic
     *            quads)
     */
    public Rule(final URI id, final boolean fixpoint, final int phase,
            @Nullable final TupleExpr deleteExpr, @Nullable final TupleExpr insertExpr,
            @Nullable final TupleExpr whereExpr) {

        Objects.requireNonNull(id, "No rule ID specified");

        this.id = id;
        this.fixpoint = fixpoint;
        this.phase = phase;
        this.deleteExpr = Algebra.normalizeVars(deleteExpr);
        this.insertExpr = Algebra.normalizeVars(insertExpr);
        this.whereExpr = Algebra.pushFilters(whereExpr);
        this.commonVariables = null;
        this.simple = 0;
        this.safe = 0;

        Algebra.internStrings(this.deleteExpr);
        Algebra.internStrings(this.insertExpr);
        Algebra.internStrings(this.whereExpr);

        Preconditions.checkArgument(this.deleteExpr == null || Algebra.isBGP(this.deleteExpr));
        Preconditions.checkArgument(this.insertExpr == null || Algebra.isBGP(this.insertExpr));
    }

    /**
     * Returns the rule ID.
     *
     * @return the rule ID
     */
    public URI getID() {
        return this.id;
    }

    /**
     * Returns true if the rule must be evaluated using a fixpoint semantics
     *
     * @return true, for fixpoint rules
     */
    public boolean isFixpoint() {
        return this.fixpoint;
    }

    /**
     * Returns the evaluation phase this rule is associated to. Evaluation phases with lower index
     * are executed first.
     *
     * @return an integer (possibly negative) identifying the evaluation phase.
     */
    public int getPhase() {
        return this.phase;
    }

    /**
     * Returns the optional DELETE expression
     *
     * @return the DELETE expression if present, null otherwise
     */
    @Nullable
    public TupleExpr getDeleteExpr() {
        return this.deleteExpr;
    }

    /**
     * Returns the optional INSERT expression
     *
     * @return the INSERT expression if present, null otherwise
     */
    @Nullable
    public TupleExpr getInsertExpr() {
        return this.insertExpr;
    }

    /**
     * Returns the optional WHERE expression.
     *
     * @return the WHERE expression if present; null otherwise
     */
    @Nullable
    public TupleExpr getWhereExpr() {
        return this.whereExpr;
    }

    /**
     * Returns the statement patterns in the DELETE expression, if any.
     *
     * @return a non-null set with the statement patterns in the DELETE expression
     */
    public Set<StatementPattern> getDeletePatterns() {
        if (this.deletePatterns == null) {
            this.deletePatterns = this.deleteExpr == null ? ImmutableSet.of() : ImmutableSet
                    .copyOf(Algebra.extractNodes(this.deleteExpr, StatementPattern.class, null,
                            null));
        }
        return this.deletePatterns;
    }

    /**
     * Returns the statement patterns in the INSERT expression, if any.
     *
     * @return a non-null set with the statement patterns in the INSERT expression
     */
    public Set<StatementPattern> getInsertPatterns() {
        if (this.insertPatterns == null) {
            this.insertPatterns = this.insertExpr == null ? ImmutableSet.of() : ImmutableSet
                    .copyOf(Algebra.extractNodes(this.insertExpr, StatementPattern.class, null,
                            null));
        }
        return this.insertPatterns;
    }

    /**
     * Returns the statement patterns in the WHERE expression, if any.
     *
     * @return a non-null set with the statement patterns in the WHERE expression
     */
    public Set<StatementPattern> getWherePatterns() {
        if (this.wherePatterns == null) {
            this.wherePatterns = this.whereExpr == null ? ImmutableSet.of() : ImmutableSet
                    .copyOf(Algebra.extractNodes(this.whereExpr, StatementPattern.class, null,
                            null));
        }
        return this.wherePatterns;
    }

    /**
     * Returns a sorted list with the variables returned by the WHERE expression that are
     * referenced either in the DELETE or in the INSERT expressions.
     *
     * @return a sorted list of common variables between the WHERE expression and the DELETE and
     *         INSERT expressions
     */
    public List<String> getCommonVariables() {
        if (this.commonVariables == null) {
            if (this.deleteExpr == null && this.insertExpr == null || this.whereExpr == null) {
                this.commonVariables = ImmutableList.of();
            } else {
                final Set<String> vars = new HashSet<>();
                if (this.deleteExpr != null) {
                    vars.addAll(Algebra.extractVariables(this.deleteExpr, true));
                }
                if (this.insertExpr != null) {
                    vars.addAll(Algebra.extractVariables(this.insertExpr, true));
                }
                vars.retainAll(Algebra.extractVariables(this.whereExpr, true));
                this.commonVariables = Ordering.natural().immutableSortedCopy(vars);
            }
        }
        return this.commonVariables;
    }

    /**
     * Returns true if the rule is safe, i.e., if all the variables referenced in the DELETE and
     * INSERT expressions are present in the bindings produced by the WHERE expression. Only safe
     * rules can be evaluated. Non-safe rules are however allowed as they can be transformed into
     * safe rules through variable binding (by calling method
     * {@link #rewriteVariables(BindingSet)}).
     *
     * @return true, if the rule is safe
     */
    public boolean isSafe() {
        if (this.safe == 0) {
            if (this.deleteExpr == null && this.insertExpr == null) {
                this.safe = 1;
            } else {
                final Set<String> vars = new HashSet<>();
                if (this.deleteExpr != null) {
                    vars.addAll(Algebra.extractVariables(this.deleteExpr, true));
                }
                if (this.insertExpr != null) {
                    vars.addAll(Algebra.extractVariables(this.insertExpr, true));
                }
                if (this.whereExpr != null) {
                    vars.removeAll(Algebra.extractVariables(this.whereExpr, true));
                }
                this.safe = (byte) (vars.isEmpty() ? 1 : -1);
            }
        }
        return this.safe == 1;
    }

    /**
     * Returns true if the rule is simple, i.e., if the WHERE expression consists only of BGPs,
     * FILTERs (without the EXISTS construct) and outer level BINDs. Simple rules can be evaluated
     * more efficiently.
     *
     * @return true, if the rule is simple
     */
    public boolean isSimple() {
        if (this.simple == 0) {
            final AtomicBoolean simple = new AtomicBoolean(true);
            if (this.whereExpr != null) {
                this.whereExpr.visit(new QueryModelVisitorBase<RuntimeException>() {

                    @Override
                    protected void meetNode(final QueryModelNode node) throws RuntimeException {
                        if (!simple.get()) {
                            return;
                        } else if (node instanceof StatementPattern || node instanceof Join
                                || node instanceof Filter || node instanceof ValueExpr
                                && !(node instanceof Exists) || node instanceof ExtensionElem) {
                            super.meetNode(node);
                        } else if (node instanceof Extension) {
                            for (QueryModelNode n = node.getParentNode(); n != null; n = n
                                    .getParentNode()) {
                                if (!(n instanceof Extension)) {
                                    simple.set(false);
                                    return;
                                }
                            }
                            super.meetNode(node);
                        } else {
                            simple.set(false);
                            return;
                        }
                    }

                });
            }
            this.simple = (byte) (simple.get() ? 1 : -1);
        }
        return this.simple == 1;
    }

    /**
     * Returns true if the rule can be evaluated in a streaming way. A rule is streamable if: (i)
     * it is simple (see {@link #isSimple()}); (ii) its where part contains at most one statement
     * pattern; and (iii) the delete part is missing or it contains exactly the statement pattern
     * of the where part (which must be non empty).
     *
     * @return true, if the rule is streamable
     */
    public boolean isStreamable() {
        if (!isSimple()) {
            return false;
        }
        if (this.streamable == 0) {
            boolean streamable = false;
            final Set<StatementPattern> wherePatterns = getWherePatterns();
            if (wherePatterns.size() < 1) {
                if (this.deleteExpr == null) {
                    streamable = true;
                } else if (wherePatterns.size() == 1) {
                    final List<StatementPattern> deletePatterns = Algebra.extractNodes(
                            this.deleteExpr, StatementPattern.class, null, null);
                    if (deletePatterns.size() == 1 && wherePatterns.containsAll(deletePatterns)) {
                        streamable = true;
                    }
                }
            }
            this.streamable = (byte) (streamable ? 1 : -1);
        }
        return this.streamable == 1;
    }

    /**
     * Returns true if the rule matches only specific types of statements. A rule is specific if
     * its where part is null or it does not contain a statement pattern that could match any
     * statement. Specific rules might be evaluated only on a subset of statements (the ones that
     * could be matched) obtaining the same results.
     *
     * @return true, if the rule is specific
     */
    public boolean isSpecific() {
        if (this.specific == 0) {
            boolean specific = true;
            for (final StatementPattern pattern : getWherePatterns()) {
                if (!pattern.getSubjectVar().hasValue()
                        && !pattern.getPredicateVar().hasValue()
                        && !pattern.getObjectVar().hasValue()
                        && (pattern.getContextVar() == null || !pattern.getContextVar().hasValue())) {
                    specific = false;
                    break;
                }
            }
            this.specific = (byte) (specific ? 1 : -1);
        }
        return this.specific == 1;
    }

    /**
     * Returns true if the rule may be activated given the dataset statistics supplied. Note that
     * false positive answers may be returned, i.e, if the result is true it is not guaranteed
     * that the rule will fire, whereas if the result is false the rule is guaranteed not to fire
     * (under the assumption that statistics returned by {@code statisitcs} are 0.0 only if no
     * triple is returned for a certain statement pattern).
     *
     * @param statistics
     *            the statistics object
     * @return true, if the rule might fire given the supplied statistics
     */
    public boolean mightActivate(final EvaluationStatistics statistics) {
        if (isSimple() && !getWherePatterns().isEmpty()) {
            for (final StatementPattern pattern : getWherePatterns()) {
                if (statistics.getCardinality(pattern) == 0.0) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Rewrites the rule according to the GLOBAL graph inference mode, using the global graph URI
     * specified. The returned rule: (i) has a new ID generated based on the ID of this rule; (ii)
     * matches quads in any graph in the WHERE part; (iii) insert quads in the specified global
     * graph; and (iv) deletes quads from any graph.
     *
     * @param globalGraph
     *            the URI of the global graph where to insert new quads; if null, quads will be
     *            inserted in the default graph {@code sesame:nil}
     * @return the rewritten rule
     */
    public Rule rewriteGlobalGM(@Nullable final URI globalGraph) {
        final Var graphVar = globalGraph != null ? newConstVar(globalGraph) : null;
        final TupleExpr newDeleteExpr = Algebra.rewriteGraph(this.deleteExpr, null);
        final TupleExpr newInsertExpr = Algebra.rewriteGraph(this.insertExpr, graphVar);
        final TupleExpr newWhereExpr = Algebra.rewriteGraph(this.whereExpr, null);
        return new Rule(newID(this.id.stringValue()), this.fixpoint, this.phase, newDeleteExpr,
                newInsertExpr, newWhereExpr);
    }

    /**
     * Rewrites the rule according to the SEPARATE graph inference mode. The returned rule: (i)
     * has a new ID generated based on the ID of this rule; and (ii) operates on a per-graph
     * basis, i.e., for each graph, the WHERE clause is applied and its results are used to
     * evaluate the DELETE and INSERT clauses on the very same graph.
     *
     * @return the rewritten rule
     */
    public Rule rewriteSeparateGM() {

        // Extract all the variables used in the rule
        final Set<String> vars = new HashSet<String>();
        vars.addAll(Algebra.extractVariables(this.deleteExpr, false));
        vars.addAll(Algebra.extractVariables(this.insertExpr, false));
        vars.addAll(Algebra.extractVariables(this.whereExpr, false));

        // Select a fresh graph variable
        String graphVarName = "g";
        int index = 0;
        while (vars.contains(graphVarName)) {
            graphVarName = "g" + index++;
        }
        final Var graphVar = new Var(graphVarName);

        // Rewrite the rule
        final TupleExpr newDeleteExpr = Algebra.rewriteGraph(this.deleteExpr, graphVar);
        final TupleExpr newInsertExpr = Algebra.rewriteGraph(this.insertExpr, graphVar);
        final TupleExpr newWhereExpr = Algebra.rewriteGraph(this.whereExpr, graphVar);
        return new Rule(newID(this.id.stringValue()), this.fixpoint, this.phase, newDeleteExpr,
                newInsertExpr, newWhereExpr);
    }

    /**
     * Rewrites the rule according to the STAR graph inference mode, using the global graph URI
     * supplied. The returned rule: (i) has a new ID generated based on the ID of this rule; (ii)
     * operates on a per-graph basis similarly to {@link #rewriteSeparateGM()}, however
     * 'importing' (as far as matching in the WHERE clause is concerned) also quads in the global
     * graph; and (iii) in case the WHERE clause is missing or a match is found on quads in the
     * global graph, deletions and insertions are performed on the global graph itself (this can
     * be useful to setup the global graph 'before' applying rules on the other graphs)
     *
     * @param globalGraph
     *            the URI of the global graph whose quads are 'imported' in other graphs; if null,
     *            the default graph {@code sesame:nil} will be used
     * @return the rewritten rule
     */
    public Rule rewriteStarGM(@Nullable final URI globalGraph) {

        // Extract all the variables used in the rule
        final Set<String> vars = new HashSet<String>();
        vars.addAll(Algebra.extractVariables(this.deleteExpr, false));
        vars.addAll(Algebra.extractVariables(this.insertExpr, false));
        vars.addAll(Algebra.extractVariables(this.whereExpr, false));

        // Select a variable prefix never used in the rule
        String candidatePrefix = "g";
        outer: while (true) {
            for (final String var : vars) {
                if (var.startsWith(candidatePrefix)) {
                    candidatePrefix = "_" + candidatePrefix;
                    continue outer;
                }
            }
            break;
        }
        final String prefix = candidatePrefix;

        // Rewrite the rule
        final URI global = globalGraph != null ? globalGraph : SESAME.NIL;
        TupleExpr newDeleteExpr = this.deleteExpr;
        TupleExpr newInsertExpr = this.insertExpr;
        TupleExpr newWhereExpr = this.whereExpr;
        if (this.whereExpr == null) {
            newDeleteExpr = Algebra.rewriteGraph(newDeleteExpr, newConstVar(global));
            newInsertExpr = Algebra.rewriteGraph(newInsertExpr, newConstVar(global));
        } else {
            final AtomicInteger counter = new AtomicInteger(0);
            final List<ValueExpr> filterGraphVars = new ArrayList<>();
            final List<ValueExpr> bindGraphVars = new ArrayList<>();
            filterGraphVars.add(newConstVar(global));
            bindGraphVars.add(newConstVar(global));
            newDeleteExpr = Algebra.rewriteGraph(newDeleteExpr, new Var(prefix));
            newInsertExpr = Algebra.rewriteGraph(newInsertExpr, new Var(prefix));
            newWhereExpr = newWhereExpr.clone();
            newWhereExpr.visit(new QueryModelVisitorBase<RuntimeException>() {

                @Override
                public void meet(final StatementPattern pattern) throws RuntimeException {
                    final Var graphVar = new Var(prefix + counter.getAndIncrement());
                    pattern.setContextVar(graphVar);
                    filterGraphVars.add(graphVar.clone());
                    bindGraphVars.add(graphVar.clone());
                }

            });
            newWhereExpr = new Filter(newWhereExpr, new Compare(new FunctionCall(
                    RR.STAR_SELECT_GRAPH.stringValue(), filterGraphVars), new Var("_const-"
                    + UUID.randomUUID(), RDF.NIL), CompareOp.NE));
            newWhereExpr = new Extension(newWhereExpr, new ExtensionElem(new FunctionCall(
                    RR.STAR_SELECT_GRAPH.stringValue(), bindGraphVars), prefix));
        }
        return new Rule(newID(this.id.stringValue()), this.fixpoint, this.phase, newDeleteExpr,
                newInsertExpr, newWhereExpr);
    }

    /**
     * Rewrites the rule by replacing selected variables with constant values as dictated by the
     * supplied bindings. In case the rewriting is unnecessary this rule is returned unchanged;
     * otherwise, a new, rewritten rule with a different ID (based on the ID of this rule) is
     * produced.
     *
     * @param bindings
     *            the variable = value bindings to use for the rewriting; if null or empty, no
     *            rewriting will take place
     * @return either the rewritten rule of this rule, if rewriting is unnecessary
     */
    public Rule rewriteVariables(@Nullable final BindingSet bindings) {
        if (bindings == null || bindings.size() == 0) {
            return this;
        }
        final TupleExpr newDeleteExpr = Algebra.rewrite(this.deleteExpr, bindings);
        final TupleExpr newInsertExpr = Algebra.rewrite(this.insertExpr, bindings);
        final TupleExpr newWhereExpr = Algebra.rewrite(this.whereExpr, bindings);
        return new Rule(newID(this.id.stringValue()), this.fixpoint, this.phase, newDeleteExpr,
                newInsertExpr, newWhereExpr);
    }

    /**
     * Merges rules with the same WHERE expression, priority and fixpoint flag. A merged rule,
     * with a fresh ID, is produced for each cluster of rules having the same values of these
     * attributes. The DELETE and INSERT expressions of the merged rule are obtained by
     * concatenating the DELETE and INSERT expressions of the rules in the cluster.
     *
     * @param rules
     *            the rules to merge
     * @return a list with the merged rules
     */
    public static List<Rule> mergeSameWhereExpr(final Iterable<Rule> rules) {

        // Group together rules with the same fixpoint, phase and WHERE expression
        final Map<List<Object>, List<Rule>> clusters = new HashMap<>();
        for (final Rule rule : rules) {
            final List<Object> key = Arrays.asList(rule.fixpoint, rule.phase, rule.whereExpr);
            List<Rule> cluster = clusters.get(key);
            if (cluster == null) {
                cluster = new ArrayList<>();
                clusters.put(key, cluster);
            }
            cluster.add(rule);
        }

        // Create a merged rule for each cluster obtained before
        final List<Rule> mergedRules = new ArrayList<>();
        for (final List<Rule> cluster : clusters.values()) {
            final Rule first = cluster.get(0);
            final String namespace = first.getID().getNamespace();
            final Set<String> names = new TreeSet<>();
            TupleExpr newDeleteExpr = null;
            TupleExpr newInsertExpr = null;
            for (int i = 0; i < cluster.size(); ++i) {
                final Rule rule = cluster.get(i);
                final String s = rule.getID().getLocalName();
                final int index = s.indexOf("__");
                names.add(index < 0 ? s : s.substring(0, index));
                newDeleteExpr = newDeleteExpr == null ? rule.deleteExpr //
                        : new Join(newDeleteExpr, rule.deleteExpr);
                newInsertExpr = newInsertExpr == null ? rule.insertExpr //
                        : new Join(newInsertExpr, rule.insertExpr);
            }
            final URI id = newID(namespace + String.join("_", names));
            mergedRules.add(new Rule(id, first.fixpoint, first.phase, newDeleteExpr,
                    newInsertExpr, first.whereExpr));
        }
        return mergedRules;
    }

    public void collect(final BindingSet bindings, @Nullable final QuadModel model,
            @Nullable final StatementHandler deleteHandler,
            @Nullable final StatementHandler insertHandler) {

        if (this.collector == null) {
            this.collector = Collector.create(this);
        }
        this.collector.collect(bindings, model, deleteHandler, insertHandler);
    }

    public void evaluate(final QuadModel model, @Nullable final QuadModel deltaModel,
            @Nullable final StatementPattern deltaPattern,
            @Nullable final Supplier<StatementHandler> deleteSink,
            @Nullable final Supplier<StatementHandler> insertSink) {

        new Evaluation(this, model, deltaModel, deltaPattern, deleteSink, insertSink).run();
    }

    public static int evaluate(final Iterable<Rule> rules, final QuadModel model,
            @Nullable final QuadModel deltaModel,
            @Nullable final Supplier<StatementHandler> deleteSink,
            @Nullable final Supplier<StatementHandler> insertSink) {

        // Evaluate all rules in parallel, collecting produced quads in the two buffers
        int numVariants = 0;
        final List<Evaluation> tasks = new ArrayList<>();
        for (final Rule rule : rules) {
            if (deltaModel == null || rule.getWhereExpr() == null) {
                ++numVariants;
                final Evaluation task = new Evaluation(rule, model, null, null, deleteSink,
                        insertSink);
                if (task.isActivable()) {
                    tasks.add(task);
                }
            } else {
                for (final StatementPattern pattern : rule.getWherePatterns()) {
                    ++numVariants;
                    final Evaluation task = new Evaluation(rule, model, deltaModel, pattern,
                            deleteSink, insertSink);
                    if (task.isActivable()) {
                        tasks.add(task);
                    }
                }
            }
        }
        Collections.sort(tasks);
        Environment.run(tasks);
        return numVariants;
    }

    /**
     * {@inheritDoc} Rules with the same ID are equal. Otherwise, rules are sorted by phase (lower
     * phase index comes first), fixpoint flag (false = no fixpoint comes first) and finally ID.
     * This sorting is compatible with the order of execution of rules.
     */
    @Override
    public int compareTo(final Rule other) {
        final int idResult = Statements.valueComparator().compare(this.id, other.id);
        if (idResult == 0) {
            return 0; // Required for compatibility with equals
        }
        int result = this.phase - other.phase;
        if (result == 0) {
            result = this.fixpoint ? other.fixpoint ? 0 : 1 : other.fixpoint ? -1 : 0;
            if (result == 0) {
                result = idResult;
            }
        }
        return result;
    }

    /**
     * {@inheritDoc} Two rules are equal if they have the same ID.
     */
    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Rule)) {
            return false;
        }
        final Rule other = (Rule) object;
        return this.id.equals(other.id);
    }

    /**
     * {@inheritDoc} The returned hash code depends exclusively on the rule ID.
     */
    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    /**
     * {@inheritDoc} The returned string has the form
     * {@code ID (phase: N, fixpoint): DELETE ... INSERT ... WHERE ...}, where the
     * {@code fixpoint}, {@code DELETE ...}, {@code INSERT ...} and {@code WHERE ...} may be
     * present or absent based on the properties of the rule.
     */
    @Override
    public String toString() {
        try {
            final StringBuilder builder = new StringBuilder();
            builder.append(this.id instanceof BNode ? ((BNode) this.id).getID() : this.id
                    .getLocalName());
            builder.append(" (phase ").append(this.phase)
                    .append(this.fixpoint ? ", fixpoint):" : "):");
            if (this.deleteExpr != null) {
                builder.append(" DELETE ");
                builder.append(new SPARQLRenderer(Namespaces.DEFAULT.prefixMap(), false)
                        .renderTupleExpr(this.deleteExpr).replaceAll("[\n\r\t ]+", " "));
            }
            if (this.insertExpr != null) {
                builder.append(" INSERT ");
                builder.append(new SPARQLRenderer(Namespaces.DEFAULT.prefixMap(), false)
                        .renderTupleExpr(this.insertExpr).replaceAll("[\n\r\t ]+", " "));
            }
            if (this.whereExpr != null) {
                builder.append(" WHERE ");
                builder.append(new SPARQLRenderer(Namespaces.DEFAULT.prefixMap(), false)
                        .renderTupleExpr(this.whereExpr).replaceAll("[\n\r\t ]+", " "));
            }
            return builder.toString();
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Emits the RDF serialization of the rule. Emitted triples are placed in the default graph.
     *
     * @param output
     *            the collection where to add emitted RDF statements, not null
     * @return the supplied collection
     */
    public <T extends Collection<? super Statement>> T toRDF(final T output) {

        final ValueFactory vf = Statements.VALUE_FACTORY;
        output.add(vf.createStatement(this.id, RDF.TYPE, RR.RULE));
        output.add(vf.createStatement(this.id, RDF.TYPE, this.fixpoint ? RR.FIXPOINT_RULE
                : RR.NON_FIXPOINT_RULE));
        if (this.phase != 0) {
            output.add(vf.createStatement(this.id, RR.PHASE, vf.createLiteral(this.phase)));
        }
        try {
            if (this.deleteExpr != null) {
                output.add(vf.createStatement(this.id, RR.DELETE,
                        vf.createLiteral(new SparqlTupleExprRenderer().render(this.deleteExpr))));
            }
            if (this.insertExpr != null) {
                output.add(vf.createStatement(this.id, RR.INSERT,
                        vf.createLiteral(new SparqlTupleExprRenderer().render(this.insertExpr))));
            }
            if (this.whereExpr != null) {
                output.add(vf.createStatement(this.id, RR.WHERE,
                        vf.createLiteral(new SparqlTupleExprRenderer().render(this.whereExpr))));
            }
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
        return output;
    }

    /**
     * Parses all the rules contained in the supplied RDF statements.
     *
     * @param model
     *            the RDF statements, not null
     * @return an unsorted list containing the parsed rules
     */
    public static List<Rule> fromRDF(final Iterable<Statement> model) {

        // Load namespaces from model metadata, reusing default prefix/ns mappings
        final Map<String, String> namespaces = new HashMap<>(Namespaces.DEFAULT.uriMap());
        if (model instanceof Model) {
            for (final Namespace namespace : ((Model) model).getNamespaces()) {
                namespaces.put(namespace.getPrefix(), namespace.getName());
            }
        }
        for (final Statement stmt : model) {
            if (stmt.getSubject() instanceof URI && stmt.getObject() instanceof Literal
                    && stmt.getPredicate().equals(RR.PREFIX_PROPERTY)) {
                namespaces.put(stmt.getObject().stringValue(), stmt.getSubject().stringValue());
            }
        }

        // Use a 5-fields Object[] record to collect the attributes of each rule.
        // fields: 0 = fixpoint, 1 = phase, 2 = delete expr, 3 = insert expr, 4 = where expr
        final Map<URI, Object[]> records = new HashMap<>();

        // Scan the statements, extracting rule properties and populating the records map
        for (final Statement stmt : model) {
            try {
                if (stmt.getSubject() instanceof URI) {

                    // Extract relevant statement components
                    final URI subj = (URI) stmt.getSubject();
                    final URI pred = stmt.getPredicate();
                    final Value obj = stmt.getObject();

                    // Identify field and value (if any) of corresponding Object[] record
                    int field = -1;
                    Object value = null;
                    if (pred.equals(RDF.TYPE)) {
                        field = 0;
                        if (obj.equals(RR.FIXPOINT_RULE)) {
                            value = true;
                        } else if (obj.equals(RR.NON_FIXPOINT_RULE)) {
                            value = false;
                        }
                    } else if (pred.equals(RR.PHASE)) {
                        field = 1;
                        value = ((Literal) obj).intValue();
                    } else if (pred.equals(RR.DELETE)) {
                        field = 2;
                    } else if (pred.equals(RR.INSERT) || pred.equals(RR.HEAD)) {
                        field = 3;
                    } else if (pred.equals(RR.WHERE) || pred.equals(RR.BODY)) {
                        field = 4;
                    }
                    if (field == 2 || field == 3 || field == 4) {
                        value = Algebra.parseTupleExpr(stmt.getObject().stringValue(), null,
                                namespaces);
                    }

                    // Update Object[] records if the statement is about a rule
                    if (value != null) {
                        Object[] record = records.get(subj);
                        if (record == null) {
                            record = new Object[] { true, 0, null, null, null };
                            records.put(subj, record);
                        }
                        record[field] = value;
                    }
                }
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Invalid rule attribute in statement: " + stmt,
                        ex);
            }
        }

        // Generate the rules from parsed heads and bodies
        final List<Rule> rules = new ArrayList<>();
        for (final Map.Entry<URI, Object[]> entry : records.entrySet()) {
            final URI id = entry.getKey();
            final Object[] record = entry.getValue();
            rules.add(new Rule(id, (Boolean) record[0], (Integer) record[1],
                    (TupleExpr) record[2], (TupleExpr) record[3], (TupleExpr) record[4]));
        }
        return rules;
    }

    static URI newID(final String baseID) {
        final int index = baseID.indexOf("__");
        final String base = index < 0 ? baseID : baseID.substring(0, index);
        return Statements.VALUE_FACTORY.createURI(base + "__" + ID_COUNTER.incrementAndGet());
    }

    static Var newConstVar(final Value value) {
        return new Var("_const-" + UUID.randomUUID(), value);
    }

    Collector getCollector() {
        if (this.collector == null) {
            this.collector = Collector.create(this);
        }
        return this.collector;
    }

    private static final class Evaluation implements Runnable, Comparable<Evaluation> {

        private final Rule rule;

        private final QuadModel model;

        @Nullable
        private final QuadModel deltaModel;

        @Nullable
        private final StatementPattern deltaPattern;

        @Nullable
        private final Supplier<StatementHandler> deleteSink;

        @Nullable
        private final Supplier<StatementHandler> insertSink;

        private final EvaluationStatistics statistics;

        private final double cardinality;

        Evaluation(final Rule rule, final QuadModel model, @Nullable final QuadModel deltaModel,
                @Nullable final StatementPattern deltaPattern,
                @Nullable final Supplier<StatementHandler> deleteSink,
                @Nullable final Supplier<StatementHandler> insertSink) {

            this.rule = rule;
            this.deleteSink = deleteSink;
            this.insertSink = insertSink;
            this.model = model;
            this.deltaModel = deltaModel;
            this.deltaPattern = deltaPattern;
            this.statistics = deltaModel == null ? model.getEvaluationStatistics()
                    : newSemiNaiveEvaluationStatistics();
            this.cardinality = rule.whereExpr == null ? 1.0 : this.statistics
                    .getCardinality(rule.whereExpr);
        }

        boolean isActivable() {
            return this.cardinality != 0.0;
        }

        @Override
        public int compareTo(final Evaluation other) {
            return -Double.compare(this.cardinality, other.cardinality);
        }

        @Override
        public void run() {

            // Take a timestamp to measure rule evaluation time
            final long ts = System.currentTimeMillis();

            // Define counter for # activations
            int numActivations = 0;

            // Start evaluating the rule
            Iterator<BindingSet> iterator;
            if (this.cardinality == 0.0) {
                iterator = Collections.emptyIterator();
            } else if (this.rule.getWhereExpr() == null) {
                iterator = Collections.singleton(EmptyBindingSet.getInstance()).iterator();
            } else if (this.deltaModel == null) {
                iterator = this.model.evaluate(this.rule.getWhereExpr(), null, null);
            } else {
                iterator = Algebra.evaluateTupleExpr(this.rule.getWhereExpr(), null, null,
                        newSemiNaiveEvaluationStrategy(), this.statistics,
                        this.model.getValueNormalizer());
            }

            try {
                // Proceed only if there is some query result to process
                if (iterator.hasNext()) {

                    // Acquire a collector, normalizing its constants so to use the same Value
                    // objects in the model
                    final Collector collector = this.rule.getCollector().normalize(
                            this.model.getValueNormalizer());

                    // Allocate the delete handler, if possible
                    StatementHandler deleteHandler = null;
                    if (this.deleteSink != null && this.rule.getDeleteExpr() != null) {
                        deleteHandler = this.deleteSink.get();
                        deleteHandler.startRDF();
                    }

                    // Allocate the insert handler, if possible
                    StatementHandler insertHandler = null;
                    if (this.insertSink != null && this.rule.getInsertExpr() != null) {
                        insertHandler = this.insertSink.get();
                        insertHandler.startRDF();
                    }

                    // Scan the bindings returned by the WHERE part, using the collector to
                    // compute deleted/inserted quads
                    while (iterator.hasNext()) {
                        ++numActivations;
                        final BindingSet bindings = iterator.next();
                        collector.collect(bindings, this.model, deleteHandler, insertHandler);
                    }

                    // Signal completion to the delete handler, if any
                    if (deleteHandler != null) {
                        deleteHandler.endRDF();
                    }

                    // Signal completion to the insert handler, if any
                    if (insertHandler != null) {
                        insertHandler.endRDF();
                    }
                }
            } catch (final RDFHandlerException ex) {
                // Wrap and propagate
                throw new RuntimeException(ex);

            } finally {
                // Ensure to close the iterator (if it needs to be closed)
                IO.closeQuietly(iterator);
            }

            // Log relevant rule evaluation statistics
            if (LOGGER.isTraceEnabled()) {
                final String patternString = this.deltaPattern == null ? "" : " (delta pattern "
                        + Algebra.format(this.deltaPattern) + ")";
                LOGGER.trace("Rule {}{} evaluated in {} ms with {} activations", this.rule.getID()
                        .getLocalName(), patternString, System.currentTimeMillis() - ts,
                        numActivations);
            }
        }

        private EvaluationStrategy newSemiNaiveEvaluationStrategy() {

            final AtomicReference<TripleSource> selectedSource = new AtomicReference<>();

            final TripleSource baseSource = this.model.getTripleSource();
            final TripleSource deltaSource = this.deltaModel.getTripleSource();
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

            return new EvaluationStrategyImpl(semiNaiveSource, null,
                    Algebra.getFederatedServiceResolver()) {

                @Nullable
                private StatementPattern normalizedDeltaPattern = null;

                @Override
                public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
                        final StatementPattern pattern, final BindingSet bindings)
                        throws QueryEvaluationException {

                    if (this.normalizedDeltaPattern == null) {
                        if (pattern.equals(Evaluation.this.deltaPattern)) {
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

        private EvaluationStatistics newSemiNaiveEvaluationStatistics() {

            return new EvaluationStatistics() {

                @Override
                protected CardinalityCalculator createCardinalityCalculator() {
                    return new CardinalityCalculator() {

                        @Override
                        public final double getCardinality(final StatementPattern pattern) {
                            if (pattern.equals(Evaluation.this.deltaPattern)) {
                                return Evaluation.this.deltaModel.getEvaluationStatistics()
                                        .getCardinality(pattern);
                            } else {
                                return Evaluation.this.model.getEvaluationStatistics()
                                        .getCardinality(pattern);
                            }
                        }

                    };
                }

            };
        }

    }

    private static final class Collector {

        private static final int[] EMPTY_INDEXES = new int[0];

        private static final String[] EMPTY_VARS = new String[0];

        private static final Value[] EMPTY_CONSTANTS = new Value[0];

        private transient final int[] deleteIndexes;

        private transient final int[] insertIndexes;

        private transient final String[] commonVars;

        private transient final Value[] constants;

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
            // index (i + 1) of the variable in commonVars correspon+ding to that quad component,
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

        void collect(final BindingSet bindings, @Nullable final QuadModel model,
                @Nullable final StatementHandler deleteHandler,
                @Nullable final StatementHandler insertHandler) {

            // Transform the var=value bindings map to a value array, using the same variable
            // order of commonVars
            final Value[] commonValues = new Value[this.commonVars.length];
            for (int i = 0; i < commonValues.length; ++i) {
                commonValues[i] = bindings.getValue(this.commonVars[i]);
            }

            try {
                // Generate and send to the delete handler the quads that need to be removed. In
                // case
                // of quads in the default context, we need to explode them including all the
                // quads
                // with same SPO and different context (due to SESAME semantics 'default context =
                // merge of all other contexts').
                if (deleteHandler != null) {
                    for (int i = 0; i < this.deleteIndexes.length; i += 4) {
                        final Value subj = resolve(this.deleteIndexes[i], commonValues);
                        final Value pred = resolve(this.deleteIndexes[i + 1], commonValues);
                        final Value obj = resolve(this.deleteIndexes[i + 2], commonValues);
                        final Value ctx = resolve(this.deleteIndexes[i + 3], commonValues);
                        if (subj instanceof Resource && pred instanceof URI
                                && obj instanceof Value) {
                            if (ctx instanceof Resource || model == null) {
                                deleteHandler.handleStatement((Resource) subj, (URI) pred, obj,
                                        (Resource) ctx);
                            } else if (ctx == null) {
                                for (final Statement stmt : model.filter((Resource) subj,
                                        (URI) pred, obj)) {
                                    deleteHandler.handleStatement((Resource) subj, (URI) pred,
                                            obj, stmt.getContext());
                                }
                            }
                        }
                    }
                }

                // Generate and send to the insert handler the quads that need to be inserted
                if (insertHandler != null) {
                    for (int i = 0; i < this.insertIndexes.length; i += 4) {
                        final Value subj = resolve(this.insertIndexes[i], commonValues);
                        final Value pred = resolve(this.insertIndexes[i + 1], commonValues);
                        final Value obj = resolve(this.insertIndexes[i + 2], commonValues);
                        final Value ctx = resolve(this.insertIndexes[i + 3], commonValues);
                        if (subj instanceof Resource && pred instanceof URI
                                && obj instanceof Value
                                && (ctx == null || ctx instanceof Resource)) {
                            insertHandler.handleStatement((Resource) subj, (URI) pred, obj,
                                    (Resource) ctx);
                        }
                    }
                }

            } catch (final RDFHandlerException ex) {
                // Wrap and propagate
                throw new RuntimeException(ex);
            }
        }

        Collector normalize(final Function<Value, Value> normalizer) {

            // Replace each Value constant in the constants array with the corresponding Value
            // instance already stored in the quad model, if any. This may enable using identity
            // comparison of values instead of string comparison (faster!)
            int numReplacements = 0;
            final Value[] normalizedConstants = new Value[this.constants.length];
            for (int i = 0; i < this.constants.length; ++i) {
                normalizedConstants[i] = normalizer.apply(this.constants[i]);
                numReplacements += normalizedConstants[i] == this.constants[i] ? 0 : 1;
            }
            LOGGER.trace("{} constant values replaced during collector normalization",
                    numReplacements);

            // Return the collector with the same parameters except the normalized constant array
            return new Collector(this.deleteIndexes, this.insertIndexes, this.commonVars,
                    normalizedConstants);
        }

    }

}
