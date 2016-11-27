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
package eu.fbk.rdfpro.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.SameTerm;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverImpl;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.BindingAssigner;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.CompareOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConstantOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.DisjunctiveConstraintOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.FilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.IterativeEvaluationOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.OrderLimitOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryJoinOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.QueryModelNormalizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.sparql.AbstractASTVisitor;
import org.eclipse.rdf4j.query.parser.sparql.BaseDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.BlankNodeVarProcessor;
import org.eclipse.rdf4j.query.parser.sparql.DatasetDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.StringEscapesProcessor;
import org.eclipse.rdf4j.query.parser.sparql.TupleExprBuilder;
import org.eclipse.rdf4j.query.parser.sparql.WildcardProjectionProcessor;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTAskQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTConstructQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTDescribeQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTIRI;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTPrefixDecl;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQName;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQueryContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTSelectQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTServiceGraphPattern;
import org.eclipse.rdf4j.query.parser.sparql.ast.ParseException;
import org.eclipse.rdf4j.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.eclipse.rdf4j.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.eclipse.rdf4j.query.parser.sparql.ast.TokenMgrError;
import org.eclipse.rdf4j.query.parser.sparql.ast.VisitorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Algebra {

    private static final Logger LOGGER = LoggerFactory.getLogger(Algebra.class);

    private static final EvaluationStatistics DEFAULT_EVALUATION_STATISTICS = new EvaluationStatistics();

    private static final FederatedServiceResolverImpl FEDERATED_SERVICE_RESOLVER = //
            new FederatedServiceResolverImpl();

    private static final TripleSource EMPTY_TRIPLE_SOURCE = new TripleSource() {

        @Override
        public ValueFactory getValueFactory() {
            return Statements.VALUE_FACTORY;
        }

        @Override
        public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                final Resource subj, final IRI pred, final Value obj, final Resource... contexts)
                throws QueryEvaluationException {
            return new EmptyIteration<Statement, QueryEvaluationException>();
        }

    };

    private static final EvaluationStrategy EMPTY_EVALUATION_STRATEGY = new StrictEvaluationStrategy(
            Algebra.EMPTY_TRIPLE_SOURCE, Algebra.FEDERATED_SERVICE_RESOLVER);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                Algebra.FEDERATED_SERVICE_RESOLVER.shutDown();
            }

        });
    }

    public static FederatedServiceResolver getFederatedServiceResolver() {
        return Algebra.FEDERATED_SERVICE_RESOLVER;
    }

    public static TripleSource getEmptyTripleSource() {
        return Algebra.EMPTY_TRIPLE_SOURCE;
    }

    public static EvaluationStrategy getEvaluationStrategy(
            @Nullable final TripleSource tripleSource, @Nullable final Dataset dataset) {

        if (tripleSource != null) {
            return new StrictEvaluationStrategy(tripleSource, dataset,
                    Algebra.FEDERATED_SERVICE_RESOLVER);
        } else if (dataset != null) {
            return new StrictEvaluationStrategy(Algebra.EMPTY_TRIPLE_SOURCE, dataset,
                    Algebra.FEDERATED_SERVICE_RESOLVER);
        } else {
            return Algebra.EMPTY_EVALUATION_STRATEGY;
        }
    }

    public static EvaluationStatistics getEvaluationStatistics(
            @Nullable final ToDoubleFunction<StatementPattern> estimator) {

        return estimator == null ? Algebra.DEFAULT_EVALUATION_STATISTICS
                : new EvaluationStatistics() {

                    @Override
                    protected CardinalityCalculator createCardinalityCalculator() {
                        return new CardinalityCalculator() {

                            @Override
                            public final double getCardinality(final StatementPattern pattern) {
                                final double estimate = estimator.applyAsDouble(pattern);
                                return estimate >= 0.0 ? estimate : super.getCardinality(pattern);
                            }

                        };
                    }

                };
    }

    public static TupleExpr parseTupleExpr(final String string, @Nullable final String baseIRI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException {

        Objects.requireNonNull(string);
        final TupleExpr expr = ((Projection) Algebra
                .parseQuery("SELECT *\nWHERE {\n" + string + "\n}", baseIRI, namespaces)
                .getTupleExpr()).getArg();
        expr.setParentNode(null);
        return expr;
    }

    public static ValueExpr parseValueExpr(final String string, @Nullable final String baseIRI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException {

        Objects.requireNonNull(string);
        final TupleExpr expr = Algebra
                .parseQuery("SELECT ((" + string + ") AS ?dummy) WHERE {}", baseIRI, namespaces)
                .getTupleExpr();
        return ((Extension) ((Projection) expr).getArg()).getElements().get(0).getExpr();
    }

    public static ParsedQuery parseQuery(final String string, @Nullable final String baseIRI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException {
        try {
            final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(string);
            StringEscapesProcessor.process(qc);
            BaseDeclProcessor.process(qc, baseIRI);

            // was: final Map<String, String> prefixes = parseHelper(qc, namespaces);
            final List<ASTPrefixDecl> prefixDeclList = qc.getPrefixDeclList();
            final Map<String, String> prefixes = new LinkedHashMap<String, String>();
            for (final ASTPrefixDecl prefixDecl : prefixDeclList) {
                final String prefix = prefixDecl.getPrefix();
                final String iri = prefixDecl.getIRI().getValue();
                if (prefixes.containsKey(prefix)) {
                    throw new MalformedQueryException(
                            "Multiple prefix declarations for prefix '" + prefix + "'");
                }
                prefixes.put(prefix, iri);
            }
            if (namespaces != null) {
                final QNameProcessor visitor = new QNameProcessor(namespaces, prefixes);
                try {
                    qc.jjtAccept(visitor, null);
                } catch (final VisitorException e) {
                    throw new MalformedQueryException(e);
                }
            }

            WildcardProjectionProcessor.process(qc);
            BlankNodeVarProcessor.process(qc);
            if (qc.containsQuery()) {
                TupleExpr tupleExpr;
                final TupleExprBuilder tupleExprBuilder = new TupleExprBuilder(
                        Statements.VALUE_FACTORY); // [FC] changed
                try {
                    tupleExpr = (TupleExpr) qc.jjtAccept(tupleExprBuilder, null);
                } catch (final VisitorException e) {
                    throw new MalformedQueryException(e.getMessage(), e);
                }
                ParsedQuery query;
                final ASTQuery queryNode = qc.getQuery();
                if (queryNode instanceof ASTSelectQuery) {
                    query = new ParsedTupleQuery(string, tupleExpr);
                } else if (queryNode instanceof ASTConstructQuery) {
                    query = new ParsedGraphQuery(string, tupleExpr, prefixes);
                } else if (queryNode instanceof ASTAskQuery) {
                    query = new ParsedBooleanQuery(string, tupleExpr);
                } else if (queryNode instanceof ASTDescribeQuery) {
                    query = new ParsedGraphQuery(string, tupleExpr, prefixes);
                } else {
                    throw new RuntimeException("Unexpected query type: " + queryNode.getClass());
                }
                final Dataset dataset = DatasetDeclProcessor.process(qc);
                if (dataset != null) {
                    query.setDataset(dataset);
                }
                return query;
            } else {
                throw new IncompatibleOperationException(
                        "supplied string is not a query operation");
            }
        } catch (final ParseException e) {
            throw new MalformedQueryException(e.getMessage(), e);
        } catch (final TokenMgrError e) {
            throw new MalformedQueryException(e.getMessage(), e);
        }
    }

    public static Value evaluateValueExpr(final ValueExpr expr, final BindingSet bindings) {
        try {
            return Algebra.EMPTY_EVALUATION_STRATEGY.evaluate(expr, bindings);
        } catch (final QueryEvaluationException ex) {
            throw new IllegalArgumentException(
                    "Error evaluating value expr:\n" + expr + "\nbindings: " + bindings, ex);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T extends QueryModelNode> T normalize(@Nullable T expr,
            final Function<Value, Value> normalizer) {
        if (expr != null) {
            expr = (T) expr.clone();
            expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

                @Override
                public void meet(final Var node) throws RuntimeException {
                    if (node.hasValue()) {
                        node.setValue(normalizer.apply(node.getValue()));
                    }
                }

                @Override
                public void meet(final ValueConstant node) throws RuntimeException {
                    node.setValue(normalizer.apply(node.getValue()));
                }

            });
        }
        return expr;
    }

    public static Iterator<BindingSet> evaluateTupleExpr(TupleExpr expr,
            @Nullable final Dataset dataset, @Nullable BindingSet bindings,
            @Nullable EvaluationStrategy evaluationStrategy,
            @Nullable EvaluationStatistics evaluationStatistics,
            @Nullable final Function<Value, Value> valueNormalizer) {

        // Apply defaults where necessary
        bindings = bindings != null ? bindings : EmptyBindingSet.getInstance();
        evaluationStrategy = evaluationStrategy != null ? evaluationStrategy //
                : Algebra.getEvaluationStrategy(null, dataset);
        evaluationStatistics = evaluationStatistics != null ? evaluationStatistics //
                : Algebra.getEvaluationStatistics(null);

        // Add a dummy query root node to help with the optimization
        expr = expr.clone();
        if (!(expr instanceof QueryRoot)) {
            expr = new QueryRoot(expr);
        }

        // Replace constant values in the query with corresponding values in the model
        if (valueNormalizer != null) {
            expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

                @Override
                public void meet(final Var node) throws RuntimeException {
                    if (node.hasValue()) {
                        node.setValue(valueNormalizer.apply(node.getValue()));
                    }
                }

                @Override
                public void meet(final ValueConstant node) throws RuntimeException {
                    node.setValue(valueNormalizer.apply(node.getValue()));
                }

            });
        }

        // Optimize the query
        Algebra.LOGGER.trace("Query before optimization:\n{}", expr);
        new BindingAssigner().optimize(expr, dataset, bindings);
        new ConstantOptimizer(evaluationStrategy).optimize(expr, dataset, bindings);
        new CompareOptimizer().optimize(expr, dataset, bindings);
        new ConjunctiveConstraintSplitter().optimize(expr, dataset, bindings);
        new DisjunctiveConstraintOptimizer().optimize(expr, dataset, bindings);
        new SameTermFilterOptimizer().optimize(expr, dataset, bindings);
        new QueryModelNormalizer().optimize(expr, dataset, bindings);
        new QueryJoinOptimizer(evaluationStatistics).optimize(expr, dataset, bindings);
        new IterativeEvaluationOptimizer().optimize(expr, dataset, bindings);
        new FilterOptimizer().optimize(expr, dataset, bindings);
        new OrderLimitOptimizer().optimize(expr, dataset, bindings);
        Algebra.LOGGER.trace("Query after optimization:\n{}", expr);

        // Start the query, returning a CloseableIteration over its results
        try {
            return eu.fbk.rdfpro.util.Iterators.forIteration(
                    evaluationStrategy.evaluate(expr, EmptyBindingSet.getInstance()));
        } catch (final QueryEvaluationException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static boolean isBGP(final TupleExpr expr) {
        final AtomicBoolean bgp = new AtomicBoolean(true);
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            protected void meetNode(final QueryModelNode node) throws RuntimeException {
                if (!bgp.get()) {
                    return;
                } else if (node instanceof StatementPattern || node instanceof Join
                        || node instanceof Var) {
                    super.meetNode(node);
                } else {
                    bgp.set(false);
                    return;
                }
            }

        });
        return bgp.get();
    }

    public static <T> List<T> extractNodes(@Nullable final QueryModelNode expr,
            final Class<T> clazz, @Nullable final Predicate<? super T> matchPredicate,
            @Nullable final Predicate<QueryModelNode> recursePredicate) {
        final List<T> result = new ArrayList<>();
        if (expr != null) {
            expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

                @SuppressWarnings("unchecked")
                @Override
                protected void meetNode(final QueryModelNode node) throws RuntimeException {
                    if (clazz.isInstance(node)
                            && (matchPredicate == null || matchPredicate.test((T) node))) {
                        result.add((T) node);
                    }
                    if (recursePredicate == null || recursePredicate.test(node)) {
                        super.meetNode(node);
                    }
                }

            });
        }
        return result;
    }

    public static Set<String> extractVariables(@Nullable final QueryModelNode expr,
            final boolean onlyOutputVars) {
        final Set<String> set = new HashSet<>();
        if (expr != null) {
            expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

                @Override
                public void meet(final Var var) throws RuntimeException {
                    if (!var.hasValue()) {
                        final String name = var.getName();
                        final int index = name.indexOf('-');
                        set.add(index < 0 ? name : name.substring(0, index));
                    }
                }

            });
            if (onlyOutputVars && expr instanceof TupleExpr) {
                set.retainAll(((TupleExpr) expr).getBindingNames());
            }
        }
        return set;
    }

    public static void internStrings(@Nullable final QueryModelNode expr) {
        if (expr != null) {
            expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

                @Override
                public void meet(final Var var) throws RuntimeException {
                    var.setName(var.getName().intern());
                }

            });
        }
    }

    @Nullable
    public static <T extends QueryModelNode> T rewrite(@Nullable final T node,
            @Nullable final Map<String, Var> substitutions) {
        if (node == null || substitutions == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final T result = (T) node.clone();
        result.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final Var var) throws RuntimeException {
                if (!var.hasValue()) {
                    final Var replacement = substitutions.get(var.getName());
                    if (replacement != null) {
                        var.setName(replacement.getName());
                        var.setValue(replacement.getValue());
                        var.setAnonymous(replacement.isAnonymous());
                    }
                }
            }

        });
        return result;
    }

    @Nullable
    public static <T extends QueryModelNode> T rewrite(@Nullable final T node,
            @Nullable final BindingSet bindings) {

        if (node == null || bindings == null) {
            return node;
        }

        @SuppressWarnings("unchecked")
        final T result = (T) node.clone();
        result.setParentNode(null);

        result.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final Var var) {
                final Binding binding = bindings.getBinding(var.getName());
                if (binding != null) {
                    if (var.getParentNode() instanceof StatementPattern) {
                        var.setValue(binding.getValue());
                        var.setName("_const-" + var.getName());
                    } else {
                        Algebra.replaceNode(result, var, new ValueConstant(binding.getValue()));
                    }
                }
            }

        });
        return result;
    }

    @Nullable
    public static TupleExpr normalizeVars(@Nullable TupleExpr expr) {

        if (expr == null) {
            return null;
        }

        expr = expr.clone();
        expr.setParentNode(null);

        final Map<String, String> replacements = new HashMap<>();
        final List<Filter> filtersToDrop = new ArrayList<>();
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final SameTerm same) throws RuntimeException {
                if (same.getParentNode() instanceof Filter && same.getLeftArg() instanceof Var
                        && same.getRightArg() instanceof Var) {
                    final Var leftVar = (Var) same.getLeftArg();
                    final Var rightVar = (Var) same.getRightArg();
                    if (leftVar.isAnonymous() || rightVar.isAnonymous()) {
                        if (!rightVar.isAnonymous()) {
                            replacements.put(leftVar.getName(), rightVar.getName());
                        } else {
                            replacements.put(rightVar.getName(), leftVar.getName());
                        }
                        filtersToDrop.add((Filter) same.getParentNode());
                    }
                }
            }

        });
        expr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final Var var) throws RuntimeException {
                if (!var.hasValue()) {
                    final String newName = replacements.get(var.getName());
                    if (newName != null) {
                        var.setName(newName);
                    } else if (var.getName().startsWith("_const-")) {
                        if (var.getParentNode() instanceof StatementPattern) {
                            for (final Var var2 : ((StatementPattern) var.getParentNode())
                                    .getVarList()) {
                                if (var2.hasValue() && var.getName().startsWith(var2.getName())) {
                                    var.setValue(var2.getValue());
                                }
                            }
                        }
                    } else if (var.getName().startsWith("-anon-")) {
                        var.setName(var.getName().replace('-', '_'));
                    } else {
                        final int index = var.getName().indexOf('-');
                        if (index >= 0) {
                            var.setName(var.getName().substring(0, index));
                        }
                    }
                }
            }

        });
        for (final Filter filter : filtersToDrop) {
            expr = (TupleExpr) Algebra.replaceNode(expr, filter, filter.getArg());
        }
        return expr;
    }

    public static TupleExpr rewriteGraph(final TupleExpr expr, final Var graphVar) {

        if (expr == null) {
            return null;
        }

        final TupleExpr result = expr.clone();
        result.setParentNode(null);

        result.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final StatementPattern pattern) throws RuntimeException {
                pattern.setContextVar(graphVar);
            }

        });
        return result;
    }

    public static QueryModelNode replaceNode(final QueryModelNode root,
            final QueryModelNode current, final QueryModelNode replacement) {
        final QueryModelNode parent = current.getParentNode();
        if (parent == null) {
            replacement.setParentNode(null);
            return replacement;
        } else {
            parent.replaceChildNode(current, replacement);
            current.setParentNode(null);
            return root;
        }
    }

    @Nullable
    public static TupleExpr explodeFilters(@Nullable TupleExpr expr) {

        if (expr == null) {
            return null;
        }

        expr = expr.clone();
        expr.setParentNode(null);

        final List<Filter> filters = Algebra.extractNodes(expr, Filter.class,
                (final Filter filter) -> {
                    return filter.getCondition() instanceof And;
                }, null);

        if (filters.isEmpty()) {
            return expr;
        }

        for (final Filter filter : filters) {
            TupleExpr arg = filter.getArg();
            for (final ValueExpr condition : Algebra.extractNodes(filter.getCondition(),
                    ValueExpr.class, e -> !(e instanceof And), e -> e instanceof And)) {
                arg = new Filter(arg, condition);
            }
            expr = (TupleExpr) Algebra.replaceNode(expr, filter, arg);
        }

        return expr;
    }

    @Nullable
    public static TupleExpr pushFilters(@Nullable TupleExpr expr) {

        if (expr == null) {
            return null;
        }

        expr = expr.clone();
        expr.setParentNode(null);

        boolean dirty = true;
        while (dirty) {
            dirty = false;
            for (final Filter filter : Algebra.extractNodes(expr, Filter.class, null, null)) {

                TupleExpr arg = filter.getArg();
                while (arg instanceof Filter) {
                    arg = ((Filter) arg).getArg();
                }

                if (arg instanceof Join) {
                    final ValueExpr condition = filter.getCondition();
                    final Set<String> filterVars = Algebra.extractVariables(condition, false);
                    final Join join = (Join) arg;
                    boolean rewritten = false;
                    if (join.getLeftArg().getAssuredBindingNames().containsAll(filterVars)) {
                        join.setLeftArg(new Filter(join.getLeftArg(), condition.clone()));
                        rewritten = true;
                    }
                    if (join.getRightArg().getAssuredBindingNames().containsAll(filterVars)) {
                        join.setRightArg(new Filter(join.getRightArg(), condition.clone()));
                        rewritten = true;
                    }
                    if (rewritten) {
                        expr = (TupleExpr) Algebra.replaceNode(expr, filter, filter.getArg());
                        dirty = true;
                    }
                } else if (arg instanceof Extension) {
                    final Extension ext = (Extension) arg;
                    final TupleExpr extArg = ext.getArg();
                    final Set<String> vars = extArg.getBindingNames();
                    boolean canMove = true;
                    for (TupleExpr e = filter; e instanceof Filter; e = ((Filter) e).getArg()) {
                        canMove = canMove && vars.containsAll(
                                Algebra.extractVariables(((Filter) e).getCondition(), false));
                    }
                    if (canMove) {
                        final Filter lastFilter = (Filter) ext.getParentNode();
                        expr = (TupleExpr) Algebra.replaceNode(expr, filter, ext);
                        lastFilter.setArg(extArg);
                        ext.setArg(filter);
                    }
                }
            }
        }

        return expr;
    }

    @Nullable
    public static TupleExpr pushExtensions(@Nullable TupleExpr expr) {

        if (expr == null) {
            return null;
        }

        expr = expr.clone();
        expr.setParentNode(null);

        boolean dirty = true;
        while (dirty) {
            dirty = false;
            for (final Extension extension : Algebra.extractNodes(expr, Extension.class, null,
                    null)) {

                TupleExpr arg = extension.getArg();
                while (arg instanceof Extension) {
                    arg = ((Filter) arg).getArg();
                }

                if (arg instanceof Join) {
                    final Join join = (Join) arg;
                    for (final ExtensionElem elem : new ArrayList<>(extension.getElements())) {
                        final Set<String> elemVars = Algebra.extractVariables(elem.getExpr(),
                                false);
                        Extension newArg = null;
                        if (join.getLeftArg().getAssuredBindingNames().containsAll(elemVars)) {
                            newArg = join.getLeftArg() instanceof Extension
                                    ? (Extension) join.getLeftArg()
                                    : new Extension(join.getLeftArg());
                            join.setLeftArg(newArg);
                        } else if (join.getRightArg().getAssuredBindingNames()
                                .contains(elemVars)) {
                            newArg = join.getRightArg() instanceof Extension
                                    ? (Extension) join.getRightArg()
                                    : new Extension(join.getRightArg());
                            join.setRightArg(newArg);
                        }
                        if (newArg != null) {
                            newArg.addElement(elem.clone());
                            extension.getElements().remove(elem);
                            dirty = true;
                        }
                    }
                    if (extension.getElements().isEmpty()) {
                        expr = (TupleExpr) Algebra.replaceNode(expr, extension,
                                extension.getArg());
                    }
                }
            }
        }

        return expr;
    }

    public static TupleExpr[] splitTupleExpr(final TupleExpr expr, final Set<IRI> vocabulary,
            final int partition) {

        return Algebra.splitTupleExpr(expr, new Predicate<StatementPattern>() {

            @Override
            public boolean test(final StatementPattern pattern) {
                for (final Var var : pattern.getVarList()) {
                    if (vocabulary.contains(var.getValue())) {
                        return true;
                    }
                }
                return false;
            }

        }, partition);
    }

    public static TupleExpr[] splitTupleExpr(@Nullable final TupleExpr expr,
            final Predicate<StatementPattern> predicate, final int partition) {

        if (expr == null) {
            return new TupleExpr[] { null, null };
        }

        final TupleExpr clonedExpr = expr.clone();
        clonedExpr.setParentNode(null);

        final AtomicReference<TupleExpr> unsplittable = new AtomicReference<>(null);
        try {
            final List<TupleExpr> matching = new ArrayList<>();
            final List<TupleExpr> nonMatching = new ArrayList<>();
            clonedExpr.visit(new AbstractQueryModelVisitor<RuntimeException>() {

                private int flag = 0;

                private boolean top = true;

                @Override
                protected void meetNode(final QueryModelNode node) throws RuntimeException {
                    if (node instanceof TupleExpr) {

                        int flag;
                        if (node instanceof StatementPattern) {
                            flag = predicate.test((StatementPattern) node) ? 1 : 2;
                            this.flag = this.flag | flag;

                        } else {
                            final int oldFlag = this.flag;
                            final boolean oldTop = this.top;
                            this.flag = 0;
                            this.top &= node instanceof Join;
                            node.visitChildren(this);
                            flag = this.flag;
                            this.flag = oldFlag | this.flag;
                            this.top = oldTop;
                        }

                        if (this.top && !(node instanceof Join)) {
                            if (flag == 1) {
                                matching.add((TupleExpr) node);
                            } else if (flag == 2) {
                                nonMatching.add((TupleExpr) node);
                            } else {
                                unsplittable.set((TupleExpr) node);
                                throw new IllegalArgumentException("Cannot split:\n" + expr);
                            }
                        }
                    }
                }

            });

            final TupleExpr[] result = new TupleExpr[2];
            final List<List<TupleExpr>> exprs = Arrays.asList(matching, nonMatching);
            for (int i = 0; i < 2; ++i) {
                for (final TupleExpr e : exprs.get(i)) {
                    final TupleExpr clone = e.clone();
                    result[i] = result[i] == null ? clone : new Join(result[i], clone);
                }
            }
            return result;

        } catch (final RuntimeException ex) {
            final TupleExpr e = unsplittable.get();
            if ((e instanceof Filter || e instanceof Extension)
                    && (partition == 0 || partition == 1)) {
                final TupleExpr expr2 = (TupleExpr) Algebra.replaceNode(clonedExpr, e,
                        ((UnaryTupleOperator) e).getArg());
                final TupleExpr[] result = Algebra.splitTupleExpr(expr2, predicate, partition);
                boolean fixed = false;
                if (e instanceof Filter) {
                    final ValueExpr condition = ((Filter) e).getCondition();
                    final Set<String> vars = Algebra.extractVariables(condition, false);
                    for (int i = 0; i < 2; ++i) {
                        if (result[i].getAssuredBindingNames().containsAll(vars)) {
                            result[i] = new Filter(result[i], condition);
                            fixed = true;
                        }
                    }
                    if (!fixed && (partition == 1 || partition == 2)) {
                        result[partition] = new Filter(result[partition], condition);
                        fixed = true;
                    }
                } else {
                    final List<ExtensionElem> elems = ((Extension) e).getElements();
                    final Set<String> vars = new HashSet<>();
                    for (final ExtensionElem elem : elems) {
                        vars.addAll(Algebra.extractVariables(elem, false));
                    }
                    for (int i = 0; i < 2; ++i) {
                        if (i == partition
                                || result[i].getAssuredBindingNames().containsAll(vars)) {
                            result[i] = new Extension(result[i], elems);
                            fixed = true;
                            break;
                        }
                    }
                    if (!fixed && (partition == 1 || partition == 2)) {
                        result[partition] = new Extension(result[partition], elems);
                        fixed = true;
                    }
                }
                if (fixed) {
                    return result;
                }
            }
            throw ex;
        }
    }

    public static String renderQuery(final TupleExpr expr, @Nullable final Dataset dataset,
            @Nullable final Map<String, String> prefixes, final boolean forceSelect) {
        return new SPARQLRenderer(prefixes, forceSelect).render(expr, dataset);
    }

    public static String renderExpr(final TupleExpr expr,
            @Nullable final Map<String, String> prefixes) {
        return new SPARQLRenderer(prefixes, false).renderTupleExpr(expr);
    }

    public static String format(final TupleExpr expr) {
        return expr == null ? "null"
                : Algebra.renderExpr(expr, Namespaces.DEFAULT.prefixMap()).replaceAll("[\n\r\t ]+",
                        " ");
    }

    private static class QNameProcessor extends AbstractASTVisitor {

        private final Map<String, String> namespacesOut;

        private final Map<String, String> namespacesIn;

        public QNameProcessor(final Map<String, String> namespacesIn,
                final Map<String, String> namespacesOut) {
            this.namespacesOut = namespacesOut;
            this.namespacesIn = namespacesIn;
        }

        @Override
        public Object visit(final ASTServiceGraphPattern node, final Object data)
                throws VisitorException {
            node.setPrefixDeclarations(this.namespacesOut);
            return super.visit(node, data);
        }

        @Override
        public Object visit(final ASTQName qnameNode, final Object data) throws VisitorException {
            final String qname = qnameNode.getValue();
            final int colonIdx = qname.indexOf(':');
            assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;
            final String prefix = qname.substring(0, colonIdx);
            String localName = qname.substring(colonIdx + 1);
            String namespace = this.namespacesOut.get(prefix);
            if (namespace == null) { // [FC] added lookup of default namespace
                namespace = this.namespacesIn.get(prefix);
            }
            if (namespace == null) {
                throw new VisitorException("QName '" + qname + "' uses an undefined prefix");
            }
            localName = this.processEscapesAndHex(localName);
            final ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
            iriNode.setValue(namespace + localName);
            qnameNode.jjtReplaceWith(iriNode);
            return null;
        }

        private String processEscapesAndHex(final String localName) {
            final StringBuffer unencoded = new StringBuffer();
            final Pattern hexPattern = Pattern.compile("([^\\\\]|^)(%[A-F\\d][A-F\\d])",
                    Pattern.CASE_INSENSITIVE);
            Matcher m = hexPattern.matcher(localName);
            boolean result = m.find();
            while (result) {
                final String previousChar = m.group(1);
                final String encoded = m.group(2);

                final int codePoint = Integer.parseInt(encoded.substring(1), 16);
                final String decoded = String.valueOf(Character.toChars(codePoint));

                m.appendReplacement(unencoded, previousChar + decoded);
                result = m.find();
            }
            m.appendTail(unencoded);
            final StringBuffer unescaped = new StringBuffer();
            final Pattern escapedCharPattern = Pattern
                    .compile("\\\\[_~\\.\\-!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:\\/\\?#\\@\\%]");
            m = escapedCharPattern.matcher(unencoded.toString());
            result = m.find();
            while (result) {
                final String escaped = m.group();
                m.appendReplacement(unescaped, escaped.substring(1));
                result = m.find();
            }
            m.appendTail(unescaped);
            return unescaped.toString();
        }

    }

}
