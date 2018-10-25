package eu.fbk.rdfpro.util;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.ToDoubleFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.common.iteration.EmptyIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.IncompatibleOperationException;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UpdateExpr;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategy;
import org.eclipse.rdf4j.query.algebra.evaluation.EvaluationStrategyFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryPreparer;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.AbstractFederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedService;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.FederatedServiceResolverImpl;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ExtendedEvaluationStrategyFactory;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedDescribeQuery;
import org.eclipse.rdf4j.query.parser.ParsedGraphQuery;
import org.eclipse.rdf4j.query.parser.ParsedOperation;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.ParsedUpdate;
import org.eclipse.rdf4j.query.parser.QueryParser;
import org.eclipse.rdf4j.query.parser.sparql.AbstractASTVisitor;
import org.eclipse.rdf4j.query.parser.sparql.BaseDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.BlankNodeVarProcessor;
import org.eclipse.rdf4j.query.parser.sparql.DatasetDeclProcessor;
import org.eclipse.rdf4j.query.parser.sparql.StringEscapesProcessor;
import org.eclipse.rdf4j.query.parser.sparql.TupleExprBuilder;
import org.eclipse.rdf4j.query.parser.sparql.UpdateExprBuilder;
import org.eclipse.rdf4j.query.parser.sparql.WildcardProjectionProcessor;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTAskQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTConstructQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTDeleteData;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTDescribeQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTIRI;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTInsertData;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTOperationContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTPrefixDecl;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQName;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTQueryContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTSelectQuery;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTServiceGraphPattern;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUnparsedQuadDataBlock;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUpdate;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUpdateContainer;
import org.eclipse.rdf4j.query.parser.sparql.ast.ASTUpdateSequence;
import org.eclipse.rdf4j.query.parser.sparql.ast.Node;
import org.eclipse.rdf4j.query.parser.sparql.ast.ParseException;
import org.eclipse.rdf4j.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.eclipse.rdf4j.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.eclipse.rdf4j.query.parser.sparql.ast.TokenMgrError;
import org.eclipse.rdf4j.query.parser.sparql.ast.VisitorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Sparql {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sparql.class);

    private static final SparqlParserEx DEFAULT_PARSER = new SparqlParserEx(
            Namespaces.DEFAULT.uriMap());

    private static final EvaluationStatistics DEFAULT_EVALUATION_STATISTICS = new EvaluationStatistics();

    private static final EvaluationStatistics EMPTY_EVALUATION_STATISTICS = getEvaluationStatistics(
            pattern -> 0.0);

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

    private static final FederatedServiceResolver FEDERATED_SERVICE_RESOLVER;

    private static final EvaluationStrategyFactory EVALUATION_STRATEGY_FACTORY;

    private static final Field[] LINKED_HASH_MODEL_NODE_FIELDS;

    static {
        // Initialize FEDERATED_SERVICE_RESOLVER
        FederatedServiceResolver resolver = null;
        try {
            Class.forName("org.apache.http.client.HttpClient");
            resolver = new FederatedServiceResolverImpl();
            LOGGER.trace("Using FederatedServiceResolverImpl");
        } catch (final Throwable ex) {
            resolver = new AbstractFederatedServiceResolver() {

                @Override
                protected FederatedService createService(final String serviceUrl)
                        throws QueryEvaluationException {
                    throw new QueryEvaluationException(
                            "Apache HttpClient not in classpath: SERVICE invocation unsupported");
                }

            };
            LOGGER.trace("Using stub FederatedServiceResolver");
        }
        FEDERATED_SERVICE_RESOLVER = resolver;

        // Initialize EVALUATION_STRATEGY_FACTORY
        EVALUATION_STRATEGY_FACTORY = new ExtendedEvaluationStrategyFactory(
                FEDERATED_SERVICE_RESOLVER);

        // Initialize LINKED_HASH_MODEL_NODE_FIELDS
        try {
            Field[] fields = null;
            for (final Class<?> clazz : LinkedHashModel.class.getDeclaredClasses()) {
                if (clazz.getName().equals("ModelNode")) {
                    fields = new Field[] { //
                            clazz.getDeclaredField("subjects"), //
                            clazz.getDeclaredField("predicates"), //
                            clazz.getDeclaredField("objects"), //
                            clazz.getDeclaredField("contexts") };
                }
            }
            LINKED_HASH_MODEL_NODE_FIELDS = Objects.requireNonNull(fields);
        } catch (final Throwable ex) {
            throw new Error("Could not access LinkedHashModel node fields", ex);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends ParsedOperation> T parse(final String sparql) {
        return (T) parse(sparql, ParsedOperation.class, null, null);
    }

    public static <T> T parse(final String sparql, final Class<T> clazz) {
        return parse(sparql, clazz, null, null);
    }

    public static <T> T parse(final String sparql, final Class<T> clazz,
            @Nullable final String baseURI, @Nullable final Map<String, String> namespaces) {

        Objects.requireNonNull(sparql);
        Objects.requireNonNull(clazz);

        final QueryParser parser = getQueryParser(namespaces);

        if (ParsedQuery.class.isAssignableFrom(clazz)) {
            return clazz.cast(parser.parseQuery(sparql, baseURI));

        } else if (ParsedUpdate.class.isAssignableFrom(clazz)) {
            return clazz.cast(parser.parseUpdate(sparql, baseURI));

        } else if (ParsedOperation.class.isAssignableFrom(clazz)) {
            try {
                return clazz.cast(parser.parseQuery(sparql, baseURI));
            } catch (final Throwable ex) {
                try {
                    return clazz.cast(parser.parseUpdate(sparql, baseURI));
                } catch (final Throwable ex2) {
                    ex.addSuppressed(ex2);
                }
                Throwables.throwIfUnchecked(ex);
                throw new RuntimeException(ex);
            }

        } else if (TupleExpr.class.isAssignableFrom(clazz)) {
            final TupleExpr expr = ((Projection) parser
                    .parseQuery("SELECT *\nWHERE {\n" + sparql + "\n}", baseURI).getTupleExpr())
                            .getArg();
            expr.setParentNode(null);
            return clazz.cast(expr);

        } else if (ValueExpr.class.isAssignableFrom(clazz)) {
            final TupleExpr expr = parser
                    .parseQuery("SELECT ((" + sparql + ") AS ?dummy) WHERE {}", baseURI)
                    .getTupleExpr();
            return clazz.cast(
                    ((Extension) ((Projection) expr).getArg()).getElements().get(0).getExpr());

        } else if (UpdateExpr.class.isAssignableFrom(clazz)) {
            final UpdateExpr expr = parser.parseUpdate(sparql, baseURI).getUpdateExprs().get(0);
            return clazz.cast(expr);

        } else {
            throw new UnsupportedOperationException("Unsupported parse target " + clazz);
        }
    }

    public static QueryParser getQueryParser() {
        return getQueryParser(null);
    }

    public static QueryParser getQueryParser(@Nullable final Map<String, String> namespaces) {
        return namespaces == null ? DEFAULT_PARSER : new SparqlParserEx(namespaces);
    }

    public static QueryOptimizer getQueryOptimizer() {
        return getQueryOptimizer(null);
    }

    public static QueryOptimizer getQueryOptimizer(
            @Nullable final EvaluationStatistics statistics) {
        // TODO use OptimizedQueryRoot to mark base optimization
        return null;
    }

    public static final TripleSource getTripleSource(
            @Nullable final Iterable<? extends Statement> statements) {

        if (statements == null
                || statements instanceof Collection<?> && ((Collection<?>) statements).isEmpty()) {
            return EMPTY_TRIPLE_SOURCE;

        } else {
            return new TripleSource() {

                @Override
                public ValueFactory getValueFactory() {
                    return Statements.VALUE_FACTORY;
                }

                @Override
                public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                        final Resource subj, final IRI pred, final Value obj,
                        final Resource... contexts) throws QueryEvaluationException {
                    if (statements instanceof Model) {
                        return Iterators.toIteration(
                                ((Model) statements).filter(subj, pred, obj, contexts).iterator());
                    } else {
                        final Set<Resource> ctxs = contexts == null || contexts.length == 0 ? null
                                : ImmutableSet.copyOf(contexts);
                        return Iterators
                                .toIteration(Iterators.filter(statements.iterator(), stmt -> {
                                    return (subj == null || subj.equals(stmt.getSubject()))
                                            && (pred == null || pred.equals(stmt.getPredicate()))
                                            && (obj == null || obj.equals(stmt.getObject()))
                                            && (ctxs == null || ctxs.contains(stmt.getContext()));
                                }));
                    }
                }

            };
        }
    }

    public static EvaluationStatistics getEvaluationStatistics() {
        return DEFAULT_EVALUATION_STATISTICS;
    }

    public static EvaluationStatistics getEvaluationStatistics(
            final ToDoubleFunction<StatementPattern> estimator) {

        Objects.requireNonNull(estimator);
        return new EvaluationStatistics() {

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

    public static EvaluationStatistics getEvaluationStatistics(
            @Nullable final TripleSource source) {

        if (source == null) {
            return EMPTY_EVALUATION_STATISTICS;

        } else {
            return getEvaluationStatistics(pattern -> {
                final Resource s = (Resource) pattern.getSubjectVar().getValue();
                final IRI p = (IRI) pattern.getPredicateVar().getValue();
                final Value o = pattern.getObjectVar().getValue();
                final Resource c = (Resource) pattern.getContextVar().getValue();
                try (CloseableIteration<?, QueryEvaluationException> i = c == null
                        ? source.getStatements(s, p, o) : source.getStatements(s, p, o, c)) {
                    return !i.hasNext() ? 0.0 : -1; // either set to 0 or default cardinality
                } catch (final Throwable ex) {
                    Throwables.throwIfUnchecked(ex);
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    public static EvaluationStatistics getEvaluationStatistics(
            @Nullable final Iterable<Statement> statements) {

        if (statements == null
                || statements instanceof Collection<?> && ((Collection<?>) statements).isEmpty()) {
            return EMPTY_EVALUATION_STATISTICS;

        } else if (statements instanceof LinkedHashModel) {
            final LinkedHashModel model = (LinkedHashModel) statements;
            return getEvaluationStatistics(pattern -> {
                final Resource s = (Resource) pattern.getSubjectVar().getValue();
                final IRI p = (IRI) pattern.getPredicateVar().getValue();
                final Value o = pattern.getObjectVar().getValue();
                final Resource c = (Resource) pattern.getContextVar().getValue();
                final Model m = c == null ? model.filter(s, p, o) : model.filter(s, p, o, c);
                if (m.isEmpty()) {
                    return 0.0;
                }
                final Statement stmt = m.iterator().next();
                int size = model.size();
                final Field[] fields = LINKED_HASH_MODEL_NODE_FIELDS;
                try {
                    size = Math.min(size, ((Set<?>) fields[0].get(stmt.getSubject())).size());
                    size = Math.min(size, ((Set<?>) fields[1].get(stmt.getPredicate())).size());
                    size = Math.min(size, ((Set<?>) fields[2].get(stmt.getObject())).size());
                    if (stmt.getContext() != null) {
                        size = Math.min(size, ((Set<?>) fields[3].get(stmt.getContext())).size());
                    }
                    return size;
                } catch (final Throwable ex) {
                    LOGGER.warn("Could not estimate cardinality for pattern " + pattern
                            + " on model " + model, ex);
                    return -1; // fallback to default cardinality
                }
            });

        } else if (statements instanceof Model) {
            final Model model = (Model) statements;
            return getEvaluationStatistics(pattern -> {
                final Resource s = (Resource) pattern.getSubjectVar().getValue();
                final IRI p = (IRI) pattern.getPredicateVar().getValue();
                final Value o = pattern.getObjectVar().getValue();
                final Resource c = (Resource) pattern.getContextVar().getValue();
                final boolean empty = c == null ? model.contains(s, p, o)
                        : model.contains(s, p, o, c);
                return empty ? 0.0 : -1; // either set to 0 or use default pattern cardinality
            });

        } else {
            return DEFAULT_EVALUATION_STATISTICS;
        }
    }

    public static FederatedServiceResolver getFederatedServiceResolver() {
        return FEDERATED_SERVICE_RESOLVER;
    }

    public static EvaluationStrategy getEvaluationStrategy(final TripleSource source) {
        return getEvaluationStrategyFactory().createEvaluationStrategy(null, source);
    }

    public static EvaluationStrategy getEvaluationStrategy(final TripleSource source,
            @Nullable final Dataset dataset) {
        return getEvaluationStrategyFactory().createEvaluationStrategy(dataset, source);
    }

    public static EvaluationStrategyFactory getEvaluationStrategyFactory() {
        return EVALUATION_STRATEGY_FACTORY;
    }

    public static QueryPreparer getQueryPreparer(final Iterable<Statement> statements,
            final QueryOptimizer... optimizers) {
        // TODO support for update operations
        return null;
    }

    public static QueryPreparer getQueryPreparer(final EvaluationStrategy strategy,
            final QueryOptimizer... optimizers) {
        // TODO no support for update operations
        return null;
    }

    private static final class SparqlParserEx implements QueryParser {

        private final Map<String, String> namespaces;

        public SparqlParserEx(final Map<String, String> namespaces) {
            this.namespaces = namespaces;
        }

        @Override
        public ParsedQuery parseQuery(final String sparql, final String baseURI)
                throws MalformedQueryException {
            try {
                final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(sparql);
                StringEscapesProcessor.process(qc);
                BaseDeclProcessor.process(qc, baseURI);
                final Map<String, String> prefixes = process(qc); // [FC]
                WildcardProjectionProcessor.process(qc);
                BlankNodeVarProcessor.process(qc);
                if (qc.containsQuery()) {
                    final TupleExpr tupleExpr = buildQueryModel(qc);
                    ParsedQuery query;
                    final ASTQuery queryNode = qc.getQuery();
                    if (queryNode instanceof ASTSelectQuery) {
                        query = new ParsedTupleQuery(sparql, tupleExpr);
                    } else if (queryNode instanceof ASTConstructQuery) {
                        query = new ParsedGraphQuery(sparql, tupleExpr, prefixes);
                    } else if (queryNode instanceof ASTAskQuery) {
                        query = new ParsedBooleanQuery(sparql, tupleExpr);
                    } else if (queryNode instanceof ASTDescribeQuery) {
                        query = new ParsedDescribeQuery(sparql, tupleExpr, prefixes);
                    } else {
                        throw new RuntimeException(
                                "Unexpected query type: " + queryNode.getClass());
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

        @Override
        public ParsedUpdate parseUpdate(final String updateStr, final String baseURI)
                throws MalformedQueryException {
            try {
                final ParsedUpdate update = new ParsedUpdate(updateStr);
                final ASTUpdateSequence updateSequence = SyntaxTreeBuilder
                        .parseUpdateSequence(updateStr);
                final List<ASTUpdateContainer> updateOperations = updateSequence
                        .getUpdateContainers();
                List<ASTPrefixDecl> sharedPrefixDeclarations = null;
                final Set<String> globalUsedBNodeIds = new HashSet<String>();
                for (int i = 0; i < updateOperations.size(); i++) {
                    final ASTUpdateContainer uc = updateOperations.get(i);
                    if (uc.jjtGetNumChildren() == 0 && i > 0 && i < updateOperations.size() - 1) {
                        throw new MalformedQueryException("empty update in sequence not allowed");
                    }
                    StringEscapesProcessor.process(uc);
                    BaseDeclProcessor.process(uc, baseURI);
                    WildcardProjectionProcessor.process(uc);
                    final List<ASTPrefixDecl> prefixDeclList = uc.getPrefixDeclList();
                    if (prefixDeclList == null || prefixDeclList.size() == 0) {
                        if (sharedPrefixDeclarations != null) {
                            for (final ASTPrefixDecl prefixDecl : sharedPrefixDeclarations) {
                                uc.jjtAppendChild(prefixDecl);
                            }
                        }
                    } else {
                        sharedPrefixDeclarations = prefixDeclList;
                    }
                    process(uc); // [FC]
                    final Set<String> usedBNodeIds = BlankNodeVarProcessor.process(uc);
                    if (uc.getUpdate() instanceof ASTInsertData
                            || uc.getUpdate() instanceof ASTInsertData) {
                        if (Collections.disjoint(usedBNodeIds, globalUsedBNodeIds)) {
                            globalUsedBNodeIds.addAll(usedBNodeIds);
                        } else {
                            throw new MalformedQueryException(
                                    "blank node identifier may not be shared across INSERT/DELETE DATA operations");
                        }
                    }
                    final UpdateExprBuilder updateExprBuilder = new UpdateExprBuilder(
                            SimpleValueFactory.getInstance());
                    final ASTUpdate updateNode = uc.getUpdate();
                    if (updateNode != null) {
                        final UpdateExpr updateExpr = (UpdateExpr) updateNode
                                .jjtAccept(updateExprBuilder, null);
                        update.addUpdateExpr(updateExpr);
                        final Dataset dataset = DatasetDeclProcessor.process(uc);
                        update.map(updateExpr, dataset);
                    }
                }
                return update;
            } catch (final ParseException e) {
                throw new MalformedQueryException(e.getMessage(), e);
            } catch (final TokenMgrError e) {
                throw new MalformedQueryException(e.getMessage(), e);
            } catch (final VisitorException e) {
                throw new MalformedQueryException(e.getMessage(), e);
            }
        }

        private TupleExpr buildQueryModel(final Node qc) throws MalformedQueryException {
            final TupleExprBuilder tupleExprBuilder = new TupleExprBuilder(
                    Statements.VALUE_FACTORY); // [FC]
            try {
                return (TupleExpr) qc.jjtAccept(tupleExprBuilder, null);
            } catch (final VisitorException e) {
                throw new MalformedQueryException(e.getMessage(), e);
            }
        }

        private Map<String, String> process(final ASTOperationContainer qc)
                throws MalformedQueryException {

            final List<ASTPrefixDecl> prefixDeclList = qc.getPrefixDeclList();
            final Map<String, String> prefixMap = new LinkedHashMap<String, String>();
            for (final ASTPrefixDecl prefixDecl : prefixDeclList) {
                final String prefix = prefixDecl.getPrefix();
                final String iri = prefixDecl.getIRI().getValue();
                if (prefixMap.containsKey(prefix)) {
                    throw new MalformedQueryException(
                            "Multiple prefix declarations for prefix '" + prefix + "'");
                }
                prefixMap.put(prefix, iri);
            }

            final int defaultPrefixesAdded = insertDefaultPrefix(prefixMap, "rdf", RDF.NAMESPACE)
                    + insertDefaultPrefix(prefixMap, "rdfs", RDFS.NAMESPACE)
                    + insertDefaultPrefix(prefixMap, "sesame", SESAME.NAMESPACE)
                    + insertDefaultPrefix(prefixMap, "owl", OWL.NAMESPACE)
                    + insertDefaultPrefix(prefixMap, "xsd", XMLSchema.NAMESPACE)
                    + insertDefaultPrefix(prefixMap, "fn", FN.NAMESPACE);

            ASTUnparsedQuadDataBlock dataBlock = null;
            if (qc.getOperation() instanceof ASTInsertData) {
                final ASTInsertData insertData = (ASTInsertData) qc.getOperation();
                dataBlock = insertData.jjtGetChild(ASTUnparsedQuadDataBlock.class);
            } else if (qc.getOperation() instanceof ASTDeleteData) {
                final ASTDeleteData deleteData = (ASTDeleteData) qc.getOperation();
                dataBlock = deleteData.jjtGetChild(ASTUnparsedQuadDataBlock.class);
            }

            if (dataBlock != null) {
                final String prefixes = createPrefixesInSPARQLFormat(prefixMap);
                dataBlock.setAddedDefaultPrefixes(defaultPrefixesAdded);
                dataBlock.setDataBlock(prefixes + dataBlock.getDataBlock());
                // TODO: add all namespaces here?
            } else {
                final QNameProcessor visitor = new QNameProcessor(prefixMap);
                try {
                    qc.jjtAccept(visitor, null);
                } catch (final VisitorException e) {
                    throw new MalformedQueryException(e);
                }
            }
            return prefixMap;
        }

        private static int insertDefaultPrefix(final Map<String, String> prefixMap,
                final String prefix, final String namespace) {
            if (!prefixMap.containsKey(prefix) && !prefixMap.containsValue(namespace)) {
                prefixMap.put(prefix, namespace);
                return 1;
            }
            return 0;
        }

        private static String createPrefixesInSPARQLFormat(final Map<String, String> prefixMap) {
            final StringBuilder sb = new StringBuilder();
            for (final Entry<String, String> entry : prefixMap.entrySet()) {
                sb.append("PREFIX");
                final String prefix = entry.getKey();
                if (prefix != null) {
                    sb.append(" " + prefix);
                }
                sb.append(":");
                sb.append(" <" + entry.getValue() + "> \n");
            }
            return sb.toString();
        }

        private class QNameProcessor extends AbstractASTVisitor {

            private final Map<String, String> prefixMap;

            public QNameProcessor(final Map<String, String> prefixMap) {
                this.prefixMap = prefixMap;
            }

            @Override
            public Object visit(final ASTServiceGraphPattern node, final Object data)
                    throws VisitorException {
                node.setPrefixDeclarations(this.prefixMap);
                return super.visit(node, data);
            }

            @Override
            public Object visit(final ASTQName qnameNode, final Object data)
                    throws VisitorException {
                final String qname = qnameNode.getValue();
                final int colonIdx = qname.indexOf(':');
                assert colonIdx >= 0 : "colonIdx should be >= 0: " + colonIdx;
                final String prefix = qname.substring(0, colonIdx);
                String localName = qname.substring(colonIdx + 1);
                String namespace = this.prefixMap.get(prefix);
                if (namespace == null) { // [FC] added lookup of default namespace
                    namespace = SparqlParserEx.this.namespaces.get(prefix);
                }
                if (namespace == null) {
                    throw new VisitorException("QName '" + qname + "' uses an undefined prefix");
                }
                localName = processEscapes(localName);
                final ASTIRI iriNode = new ASTIRI(SyntaxTreeBuilderTreeConstants.JJTIRI);
                iriNode.setValue(namespace + localName);
                qnameNode.jjtReplaceWith(iriNode);
                return null;
            }

            private String processEscapes(final String localName) {

                final StringBuffer unescaped = new StringBuffer();
                final Pattern escapedCharPattern = Pattern
                        .compile("\\\\[_~\\.\\-!\\$\\&\\'\\(\\)\\*\\+\\,\\;\\=\\:\\/\\?#\\@\\%]");
                final Matcher m = escapedCharPattern.matcher(localName);
                boolean result = m.find();
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

}
