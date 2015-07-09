package eu.fbk.rdfpro.rules.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.IncompatibleOperationException;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.federation.FederatedService;
import org.openrdf.query.algebra.evaluation.federation.FederatedServiceResolver;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.ASTVisitorBase;
import org.openrdf.query.parser.sparql.BaseDeclProcessor;
import org.openrdf.query.parser.sparql.BlankNodeVarProcessor;
import org.openrdf.query.parser.sparql.DatasetDeclProcessor;
import org.openrdf.query.parser.sparql.StringEscapesProcessor;
import org.openrdf.query.parser.sparql.TupleExprBuilder;
import org.openrdf.query.parser.sparql.WildcardProjectionProcessor;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTQuery;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTServiceGraphPattern;
import org.openrdf.query.parser.sparql.ast.ParseException;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilder;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderTreeConstants;
import org.openrdf.query.parser.sparql.ast.TokenMgrError;
import org.openrdf.query.parser.sparql.ast.VisitorException;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

public final class Algebra {

    private static final EvaluationStrategy STRATEGY = new EvaluationStrategyImpl(
            new TripleSource() {

                @Override
                public ValueFactory getValueFactory() {
                    return Statements.VALUE_FACTORY;
                }

                @Override
                public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                        final Resource subj, final URI pred, final Value obj,
                        final Resource... contexts) throws QueryEvaluationException {
                    return new EmptyIteration<Statement, QueryEvaluationException>();
                }

            }, new FederatedServiceResolver() {

                @Override
                public FederatedService getService(final String serviceURL) {
                    throw new Error();
                }

            });

    public static TupleExpr parseTupleExpr(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException {

        Objects.requireNonNull(string);
        final TupleExpr expr = ((Projection) parseQuery("SELECT *\nWHERE {\n" + string + "\n}",
                baseURI, namespaces).getTupleExpr()).getArg();
        expr.setParentNode(null);
        return expr;
    }

    public static ValueExpr parseValueExpr(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException {

        Objects.requireNonNull(string);
        final TupleExpr expr = parseQuery("SELECT ((" + string + ") AS ?dummy) WHERE {}", baseURI,
                namespaces).getTupleExpr();
        return ((Extension) ((Projection) expr).getArg()).getElements().get(0).getExpr();
    }

    public static ParsedQuery parseQuery(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) throws MalformedQueryException {
        try {
            final ASTQueryContainer qc = SyntaxTreeBuilder.parseQuery(string);
            StringEscapesProcessor.process(qc);
            BaseDeclProcessor.process(qc, baseURI);

            // was: final Map<String, String> prefixes = parseHelper(qc, namespaces);
            final List<ASTPrefixDecl> prefixDeclList = qc.getPrefixDeclList();
            final Map<String, String> prefixes = new LinkedHashMap<String, String>();
            for (final ASTPrefixDecl prefixDecl : prefixDeclList) {
                final String prefix = prefixDecl.getPrefix();
                final String iri = prefixDecl.getIRI().getValue();
                if (prefixes.containsKey(prefix)) {
                    throw new MalformedQueryException("Multiple prefix declarations for prefix '"
                            + prefix + "'");
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
            return STRATEGY.evaluate(expr, bindings);
        } catch (final QueryEvaluationException ex) {
            throw new IllegalArgumentException("Error evaluating value expr:\n" + expr
                    + "\nbindings: " + bindings, ex);
        }
    }

    public static <T> List<T> extractNodes(@Nullable final QueryModelNode expr,
            final Class<T> clazz, @Nullable final Predicate<? super T> matchPredicate,
            @Nullable final Predicate<QueryModelNode> recursePredicate) {
        final List<T> result = new ArrayList<>();
        if (expr != null) {
            expr.visit(new QueryModelVisitorBase<RuntimeException>() {

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

    public static Set<String> extractVariables(@Nullable final QueryModelNode expr) {
        final Set<String> set = new HashSet<>();
        if (expr != null) {
            expr.visit(new QueryModelVisitorBase<RuntimeException>() {

                @Override
                public void meet(final Var var) throws RuntimeException {
                    if (!var.hasValue()) {
                        final String name = var.getName();
                        final int index = name.indexOf('-');
                        set.add(index < 0 ? name : name.substring(0, index));
                    }
                }

            });
        }
        return set;
    }

    @Nullable
    public static <T extends QueryModelNode> T rewrite(@Nullable final T node,
            @Nullable final Map<String, Var> substitutions) {
        if (node == null || substitutions == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final T result = (T) node.clone();
        result.visit(new QueryModelVisitorBase<RuntimeException>() {

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

        result.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Var var) {
                final Binding binding = bindings.getBinding(var.getName());
                if (binding != null) {
                    if (var.getParentNode() instanceof StatementPattern) {
                        var.setValue(binding.getValue());
                        var.setName("_const-" + var.getName());
                    } else {
                        replaceNode(result, var, new ValueConstant(binding.getValue()));
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
        expr.visit(new QueryModelVisitorBase<RuntimeException>() {

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
        expr.visit(new QueryModelVisitorBase<RuntimeException>() {

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
            expr = (TupleExpr) replaceNode(expr, filter, filter.getArg());
        }
        return expr;
    }

    public static TupleExpr rewriteGraph(final TupleExpr expr, final Var graphVar) {

        if (expr == null) {
            return null;
        }

        final TupleExpr result = expr.clone();
        result.setParentNode(null);

        result.visit(new QueryModelVisitorBase<RuntimeException>() {

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

        final List<Filter> filters = extractNodes(expr, Filter.class, (final Filter filter) -> {
            return filter.getCondition() instanceof And;
        }, null);

        if (filters.isEmpty()) {
            return expr;
        }

        for (final Filter filter : filters) {
            TupleExpr arg = filter.getArg();
            for (final ValueExpr condition : extractNodes(filter.getCondition(), ValueExpr.class,
                    e -> !(e instanceof And), e -> e instanceof And)) {
                arg = new Filter(arg, condition);
            }
            expr = (TupleExpr) replaceNode(expr, filter, arg);
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
            for (final Filter filter : extractNodes(expr, Filter.class, null, null)) {

                TupleExpr arg = filter.getArg();
                while (arg instanceof Filter) {
                    arg = ((Filter) arg).getArg();
                }

                if (arg instanceof Join) {
                    final ValueExpr condition = filter.getCondition();
                    final Set<String> filterVars = extractVariables(condition);
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
                        expr = (TupleExpr) replaceNode(expr, filter, filter.getArg());
                        dirty = true;
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
            for (final Extension extension : extractNodes(expr, Extension.class, null, null)) {

                TupleExpr arg = extension.getArg();
                while (arg instanceof Extension) {
                    arg = ((Filter) arg).getArg();
                }

                if (arg instanceof Join) {
                    final Join join = (Join) arg;
                    for (final ExtensionElem elem : new ArrayList<>(extension.getElements())) {
                        final Set<String> elemVars = extractVariables(elem.getExpr());
                        Extension newArg = null;
                        if (join.getLeftArg().getAssuredBindingNames().containsAll(elemVars)) {
                            newArg = join.getLeftArg() instanceof Extension ? (Extension) join
                                    .getLeftArg() : new Extension(join.getLeftArg());
                                    join.setLeftArg(newArg);
                        } else if (join.getRightArg().getAssuredBindingNames().contains(elemVars)) {
                            newArg = join.getRightArg() instanceof Extension ? (Extension) join
                                    .getRightArg() : new Extension(join.getRightArg());
                                    join.setRightArg(newArg);
                        }
                        if (newArg != null) {
                            newArg.addElement(elem.clone());
                            extension.getElements().remove(elem);
                            dirty = true;
                        }
                    }
                    if (extension.getElements().isEmpty()) {
                        expr = (TupleExpr) replaceNode(expr, extension, extension.getArg());
                    }
                }
            }
        }

        return expr;
    }

    public static TupleExpr[] splitTupleExpr(final TupleExpr expr, final Set<URI> vocabulary,
            final int partition) {

        return splitTupleExpr(expr, (final StatementPattern pattern) -> {
            for (final Var var : pattern.getVarList()) {
                if (vocabulary.contains(var.getValue())) {
                    return true;
                }
            }
            return false;
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
            clonedExpr.visit(new QueryModelVisitorBase<RuntimeException>() {

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
                final TupleExpr expr2 = (TupleExpr) replaceNode(clonedExpr, e,
                        ((UnaryTupleOperator) e).getArg());
                final TupleExpr[] result = splitTupleExpr(expr2, predicate, partition);
                boolean fixed = false;
                if (e instanceof Filter) {
                    final ValueExpr condition = ((Filter) e).getCondition();
                    final Set<String> vars = extractVariables(condition);
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
                        vars.addAll(extractVariables(elem));
                    }
                    for (int i = 0; i < 2; ++i) {
                        if (i == partition || result[i].getAssuredBindingNames().containsAll(vars)) {
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

    public static String format(final TupleExpr expr) {
        return expr == null ? "null" : new SPARQLRenderer(Namespaces.DEFAULT.prefixMap(), false)
        .renderTupleExpr(expr).replaceAll("[\n\r\t ]+", " ");
    }

    private static class QNameProcessor extends ASTVisitorBase {

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
            localName = processEscapesAndHex(localName);
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
