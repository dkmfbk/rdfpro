package eu.fbk.rdfpro.rules.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.sparql.SPARQLParser;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;

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

            });

    public static TupleExpr parseTupleExpr(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) {

        Objects.requireNonNull(string);

        final StringBuilder builder = new StringBuilder();

        if (namespaces != null) {
            for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                builder.append("PREFIX ").append(entry.getKey()).append(": ")
                        .append(Statements.formatValue(new URIImpl(entry.getValue())))
                        .append("\n");
            }
        }

        builder.append("SELECT *\nWHERE {\n").append(string).append("\n}");

        try {
            final TupleExpr expr = ((Projection) new SPARQLParser().parseQuery(builder.toString(),
                    baseURI).getTupleExpr()).getArg();
            expr.setParentNode(null);
            return expr;
        } catch (final MalformedQueryException ex) {
            throw new IllegalArgumentException("Invalid tuple expr:\n" + string, ex);
        }
    }

    public static ValueExpr parseValueExpr(final String string, @Nullable final String baseURI,
            @Nullable final Map<String, String> namespaces) {

        Objects.requireNonNull(string);

        final StringBuilder builder = new StringBuilder();

        if (namespaces != null) {
            for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                builder.append("PREFIX ").append(entry.getKey()).append(": ")
                        .append(Statements.formatValue(new URIImpl(entry.getValue())))
                        .append("\n");
            }
        }

        builder.append("SELECT ((").append(string).append(") AS ?dummy) WHERE {}");

        try {
            final TupleExpr expr = new SPARQLParser().parseQuery(builder.toString(), baseURI)
                    .getTupleExpr();
            return ((Extension) ((Projection) expr).getArg()).getElements().get(0).getExpr();
        } catch (final MalformedQueryException ex) {
            throw new IllegalArgumentException("Invalid value expr:\n" + string, ex);
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

    public static Set<String> extractVariables(final QueryModelNode expr) {
        final Set<String> set = new HashSet<>();
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
        result.visit(new QueryModelVisitorBase<RuntimeException>() {

            @Override
            public void meet(final Var var) {
                final Binding binding = bindings.getBinding(var.getName());
                if (binding != null) {
                    var.setValue(binding.getValue());
                    var.setName("_const-" + var.getName());
                }
            }

        });
        return result;
    }

}
