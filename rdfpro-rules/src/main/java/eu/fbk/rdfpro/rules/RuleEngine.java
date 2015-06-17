package eu.fbk.rdfpro.rules;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import org.openrdf.model.Model;
import org.openrdf.model.Statement;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.SameTerm;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.rio.RDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.Namespaces;

public abstract class RuleEngine {

    protected static final Logger LOGGER = LoggerFactory.getLogger(RuleEngine.class);

    private static final String BUILDER_CLASS = Environment.getProperty("rdfpro.rules.builder",
            "eu.fbk.rdfpro.rules.drools.Engine$Builder");

    public RDFHandler newSession(final RDFHandler handler) {
        return newSession(handler, null);
    }

    public abstract RDFHandler newSession(final RDFHandler handler,
            @Nullable final Callback callback);

    public static Builder builder() {
        return builder(new EmptyBindingSet());
    }

    public static Builder builder(@Nullable final BindingSet bindings) {
        try {
            final BindingSet actualBindings = bindings != null ? bindings : new EmptyBindingSet();
            final Class<?> clazz = Class.forName(BUILDER_CLASS);
            final Constructor<?> constructor = clazz.getConstructor(BindingSet.class);
            return (Builder) constructor.newInstance(actualBindings);
        } catch (final Throwable ex) {
            throw new Error("Illegal rule engine implementation: " + BUILDER_CLASS);
        }
    }

    public static abstract class Builder {

        private final BindingSet bindings;

        private final boolean normalizeBodyVars;

        protected Builder(final BindingSet bindings, final boolean normalizeBodyVars) {
            this.bindings = bindings;
            this.normalizeBodyVars = normalizeBodyVars;
        }

        protected abstract void doAddRule(final String ruleID, final List<StatementPattern> head,
                @Nullable final TupleExpr body);

        protected abstract RuleEngine doBuild();

        public final Builder addRules(final Model model) {
            final Set<String> ids = new HashSet<>();
            final Map<String, String> heads = new HashMap<>();
            final Map<String, String> bodies = new HashMap<>();
            for (final Statement stmt : model.filter(null, RR.HEAD, null)) {
                ids.add(stmt.getSubject().stringValue());
                heads.put(stmt.getSubject().stringValue(), stmt.getObject().stringValue());
            }
            for (final Statement stmt : model.filter(null, RR.BODY, null)) {
                ids.add(stmt.getSubject().stringValue());
                bodies.put(stmt.getSubject().stringValue(), stmt.getObject().stringValue());
            }
            final List<String> sortedIDs = new ArrayList<>(ids);
            Collections.sort(sortedIDs);
            for (final String id : sortedIDs) {
                addRule(id, heads.get(id), bodies.get(id),
                        Namespaces.forIterable(model.getNamespaces(), false), null);
            }
            return this;
        }

        public final Builder addRule(final String id, @Nullable final String head,
                @Nullable final String body, @Nullable final Namespaces namespaces,
                @Nullable final Map<BindingSet, Iterable<BindingSet>> mappings) {
            final TupleExpr headExpr = head == null ? null : Algebra.parseTupleExpr(head, null,
                    namespaces.uriMap());
            final TupleExpr bodyExpr = body == null ? null : Algebra.parseTupleExpr(body, null,
                    namespaces.uriMap());
            addRule(id, headExpr, bodyExpr);
            return this;
        }

        public final Builder addRule(final String ruleID, @Nullable final TupleExpr head,
                @Nullable final TupleExpr body) {

            final TupleExpr rewrittenHead = rewrite(head, true);
            final TupleExpr rewrittenBody = rewrite(body, this.normalizeBodyVars);

            final List<StatementPattern> headAtoms = new ArrayList<>();
            if (rewrittenHead != null) {
                rewrittenHead.visit(new QueryModelVisitorBase<RuntimeException>() {

                    @Override
                    protected void meetNode(final QueryModelNode node) throws RuntimeException {
                        if (node instanceof StatementPattern) {
                            headAtoms.add((StatementPattern) node);
                        } else if (node instanceof Join) {
                            node.visitChildren(this);
                        } else {
                            throw new IllegalArgumentException("Unsupported head expression: "
                                    + node);
                        }
                    }

                });
            }

            doAddRule(ruleID, headAtoms, rewrittenBody);

            return this;
        }

        public final RuleEngine build() {
            return doBuild();
        }

        @Nullable
        private TupleExpr rewrite(@Nullable TupleExpr expr, final boolean normalizeVars) {
            if (expr != null) {
                final List<Filter> filtersToDrop = new ArrayList<>();
                expr = expr.clone();
                expr.visit(new QueryModelVisitorBase<RuntimeException>() {

                    @Override
                    public void meet(final SameTerm same) throws RuntimeException {
                        if (normalizeVars && same.getParentNode() instanceof Filter
                                && same.getLeftArg() instanceof Var
                                && same.getRightArg() instanceof Var) {
                            final String leftName = ((Var) same.getLeftArg()).getName();
                            final String rightName = ((Var) same.getRightArg()).getName();
                            if (leftName.startsWith(rightName + "-")
                                    || rightName.startsWith(leftName + "-")) {
                                filtersToDrop.add((Filter) same.getParentNode());
                            }
                        }
                    }

                    @Override
                    public void meet(final Var var) throws RuntimeException {
                        if (!var.hasValue()) {
                            if (normalizeVars) {
                                final int index = var.getName().indexOf('-');
                                if (index >= 0) {
                                    var.setName(var.getName().substring(0, index));
                                }
                            }
                            final Binding binding = Builder.this.bindings.getBinding(var.getName());
                            if (binding != null) {
                                var.setName("_const-" + UUID.randomUUID().toString());
                                var.setValue(binding.getValue());
                            }
                        }
                    }

                });
                for (final Filter filter : filtersToDrop) {
                    final QueryModelNode parent = filter.getParentNode();
                    filter.getArg().setParentNode(parent);
                    if (parent == null) {
                        expr = filter.getArg();
                    } else if (parent instanceof UnaryTupleOperator) {
                        ((UnaryTupleOperator) parent).setArg(filter.getArg());
                    } else {
                        parent.replaceChildNode(filter, filter.getArg());
                    }
                }
            }
            return expr;
        }

    }

    public interface Callback {

        boolean ruleTriggered(final RDFHandler handler, final String ruleID,
                final BindingSet bindings);

    }

}
