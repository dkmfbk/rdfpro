package eu.fbk.rdfpro.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

public final class Ruleset {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ruleset.class);

    private final Set<Rule> rules;

    private final Set<URI> staticTerms;

    @Nullable
    private transient Map<URI, Rule> ruleIndex;

    private transient int hash;

    @Nullable
    private transient Map<URI, TupleExpr> staticHeads;

    @Nullable
    private transient Map<URI, TupleExpr> dynamicHeads;

    @Nullable
    private transient Map<URI, TupleExpr> staticBodies;

    @Nullable
    private transient Map<URI, TupleExpr> dynamicBodies;

    @Nullable
    private transient Ruleset staticRuleset;

    @Nullable
    private transient Ruleset preprocessingRuleset;

    public Ruleset(final Iterable<Rule> rules, @Nullable final Iterable<URI> staticTerms) {
        this.rules = newUnmodifiableSet(rules, false);
        this.staticTerms = newUnmodifiableSet(staticTerms, true);
        this.ruleIndex = null;
        this.hash = 0;
        this.staticBodies = null;
        this.dynamicBodies = null;
        this.staticRuleset = null;
        this.preprocessingRuleset = null;
    }

    private void split() {
        final Map<URI, TupleExpr> staticHeads = new HashMap<>();
        final Map<URI, TupleExpr> dynamicHeads = new HashMap<>();
        final Map<URI, TupleExpr> staticBodies = new HashMap<>();
        final Map<URI, TupleExpr> dynamicBodies = new HashMap<>();
        for (final Rule rule : this.rules) {
            try {
                final TupleExpr head = Algebra.explodeFilters(rule.getHead());
                final TupleExpr body = Algebra.explodeFilters(rule.getBody());
                final TupleExpr[] headExprs = Algebra.splitTupleExpr(head, this.staticTerms, -1);
                final TupleExpr[] bodyExprs = Algebra.splitTupleExpr(body, this.staticTerms, 1);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Splitting of rule {}:"
                            + "\n  head original: {}\n  head static:   {}\n  head dynamic:  {}"
                            + "\n  body original: {}\n  body static:   {}\n  body dynamic:  {}",
                            rule.getID(), Algebra.format(rule.getHead()),
                            Algebra.format(headExprs[0]), Algebra.format(headExprs[1]),
                            Algebra.format(rule.getBody()), Algebra.format(bodyExprs[0]),
                            Algebra.format(bodyExprs[1]));
                }
                staticHeads.put(rule.getID(), headExprs[0]);
                dynamicHeads.put(rule.getID(), headExprs[1]);
                staticBodies.put(rule.getID(), bodyExprs[0]);
                dynamicBodies.put(rule.getID(), bodyExprs[1]);
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Cannot split rule " + rule.getID(), ex);
            }
        }
        this.staticHeads = staticHeads;
        this.dynamicHeads = dynamicHeads;
        this.staticBodies = staticBodies;
        this.dynamicBodies = dynamicBodies;
    }

    public Set<Rule> getRules() {
        return this.rules;
    }

    @Nullable
    public Rule getRule(final URI ruleID) {
        if (this.ruleIndex == null) {
            final Map<URI, Rule> index = new HashMap<>();
            for (final Rule rule : this.rules) {
                index.put(rule.getID(), rule);
            }
            this.ruleIndex = index;
        }
        return this.ruleIndex.get(ruleID);
    }

    @Nullable
    public TupleExpr getStaticHead(final Resource ruleID) {
        if (this.staticHeads == null) {
            split();
        }
        return this.staticHeads.get(ruleID);
    }

    @Nullable
    public TupleExpr getStaticBody(final Resource ruleID) {
        if (this.staticBodies == null) {
            split();
        }
        return this.staticBodies.get(ruleID);
    }

    public Set<URI> getStaticTerms() {
        return this.staticTerms;
    }

    @Nullable
    public TupleExpr getDynamicHead(final Resource ruleID) {
        if (this.dynamicHeads == null) {
            split();
        }
        return this.dynamicHeads.get(ruleID);
    }

    @Nullable
    public TupleExpr getDynamicBody(final Resource ruleID) {
        if (this.dynamicBodies == null) {
            split();
        }
        return this.dynamicBodies.get(ruleID);
    }

    public Ruleset getDynamicRuleset(
            final Map<URI, ? extends Iterable<? extends BindingSet>> staticBindings) {

        final ValueFactory vf = Statements.VALUE_FACTORY;
        final List<Rule> rules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            final TupleExpr dynamicHead = getDynamicHead(rule.getID());
            final TupleExpr staticBody = getStaticBody(rule.getID());
            final TupleExpr dynamicBody = getDynamicBody(rule.getID());
            if (dynamicHead != null) {
                if (staticBody == null) {
                    final URI id = vf.createURI(rule.getID() + "_" + rules.size());
                    rules.add(new Rule(id, dynamicHead, dynamicBody));
                } else {
                    final Iterable<? extends BindingSet> list = staticBindings.get(rule.getID());
                    if (list != null) {
                        for (final BindingSet bindings : list) {
                            final TupleExpr rewrittenHead = Algebra.rewrite(dynamicHead, bindings);
                            final TupleExpr rewrittenBody = Algebra.rewrite(dynamicBody, bindings);
                            if (!Objects.equals(rewrittenHead, rewrittenBody)) {
                                final URI id = vf.createURI(rule.getID() + "_" + rules.size());
                                rules.add(new Rule(id, rewrittenHead, rewrittenBody));
                            }
                        }
                    }
                }
            }
        }
        return new Ruleset(rules, this.staticTerms);
    }

    public Ruleset getPreprocessingRuleset() {
        if (this.preprocessingRuleset == null) {
            final List<Rule> preprocessingRules = new ArrayList<>();
            for (final Rule rule : this.rules) {
                final TupleExpr dynamicHead = getDynamicHead(rule.getID());
                final TupleExpr staticBody = getStaticBody(rule.getID());
                if (dynamicHead != null && staticBody != null) {
                    preprocessingRules.add(new Rule(rule.getID(), null, staticBody));
                }
            }
            this.preprocessingRuleset = new Ruleset(preprocessingRules, this.staticTerms);
        }
        return this.preprocessingRuleset;
    }

    public Ruleset transform(@Nullable final BindingSet bindings) {
        if (bindings == null || bindings.size() == 0) {
            return this;
        }
        final List<Rule> transformedRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            final TupleExpr head = Algebra.rewrite(rule.getHead(), bindings);
            final TupleExpr body = Algebra.rewrite(rule.getBody(), bindings);
            transformedRules.add(new Rule(rule.getID(), head, body));
        }
        return new Ruleset(transformedRules, this.staticTerms);
    }

    public Ruleset transformMergeHeads() {

        final Map<TupleExpr, List<Rule>> clusters = new HashMap<>();
        for (final Rule rule : this.rules) {
            List<Rule> cluster = clusters.get(rule.getBody());
            if (cluster == null) {
                cluster = new ArrayList<>();
                clusters.put(rule.getBody(), cluster);
            }
            cluster.add(rule);
        }

        final ValueFactory vf = Statements.VALUE_FACTORY;
        final List<Rule> mergedRules = new ArrayList<>();
        for (final List<Rule> cluster : clusters.values()) {
            final Rule first = cluster.get(0);
            final String namespace = first.getID().getNamespace();
            final Set<String> names = new TreeSet<>();
            final TupleExpr body = first.getBody();
            TupleExpr head = null;
            for (int i = 0; i < cluster.size(); ++i) {
                final Rule rule = cluster.get(i);
                final String s = rule.getID().getLocalName();
                final int index = s.indexOf("__");
                names.add(index < 0 ? s : s.substring(0, index));
                head = head == null ? rule.getHead() : new Join(head, rule.getHead());
            }
            final URI id = vf.createURI(namespace,
                    String.join("_", names) + "__" + mergedRules.size());
            mergedRules.add(new Rule(id, head, body));
        }

        return new Ruleset(mergedRules, this.staticTerms);
    }

    public Ruleset transformGlobalGM(@Nullable final Resource globalGraph) {
        final Var graphVar = globalGraph == null ? null : new Var("_const-" + UUID.randomUUID(),
                globalGraph);
        final List<Rule> transformedRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            final TupleExpr head = Algebra.rewriteGraph(rule.getHead(), graphVar);
            final TupleExpr body = Algebra.rewriteGraph(rule.getBody(), null);
            transformedRules.add(new Rule(rule.getID(), head, body));
        }
        return new Ruleset(transformedRules, this.staticTerms);
    }

    public Ruleset transformSeparateGM() {

        // Extract all the vars used in the rules
        final Set<String> vars = new HashSet<String>();
        for (final Rule rule : this.rules) {
            vars.addAll(Algebra.extractVariables(rule.getHead()));
            vars.addAll(Algebra.extractVariables(rule.getBody()));
        }

        // Select a fresh graph var that does not appear in the rules
        String graphVarName = "g";
        int index = 0;
        while (vars.contains(graphVarName)) {
            graphVarName = "g" + index++;
        }
        final Var graphVar = new Var(graphVarName);

        // Rewrite rules
        final List<Rule> transformedRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            final TupleExpr head = Algebra.rewriteGraph(rule.getHead(), graphVar);
            final TupleExpr body = Algebra.rewriteGraph(rule.getBody(), graphVar);
            transformedRules.add(new Rule(rule.getID(), head, body));
        }
        return new Ruleset(transformedRules, this.staticTerms);
    }

    public Ruleset transformStarGM(final Resource globalGraph) {

        // Extract all the vars used in the rules
        final Set<String> vars = new HashSet<String>();
        for (final Rule rule : this.rules) {
            vars.addAll(Algebra.extractVariables(rule.getHead()));
            vars.addAll(Algebra.extractVariables(rule.getBody()));
        }

        // Select a variable prefix never used in the rules
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

        // Rewrite rules
        final List<Rule> transformedRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            TupleExpr head = rule.getHead();
            TupleExpr body = rule.getBody();
            if (body == null) {
                head = Algebra.rewriteGraph(head, new Var("_const-" + UUID.randomUUID(),
                        globalGraph));
            } else {
                final AtomicInteger counter = new AtomicInteger(0);
                final List<ValueExpr> filterGraphVars = new ArrayList<>();
                final List<ValueExpr> bindGraphVars = new ArrayList<>();
                filterGraphVars.add(new Var("_const-" + UUID.randomUUID(), globalGraph));
                bindGraphVars.add(new Var("_const-" + UUID.randomUUID(), globalGraph));
                head = Algebra.rewriteGraph(head, new Var(prefix));
                body = body.clone();
                body.visit(new QueryModelVisitorBase<RuntimeException>() {

                    @Override
                    public void meet(final StatementPattern pattern) throws RuntimeException {
                        final Var graphVar = new Var(prefix + counter.getAndIncrement());
                        pattern.setContextVar(graphVar);
                        filterGraphVars.add(graphVar.clone());
                        bindGraphVars.add(graphVar.clone());
                    }

                });
                body = new Filter(body, new Compare(new FunctionCall(
                        RR.STAR_SELECT_GRAPH.stringValue(), filterGraphVars), new Var("_const-"
                        + UUID.randomUUID(), RDF.NIL), CompareOp.NE));
                body = new Extension(body, new ExtensionElem(new FunctionCall(
                        RR.STAR_SELECT_GRAPH.stringValue(), bindGraphVars), prefix));
            }
            transformedRules.add(new Rule(rule.getID(), head, body));
        }
        return new Ruleset(transformedRules, this.staticTerms);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Ruleset)) {
            return false;
        }
        final Ruleset other = (Ruleset) object;
        return this.rules.equals(other.rules) && this.staticTerms.equals(other.staticTerms);
    }

    @Override
    public int hashCode() {
        if (this.hash == 0) {
            this.hash = Objects.hash(this.rules, this.staticTerms);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("STATIC TERMS (").append(this.staticTerms.size()).append("):");
        for (final URI staticTerm : this.staticTerms) {
            builder.append("\n").append(Statements.formatValue(staticTerm, Namespaces.DEFAULT));
        }
        builder.append("\n\nRULES (").append(this.rules.size()).append("):");
        for (final Rule rule : this.rules) {
            builder.append("\n").append(rule);
        }
        return builder.toString();
    }

    public <T extends Collection<? super Statement>> T toRDF(final T output) {

        // Emit static terms
        final ValueFactory vf = Statements.VALUE_FACTORY;
        for (final URI staticTerm : this.staticTerms) {
            vf.createStatement(staticTerm, RDF.TYPE, RR.STATIC_TERM);
        }

        // Emit rules
        for (final Rule rule : this.rules) {
            rule.toRDF(output);
        }
        return output;
    }

    public static Ruleset fromRDF(final Iterable<Statement> model) {

        // Parse static terms
        final List<URI> staticTerms = new ArrayList<>();
        for (final Statement stmt : model) {
            if (stmt.getSubject() instanceof URI && RDF.TYPE.equals(stmt.getPredicate())
                    && RR.STATIC_TERM.equals(stmt.getObject())) {
                staticTerms.add((URI) stmt.getSubject());
            }
        }

        // Parse rules
        final List<Rule> rules = Rule.fromRDF(model);

        // Build resulting ruleset
        return new Ruleset(rules, staticTerms);
    }

    public static Ruleset merge(final Ruleset... rulesets) {
        if (rulesets.length == 0) {
            return new Ruleset(Collections.emptyList(), Collections.emptyList());
        } else if (rulesets.length == 1) {
            return rulesets[0];
        } else {
            final List<URI> staticTerms = new ArrayList<>();
            final List<Rule> rules = new ArrayList<>();
            for (final Ruleset ruleset : rulesets) {
                staticTerms.addAll(ruleset.getStaticTerms());
                rules.addAll(ruleset.getRules());
            }
            return new Ruleset(rules, staticTerms);
        }
    }

    private static <T> Set<T> newUnmodifiableSet(final Iterable<? extends T> elements,
            final boolean deduplicate) {
        final Set<T> set = new LinkedHashSet<T>();
        for (final T element : elements) {
            if (!deduplicate && set.contains(element)) {
                throw new IllegalArgumentException("Duplicate element: " + element);
            }
            set.add(element);
        }
        return Collections.unmodifiableSet(set);
    }

}
