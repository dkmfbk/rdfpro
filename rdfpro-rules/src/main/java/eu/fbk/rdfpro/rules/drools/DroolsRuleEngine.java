package eu.fbk.rdfpro.rules.drools;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.drools.core.spi.KnowledgeHelper;
import org.kie.api.KieServices;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.Message.Level;
import org.kie.api.builder.ReleaseId;
import org.kie.api.builder.Results;
import org.kie.api.builder.model.KieBaseModel;
import org.kie.api.builder.model.KieModuleModel;
import org.kie.api.builder.model.KieSessionModel;
import org.kie.api.conf.EqualityBehaviorOption;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Exists;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.rules.Rule;
import eu.fbk.rdfpro.rules.RuleEngine;
import eu.fbk.rdfpro.rules.Ruleset;
import eu.fbk.rdfpro.rules.util.Algebra;

public class DroolsRuleEngine extends RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DroolsRuleEngine.class);

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final KieContainer container;

    private final Dictionary dictionary;

    private final List<Quad> axioms;

    private final List<Expression> expressions;

    private final List<URI> ruleIDs;

    public DroolsRuleEngine(final Ruleset ruleset) {

        super(ruleset);

        final Translation t = new Translation(ruleset);

        this.container = t.container;
        this.dictionary = t.dictionary;
        this.axioms = new ArrayList<>(t.axioms);
        this.expressions = t.expressions;
        this.ruleIDs = t.ruleIDs;
    }

    @Override
    public String toString() {
        return "DR rule engine";
    }

    @Override
    protected RDFHandler doEval(final RDFHandler handler, final boolean deduplicate) {
        return new Handler(handler); // deduplicate ignored: output always deduplicated
    }

    public final class Handler extends AbstractRDFHandlerWrapper {

        private final Dictionary dictionary;

        private KieSession session;

        private long timestamp;

        private long initialSize;

        private long[] activations;

        Handler(final RDFHandler handler) {
            super(handler);
            this.dictionary = new Dictionary(DroolsRuleEngine.this.dictionary);
            this.session = null;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.timestamp = System.currentTimeMillis();
            this.activations = new long[DroolsRuleEngine.this.ruleIDs.size()];
            this.session = DroolsRuleEngine.this.container.newKieSession();
            this.session.setGlobal("handler", this);
            for (final Quad axiom : DroolsRuleEngine.this.axioms) {
                this.session.insert(axiom);
            }
            this.initialSize = this.session.getFactCount();
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            final long countBefore = this.session.getFactCount();
            this.session.insert(Quad.encode(this.dictionary, statement));
            final long countAfter = this.session.getFactCount();
            if (countAfter > countBefore) {
                this.handler.handleStatement(statement);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.session.fireAllRules();
            if (LOGGER.isDebugEnabled()) {
                final StringBuilder builder = new StringBuilder();
                builder.append("Inference completed in ")
                        .append(System.currentTimeMillis() - this.timestamp).append(" ms, ")
                        .append(this.session.getFactCount() - this.initialSize)
                        .append(" quads total");
                LOGGER.debug(builder.toString());
            }
            super.endRDF();
            this.session.dispose();
            this.session = null;
        }

        @Override
        public void close() {
            try {
                if (this.session != null) {
                    this.session.dispose();
                    this.session = null;
                }
            } finally {
                super.close();
            }
        }

        public void triggered(final int ruleIndex) {
            ++this.activations[ruleIndex];
        }

        public void insert(final KnowledgeHelper drools, final int subjectID,
                final int predicateID, final int objectID, final int contextID)
                throws RDFHandlerException {

            if (!Dictionary.isResource(subjectID) || !Dictionary.isURI(predicateID)
                    || !Dictionary.isResource(contextID)) {
                return;
            }
            final Quad quad = new Quad(subjectID, predicateID, objectID, contextID);
            final long countBefore = this.session.getFactCount();
            drools.insert(quad);
            final long countAfter = this.session.getFactCount();
            if (countAfter > countBefore) {
                this.handler.handleStatement(quad.decode(this.dictionary));
            }
        }

        public int eval(final int expressionIndex, final int... argIDs) {
            return DroolsRuleEngine.this.expressions.get(expressionIndex).evaluate(
                    this.dictionary, argIDs);
        }

        public boolean test(final int expressionIndex, final int... argIDs) {
            try {
                return ((Literal) DroolsRuleEngine.this.expressions.get(expressionIndex).evaluate(
                        this.dictionary.decode(argIDs))).booleanValue();
            } catch (final Throwable ex) {
                return false;
            }
        }

    }

    private static final class Translation {

        private final StringBuilder builder;

        public final Dictionary dictionary;

        public final Set<Quad> axioms;

        public final List<Expression> expressions;

        public final List<URI> ruleIDs;

        public KieContainer container;

        public Translation(final Ruleset ruleset) {
            this.dictionary = new Dictionary();
            this.builder = new StringBuilder();
            this.axioms = new HashSet<>();
            this.expressions = new ArrayList<>();
            this.ruleIDs = new ArrayList<>();
            this.container = translate(ruleset);
        }

        private KieContainer translate(final Ruleset ruleset) {

            this.builder.append("package eu.fbk.rdfpro.rules.drools;\n");
            this.builder.append("import eu.fbk.rdfpro.rules.drools.Quad;\n");
            this.builder.append("import eu.fbk.rdfpro.rules.drools.DroolsRuleEngine.Handler;\n");
            this.builder.append("import eu.fbk.rdfpro.rules.drools.Dictionary;\n");
            this.builder.append("global Handler handler;\n");

            for (final Rule rule : ruleset.getRules()) {

                // Declare rule
                final int ruleIndex = this.ruleIDs.size();
                this.ruleIDs.add(rule.getID());
                this.builder.append("\nrule \"").append(rule.getID().getLocalName())
                        .append("\"\n");
                this.builder.append("when\n");

                // Emit rule body
                final Map<String, Expression> extensionExprs = new HashMap<>();
                final Set<String> matchedVars = new HashSet<>();
                if (rule.getWhereExpr() != null) {
                    translate(Algebra.normalizeVars(rule.getWhereExpr()), Collections.emptySet(),
                            extensionExprs, matchedVars);
                    for (final String extensionVar : extensionExprs.keySet()) {
                        if (matchedVars.contains(extensionVar)) {
                            throw new IllegalArgumentException("Variable " + extensionVar
                                    + " already used in body patterns");
                        }
                    }
                }

                // Emit rule head: handler.trigger(ruleNum, var1, ..., varN);
                this.builder.append("\nthen\n");
                this.builder.append("handler.triggered(").append(ruleIndex).append(");\n");
                if (rule.getInsertExpr() != null) {
                    for (final Map.Entry<String, Expression> entry : extensionExprs.entrySet()) {
                        final String var = entry.getKey();
                        final Expression expr = entry.getValue();
                        final int index = register(expr);
                        this.builder.append("int $").append(var).append(" = ")
                                .append(expr.toString("handler.eval(" + index + ", ", ");\n"));
                    }
                    for (final StatementPattern atom : Algebra.extractNodes(
                            Algebra.normalizeVars(rule.getInsertExpr()), StatementPattern.class,
                            null, null)) {
                        final List<Var> vars = atom.getVarList();
                        this.builder.append("handler.insert(drools, ");
                        for (int j = 0; j < 4; ++j) {
                            this.builder.append(j == 0 ? "" : ", ");
                            String name = null;
                            Value value = null;
                            if (j < vars.size()) {
                                final Var var = vars.get(j);
                                value = var.getValue();
                                name = var.getName();
                            }
                            if (name != null && value == null) {
                                this.builder.append("$").append(name);
                            } else {
                                this.builder.append(this.dictionary.encode(value));
                            }
                        }
                        this.builder.append(");\n");
                    }
                }
                this.builder.append("end\n");
            }

            // Create a new virtual filesystem where to emit drools files
            final KieServices services = KieServices.Factory.get();
            final KieFileSystem kfs = services.newKieFileSystem();

            // Generate the module
            final String rulesetID = "ruleset" + COUNTER.getAndIncrement();
            final KieModuleModel module = services.newKieModuleModel();
            final KieBaseModel base = module.newKieBaseModel("kbase_" + rulesetID)
                    .setDefault(true).setEqualsBehavior(EqualityBehaviorOption.EQUALITY)
                    .addPackage("eu.fbk.rdfpro.rules.drools");
            base.newKieSessionModel("session_" + rulesetID).setDefault(true)
                    .setType(KieSessionModel.KieSessionType.STATEFUL);
            kfs.write("src/main/resources/META-INF/kmodule.xml", module.toXML());

            // Generate the pom.xml
            final ReleaseId releaseId = services.newReleaseId("eu.fbk.rdfpro." + rulesetID,
                    rulesetID, "1.0");
            kfs.writePomXML("<?xml version=\"1.0\"?>\n" //
                    + "<project>\n" //
                    + "<modelVersion>4.0.0</modelVersion>\n" //
                    + "<groupId>" + releaseId.getGroupId() + "</groupId>\n" //
                    + "<artifactId>" + releaseId.getArtifactId() + "</artifactId>\n" //
                    + "<version>" + releaseId.getVersion() + "</version>\n" //
                    + "<packaging>jar</packaging>\n" //
                    + "</project>\n");

            // Generate the rules
            kfs.write("src/main/resources/eu/fbk/rdfpro/rules/drools/" + rulesetID + ".drl",
                    this.builder.toString());
            LOGGER.trace("Generated DROOLS rules:\n" + this.builder);

            // Build and return the container.
            final Results results = services.newKieBuilder(kfs).buildAll().getResults();
            for (final Message message : results.getMessages(Level.INFO)) {
                LOGGER.info("[DROOLS] {}", message);
            }
            for (final Message message : results.getMessages(Level.WARNING)) {
                LOGGER.warn("[DROOLS] {}", message);
            }
            for (final Message message : results.getMessages(Level.ERROR)) {
                LOGGER.error("[DROOLS] {}", message);
            }
            return services.newKieContainer(releaseId);
        }

        private void translate(final TupleExpr expr, final Set<Expression> conditionExprs,
                final Map<String, Expression> extensionExprs, final Set<String> matchedVars) {

            if (expr instanceof StatementPattern) {
                final List<Var> vars = ((StatementPattern) expr).getVarList();
                this.builder.append("Quad(");
                for (int i = 0; i < vars.size(); ++i) {
                    this.builder.append(i == 0 ? "" : ", ");
                    if (vars.get(i).getValue() != null) {
                        this.builder.append(this.dictionary.encode(vars.get(i).getValue()));
                    } else {
                        this.builder.append('$').append(vars.get(i).getName());
                        matchedVars.add(vars.get(i).getName());
                    }
                }
                this.builder.append(';');
                String separator = " ";
                for (final Expression conditionExpr : conditionExprs) {
                    this.builder.append(separator).append(conditionExpr.toString( //
                            "handler.test(" + register(conditionExpr) + ", ", ")"));
                    separator = ", ";
                }
                this.builder.append(")");

            } else if (expr instanceof Join) {
                final Join join = (Join) expr;
                final Set<String> leftVars = Algebra.extractVariables(join.getLeftArg(), true);
                final Set<Expression> leftConditionExprs = new HashSet<>();
                final Set<Expression> rightConditionExprs = new HashSet<>();
                for (final Expression conditionExpr : conditionExprs) {
                    if (leftVars.containsAll(conditionExpr.getVariables())) {
                        leftConditionExprs.add(conditionExpr);
                    } else {
                        rightConditionExprs.add(conditionExpr);
                    }
                }
                this.builder.append('(');
                translate(join.getLeftArg(), leftConditionExprs, extensionExprs, matchedVars);
                this.builder.append(" and ");
                translate(join.getRightArg(), rightConditionExprs, extensionExprs, matchedVars);
                this.builder.append(')');

            } else if (expr instanceof Union) {
                final Union union = (Union) expr;
                this.builder.append('(');
                translate(union.getLeftArg(), conditionExprs, extensionExprs, matchedVars);
                this.builder.append(" or ");
                translate(union.getRightArg(), conditionExprs, extensionExprs, matchedVars);
                this.builder.append(')');

            } else if (expr instanceof Extension) {
                final Extension extension = (Extension) expr;
                translate(extension.getArg(), conditionExprs, extensionExprs, matchedVars);
                for (final ExtensionElem elem : extension.getElements()) {
                    if (elem.getExpr() instanceof Var
                            && elem.getName().equals(((Var) elem.getExpr()).getName())) {
                        continue;
                    }
                    if (extensionExprs.put(elem.getName(), new Expression(elem.getExpr())) != null) {
                        throw new IllegalArgumentException("Multiple bindings for variable "
                                + elem.getName());
                    }
                }

            } else if (expr instanceof Filter) {
                final Filter filter = (Filter) expr;
                final ValueExpr condition = filter.getCondition();
                if (condition instanceof And) {
                    final ValueExpr leftCondition = ((And) condition).getLeftArg();
                    final ValueExpr rightCondition = ((And) condition).getRightArg();
                    translate(new Filter(new Filter(filter.getArg(), leftCondition),
                            rightCondition), conditionExprs, extensionExprs, matchedVars);
                } else {
                    String existsOperator = null;
                    TupleExpr existsArg = null;
                    if (condition instanceof Exists) {
                        existsOperator = "exists";
                        existsArg = ((Exists) condition).getSubQuery();
                    } else if (condition instanceof Not
                            && ((Not) condition).getArg() instanceof Exists) {
                        existsOperator = "not";
                        existsArg = ((Exists) ((Not) condition).getArg()).getSubQuery();
                    }
                    if (existsOperator == null) {
                        final Set<Expression> newConditionExprs = new HashSet<>(conditionExprs);
                        newConditionExprs.add(new Expression(condition));
                        translate(filter.getArg(), newConditionExprs, extensionExprs, matchedVars);
                    } else {
                        final boolean emptyArg = filter.getArg() instanceof EmptySet;
                        if (!emptyArg) {
                            this.builder.append('(');
                            translate(filter.getArg(), conditionExprs, extensionExprs, matchedVars);
                            this.builder.append(" and ");
                        } else if (!conditionExprs.isEmpty()) {
                            throw new IllegalArgumentException("Unsupported body pattern: " + expr);
                        }
                        this.builder.append(existsOperator).append('(');
                        translate(existsArg, Collections.emptySet(), extensionExprs,
                                new HashSet<>()); // existential variables in filter discarded
                        this.builder.append(")").append(emptyArg ? "" : ")");
                    }
                }

            } else {
                throw new IllegalArgumentException("Unsupported body pattern: " + expr);
            }
        }

        private int register(final Expression expression) {
            int index = this.expressions.indexOf(expression);
            if (index < 0) {
                index = this.expressions.size();
                this.expressions.add(expression);
            }
            return index;
        }

    }

    private static final class Expression {

        private final ValueExpr expr;

        private final List<String> variables;

        public Expression(final ValueExpr expr) {
            this.expr = expr;
            this.variables = new ArrayList<>(Algebra.extractVariables(expr, false));
            Collections.sort(this.variables);
        }

        public List<String> getVariables() {
            return this.variables;
        }

        public Value evaluate(final Value... args) {
            final ListBindingSet bindings = new ListBindingSet(this.variables, args);
            return Algebra.evaluateValueExpr(this.expr, bindings);
        }

        public int evaluate(final Dictionary dictionary, final int... argIDs) {
            return dictionary.encode(evaluate(dictionary.decode(argIDs)));
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Expression)) {
                return false;
            }
            final Expression other = (Expression) object;
            return this.expr.equals(other.expr);
        }

        @Override
        public int hashCode() {
            return this.expr.hashCode();
        }

        public String toString(final String prefix, final String suffix) {
            final StringBuilder builder = new StringBuilder();
            builder.append(prefix);
            for (int i = 0; i < this.variables.size(); ++i) {
                builder.append(i == 0 ? "" : ", ");
                builder.append("$").append(this.variables.get(i));
            }
            builder.append(suffix);
            return builder.toString();
        }

        @Override
        public String toString() {
            return toString("eval(", ")");
        }

    }

}
