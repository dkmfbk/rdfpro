package eu.fbk.rdfpro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.drools.core.spi.KnowledgeHelper;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
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
import org.kie.api.definition.type.Position;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Algebra;
import eu.fbk.rdfpro.util.Statements;

public class RuleEngineDrools extends RuleEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuleEngineDrools.class);

    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    private final KieContainer container;

    private final Dictionary dictionary;

    private final List<Quad> axioms;

    private final List<Expression> expressions;

    private final List<IRI> ruleIDs;

    public RuleEngineDrools(final Ruleset ruleset) {

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
            this.dictionary = new Dictionary(RuleEngineDrools.this.dictionary);
            this.session = null;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.timestamp = System.currentTimeMillis();
            this.activations = new long[RuleEngineDrools.this.ruleIDs.size()];
            this.session = RuleEngineDrools.this.container.newKieSession();
            this.session.setGlobal("handler", this);
            for (final Quad axiom : RuleEngineDrools.this.axioms) {
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

            if (!Dictionary.isResource(subjectID) || !Dictionary.isIRI(predicateID)
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
            return RuleEngineDrools.this.expressions.get(expressionIndex).evaluate(this.dictionary,
                    argIDs);
        }

        public boolean test(final int expressionIndex, final int... argIDs) {
            try {
                return ((Literal) RuleEngineDrools.this.expressions.get(expressionIndex)
                        .evaluate(this.dictionary.decode(argIDs))).booleanValue();
            } catch (final Throwable ex) {
                return false;
            }
        }

    }

    public static final class Dictionary {

        static final int SIZE = 4 * 1024 * 1024 - 1;

        private final Value[] table;

        public Dictionary() {
            this.table = new Value[SIZE];
        }

        public Dictionary(final Dictionary source) {
            this.table = source.table.clone();
        }

        public Value[] decode(final int... ids) {
            final Value[] values = new Value[ids.length];
            for (int i = 0; i < ids.length; ++i) {
                values[i] = decode(ids[i]);
            }
            return values;
        }

        @Nullable
        public Value decode(final int id) {
            return this.table[id & 0x1FFFFFFF];
        }

        public int[] encode(final Value... values) {
            final int[] ids = new int[values.length];
            for (int i = 0; i < values.length; ++i) {
                ids[i] = encode(values[i]);
            }
            return ids;
        }

        public int encode(@Nullable final Value value) {
            if (value == null) {
                return 0;
            }
            int id = (value.hashCode() & 0x7FFFFFFF) % SIZE;
            if (id == 0) {
                id = 1; // 0 used for null context ID
            }
            final int initialID = id;
            while (true) {
                final Value storedValue = this.table[id];
                if (storedValue == null) {
                    this.table[id] = value;
                    break;
                }
                if (storedValue.equals(value)) {
                    break;
                }
                ++id;
                if (id == SIZE) {
                    id = 1;
                }
                if (id == initialID) {
                    throw new Error("Dictionary full (capacity " + SIZE + ")");
                }
            }
            if (value instanceof IRI) {
                return id;
            } else if (value instanceof BNode) {
                return id | 0x20000000;
            } else {
                return id | 0x40000000;
            }
        }

        public static boolean isResource(final int id) {
            return (id & 0x40000000) == 0;
        }

        public static boolean isIRI(final int id) {
            return (id & 0x60000000) == 0;
        }

        public static boolean isBNode(final int id) {
            return (id & 0x60000000) == 0x20000000;
        }

        public static boolean isLiteral(final int id) {
            return (id & 0x60000000) == 0x40000000;
        }

    }

    public static final class Quad {

        @Position(0)
        private final int subjectID;

        @Position(1)
        private final int predicateID;

        @Position(2)
        private final int objectID;

        @Position(3)
        private final int contextID;

        public Quad(final int subjectID, final int predicateID, final int objectID,
                final int contextID) {
            this.subjectID = subjectID;
            this.predicateID = predicateID;
            this.objectID = objectID;
            this.contextID = contextID;
        }

        public int getSubjectID() {
            return this.subjectID;
        }

        public int getPredicateID() {
            return this.predicateID;
        }

        public int getObjectID() {
            return this.objectID;
        }

        public int getContextID() {
            return this.contextID;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Quad)) {
                return false;
            }
            final Quad other = (Quad) object;
            return this.subjectID == other.subjectID && this.predicateID == other.predicateID
                    && this.objectID == other.objectID && this.contextID == other.contextID;
        }

        @Override
        public int hashCode() {
            return 7829 * this.subjectID + 1103 * this.predicateID + 137 * this.objectID
                    + this.contextID;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append('(');
            builder.append(this.subjectID);
            builder.append(", ");
            builder.append(this.predicateID);
            builder.append(", ");
            builder.append(this.objectID);
            builder.append(", ");
            builder.append(this.contextID);
            builder.append(')');
            return builder.toString();
        }

        public Statement decode(final Dictionary dictionary) {
            return Statements.VALUE_FACTORY.createStatement( //
                    (Resource) dictionary.decode(this.subjectID), //
                    (IRI) dictionary.decode(this.predicateID), //
                    dictionary.decode(this.objectID), //
                    (Resource) dictionary.decode(this.contextID));
        }

        public static Quad encode(final Dictionary dictionary, final Statement statement) {
            return new Quad( //
                    dictionary.encode(statement.getSubject()), //
                    dictionary.encode(statement.getPredicate()), //
                    dictionary.encode(statement.getObject()), //
                    dictionary.encode(statement.getContext()));
        }

        public static Quad encode(final Dictionary dictionary, final Resource subject,
                final IRI predicate, final Value object, final Resource context) {
            return new Quad( //
                    dictionary.encode(subject), //
                    dictionary.encode(predicate), //
                    dictionary.encode(object), //
                    dictionary.encode(context));
        }

    }

    private static final class Translation {

        private final StringBuilder builder;

        public final Dictionary dictionary;

        public final Set<Quad> axioms;

        public final List<Expression> expressions;

        public final List<IRI> ruleIDs;

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
            this.builder.append("import eu.fbk.rdfpro.RuleEngineDrools.Quad;\n");
            this.builder.append("import eu.fbk.rdfpro.RuleEngineDrools.Handler;\n");
            this.builder.append("import eu.fbk.rdfpro.RuleEngineDrools.Dictionary;\n");
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
                            throw new IllegalArgumentException(
                                    "Variable " + extensionVar + " already used in body patterns");
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
            final KieBaseModel base = module.newKieBaseModel("kbase_" + rulesetID).setDefault(true)
                    .setEqualsBehavior(EqualityBehaviorOption.EQUALITY)
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
                    if (extensionExprs.put(elem.getName(),
                            new Expression(elem.getExpr())) != null) {
                        throw new IllegalArgumentException(
                                "Multiple bindings for variable " + elem.getName());
                    }
                }

            } else if (expr instanceof Filter) {
                final Filter filter = (Filter) expr;
                final ValueExpr condition = filter.getCondition();
                if (condition instanceof And) {
                    final ValueExpr leftCondition = ((And) condition).getLeftArg();
                    final ValueExpr rightCondition = ((And) condition).getRightArg();
                    translate(
                            new Filter(new Filter(filter.getArg(), leftCondition), rightCondition),
                            conditionExprs, extensionExprs, matchedVars);
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
                            translate(filter.getArg(), conditionExprs, extensionExprs,
                                    matchedVars);
                            this.builder.append(" and ");
                        } else if (!conditionExprs.isEmpty()) {
                            throw new IllegalArgumentException(
                                    "Unsupported body pattern: " + expr);
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
