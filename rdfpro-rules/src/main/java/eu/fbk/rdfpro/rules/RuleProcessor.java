package eu.fbk.rdfpro.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.Mapper;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.RDFSource;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.Reducer;
import eu.fbk.rdfpro.rules.RuleEngine.Callback;
import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;

public final class RuleProcessor implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RuleProcessor.class);

    private final RuleEngine engine;

    @Nullable
    private final Mapper mapper;

    @Nullable
    private final RDFSource staticClosure;

    private final boolean dropBNodeTypes;

    static RDFProcessor create(final String name, final String... args) throws RDFHandlerException {

        // Validate and parse options
        final Options options = Options.parse("r!|B!|e|g|p|t|C|c!|b!|w|*", args);

        // Read base and preserve BNodes settings
        final boolean preserveBNodes = !options.hasOption("w");
        String base = options.getOptionArg("b", String.class);
        base = base == null ? null : Statements.parseValue(base.contains(":") ? base : base + ":",
                Namespaces.DEFAULT).stringValue();

        // Read rulesets
        final List<String> rulesetURLs = new ArrayList<>();
        final String rulesetNames = options.getOptionArg("r", String.class);
        for (final String rulesetName : rulesetNames.split(",")) {
            String location = Environment.getProperty("rdfpro.rules." + rulesetName);
            location = location != null ? location : rulesetName;
            rulesetURLs.add(IO.extractURL(location).toString());
        }
        final RDFSource rulesetSource = RDFSources.read(true, preserveBNodes, base, null,
                rulesetURLs.toArray(new String[0]));
        final Model rulesetModel = new LinkedHashModel();
        rulesetSource.emit(RDFHandlers.wrap(rulesetModel), 1);

        // Read ruleset bindings
        final String parameters = options.getOptionArg("B", String.class, "");
        final MapBindingSet rulesetBindings = new MapBindingSet();
        for (final String token : parameters.split("\\s+")) {
            final int index = token.indexOf('=');
            if (index >= 0) {
                rulesetBindings.addBinding(token.substring(0, index).trim(), Statements
                        .parseValue(token.substring(index + 1).trim(), Namespaces.DEFAULT));
            }
        }

        // Read static closure settings
        final List<Statement> staticClosure = new ArrayList<>();
        RDFHandler staticSink = RDFHandlers.NIL;
        if (options.hasOption("C")) {
            staticSink = RDFHandlers.wrap(staticClosure);
        } else if (options.hasOption("c")) {
            final String ctx = options.getOptionArg("c", String.class);
            final URI uri = (URI) Statements.parseValue(ctx.contains(":") ? ctx //
                    : ctx + ":", Namespaces.DEFAULT);
            final URI actualURI = uri.equals(SESAME.NIL) ? null : uri;
            staticSink = new AbstractRDFHandlerWrapper(RDFHandlers.wrap(staticClosure)) {

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {
                    super.handleStatement(Statements.VALUE_FACTORY.createStatement(
                            stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), actualURI));
                }

            };
        }

        // Read bnode types settings
        final boolean dropBNodeTypes = options.hasOption("t");

        // Read Mapper for optional partitioning
        Mapper mapper = null;
        if (options.hasOption("g")) {
            mapper = Mapper.select("c");
        } else if (options.hasOption("e")) {
            mapper = Mapper.concat(Mapper.select("s"), Mapper.select("o"));
        } else if (options.hasOption("p")) {
            throw new UnsupportedOperationException("Rule-based partitioning not yet implemented");
        }

        // Read static data, if any
        final String[] staticSpecs = options.getPositionalArgs(String.class)
                .toArray(new String[0]);
        final RDFSource staticData = staticSpecs.length == 0 ? null : RDFProcessors.track(
                new Tracker(LOGGER, null, "%d static triples read (%d tr/s avg)", //
                        "%d static triples read (%d tr/s, %d tr/s avg)")).wrap(
                RDFSources.read(true, preserveBNodes, base, null, staticSpecs));

        // Perform preprocessing
        final RuleEngine engine = preprocess(rulesetModel, rulesetBindings, staticData, staticSink);

        // Build processor
        return new RuleProcessor(engine, mapper, staticClosure.isEmpty() ? null
                : RDFSources.wrap(staticClosure), dropBNodeTypes);
    }

    private RuleProcessor(final RuleEngine engine, @Nullable final Mapper mapper,
            @Nullable final RDFSource staticClosure, final boolean dropBNodeTypes) {

        this.engine = engine;
        this.mapper = mapper;
        this.staticClosure = staticClosure;
        this.dropBNodeTypes = dropBNodeTypes;
    }

    private static RuleEngine preprocess(final Model ruleset, @Nullable final BindingSet bindings,
            @Nullable final RDFSource staticData, final RDFHandler staticSink) {

        // Extract heads, static and dynamic bodies from ruleset data
        final Namespaces namespaces = Namespaces.forIterable(ruleset.getNamespaces(), false);
        final Map<String, TupleExpr> headMap = parse(ruleset, bindings, RR.HEAD, namespaces);
        final Map<String, TupleExpr> bodyMap = parse(ruleset, bindings, RR.BODY, namespaces);
        final Map<String, TupleExpr> dataMap = parse(ruleset, bindings, RR.DATA, namespaces);
        final Set<String> allIDs = new TreeSet<>();
        final Set<String> staticIDs = new TreeSet<>();
        final Set<String> dynamicIDs = new TreeSet<>();
        allIDs.addAll(headMap.keySet());
        allIDs.addAll(bodyMap.keySet());
        allIDs.addAll(dataMap.keySet());
        for (final Resource subj : ruleset.filter(null, RDF.TYPE, RR.STATIC_RULE).subjects()) {
            staticIDs.add(subj.stringValue());
        }
        for (final Resource subj : ruleset.filter(null, RDF.TYPE, RR.DYNAMIC_RULE).subjects()) {
            dynamicIDs.add(subj.stringValue());
        }

        // Build the dynamic engine and compute the static closure by handling two cases
        LOGGER.info("Processing {} rules {} static data", allIDs.size(),
                staticData == null ? "without" : "with");
        final long ts = System.currentTimeMillis();
        if (staticData == null) {

            // (1) Static data not provided, use input rules without static/dynamic distinction
            final RuleEngine.Builder builder = RuleEngine.builder(null);
            for (final String id : allIDs) {
                final TupleExpr head = headMap.get(id);
                final TupleExpr body = bodyMap.get(id);
                final TupleExpr data = dataMap.get(id);
                final TupleExpr actualBodyExpr = body == null ? data : data == null ? body
                        : new Join(data, body);
                builder.addRule(id, head, actualBodyExpr);
            }
            final RuleEngine engine = builder.build();
            LOGGER.info("Rule engine initialized with {} rules in {} ms", allIDs.size(),
                    System.currentTimeMillis() - ts);
            return engine;

        } else {

            // (2) Static data provided, use dynamic rules obtained by preprocessing.
            // First build the static rule engine
            final RuleEngine.Builder staticBuilder = RuleEngine.builder(null);
            final Set<String> mixedIDs = new HashSet<>();
            final Map<TupleExpr, DynamicRule> dynamicRules = new HashMap<>();
            for (final String id : allIDs) {
                final TupleExpr head = headMap.get(id);
                final TupleExpr body = bodyMap.get(id);
                final TupleExpr data = dataMap.get(id);
                if (staticIDs.contains(id)) {
                    staticBuilder.addRule(id + "_static", head, body == null ? data //
                            : data == null ? body : new Join(data, body));
                }
                if (dynamicIDs.contains(id) && data != null) {
                    staticBuilder.addRule(id, null, data);
                    mixedIDs.add(id);
                }
            }
            final RuleEngine staticEngine = staticBuilder.build();

            // Then run the static engine to compute the closure of static data, populating alo
            // dynamic rules based on matches found for the static body parts
            final RDFHandler handler = staticEngine.newSession(staticSink, new Callback() {

                @Override
                public boolean ruleTriggered(final RDFHandler handler, final String id,
                        final BindingSet bindings) {
                    if (mixedIDs.contains(id)) {
                        final TupleExpr head = Algebra.rewrite(headMap.get(id), bindings);
                        final TupleExpr body = Algebra.rewrite(bodyMap.get(id), bindings);
                        if (!head.equals(body)) {
                            DynamicRule.add(id, head, body, dynamicRules);
                        }
                    }
                    return true;
                }

            });
            try {
                staticData.emit(handler, 1);
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }

            // Complete the dynamic rules by adding all rules that are purely dynamic
            for (final String id : dynamicIDs) {
                if (!mixedIDs.contains(id)) {
                    DynamicRule.add(id, headMap.get(id), bodyMap.get(id), dynamicRules);
                }
            }

            // Build the dynamic engine
            final List<DynamicRule> sortedRules = new ArrayList<>(dynamicRules.values());
            Collections.sort(sortedRules);
            final RuleEngine.Builder dynamicBuilder = RuleEngine.builder(null);
            for (final DynamicRule rule : sortedRules) {
                dynamicBuilder.addRule(rule.getID(), rule.getHead(), rule.getBody());
            }
            final RuleEngine engine = dynamicBuilder.build();

            // Log results
            LOGGER.info("Rule engine initialized with {} dynamic rules in {} ms",
                    sortedRules.size(), System.currentTimeMillis() - ts);
            return engine;
        }
    }

    private static Map<String, TupleExpr> parse(final Model ruleset,
            @Nullable final BindingSet bindings, final URI property, final Namespaces namespaces) {

        final Map<String, TupleExpr> exprs = new HashMap<>();
        for (final Statement stmt : ruleset.filter(null, property, null)) {
            final String id = stmt.getSubject().stringValue();
            TupleExpr expr = Algebra.parseTupleExpr(stmt.getObject().stringValue(), null,
                    namespaces.uriMap());
            expr = Algebra.rewrite(expr, bindings);
            exprs.put(id, expr);
        }
        return exprs;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {

        // Start from the supplied handler
        RDFHandler result = handler;

        // If necessary, filter the handler so to drop <s rdf:type _:bnode> statements
        if (this.dropBNodeTypes) {
            result = new AbstractRDFHandlerWrapper(result) {

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {
                    if (!RDF.TYPE.equals(stmt.getPredicate())
                            || !(stmt.getObject() instanceof BNode)) {
                        super.handleStatement(stmt);
                    }
                }

            };
        }

        // If necessary, filter the handler so to inject the static closure (in parallel)
        if (this.staticClosure != null) {
            result = RDFProcessors.inject(this.staticClosure).wrap(result);
        }

        // Filter the handler to perform inference. Handle two cases.
        if (this.mapper == null) {

            // (1) No mapper: just invoke the rule engine
            result = this.engine.newSession(result);

        } else {

            // (2) Mapper configured: perform map/reduce and do inference on reduce phase
            result = RDFProcessors.mapReduce(this.mapper, new Reducer() {

                @Override
                public void reduce(final Value key, final Statement[] stmts,
                        final RDFHandler handler) throws RDFHandlerException {
                    final RDFHandler session = RuleProcessor.this.engine.newSession(RDFHandlers
                            .ignoreMethods(handler, RDFHandlers.METHOD_START_RDF
                                    | RDFHandlers.METHOD_END_RDF | RDFHandlers.METHOD_CLOSE));
                    try {
                        session.startRDF();
                        for (final Statement stmt : stmts) {
                            session.handleStatement(stmt);
                        }
                        session.endRDF();
                    } finally {
                        IO.closeQuietly(session);
                    }
                }

            }, true).wrap(result);

        }

        // Return the resulting handler after all the necessary wrappings
        return result;
    }

    private static final class DynamicRule implements Comparable<DynamicRule> {

        private final TupleExpr body;

        private final Set<TupleExpr> heads;

        private final Set<String> ids;

        private final int num;

        public DynamicRule(final TupleExpr body, final int num) {
            this.body = body;
            this.heads = new HashSet<>();
            this.ids = new HashSet<>();
            this.num = num;
        }

        public String getID() {
            return String.join("_", this.ids) + "_" + Integer.toHexString(this.num);
        }

        public TupleExpr getHead() {
            TupleExpr result = null;
            for (final TupleExpr expr : this.heads) {
                result = result == null ? expr : new Join(result, expr);
            }
            return result;
        }

        public TupleExpr getBody() {
            return this.body;
        }

        @Override
        public int compareTo(final DynamicRule other) {
            return getID().compareTo(other.getID());
        }

        public void add(final String id, final TupleExpr head) {
            this.ids.add(id);
            this.heads.add(head);
        }

        public static void add(final String id, final TupleExpr head, final TupleExpr body,
                final Map<TupleExpr, DynamicRule> map) {
            DynamicRule rule = map.get(body);
            if (rule == null) {
                rule = new DynamicRule(body, map.size());
                map.put(body, rule);
            }
            rule.add(id, head);
        }

    }

}
