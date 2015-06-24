package eu.fbk.rdfpro.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.BindingSet;
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
        final Options options = Options.parse("r!|B!|p!|g!|t|C|c!|b!|w|*", args);

        // Read base and preserve BNodes settings
        final boolean preserveBNodes = !options.hasOption("w");
        String base = options.getOptionArg("b", String.class);
        base = base == null ? null : Statements.parseValue(base.contains(":") ? base : base + ":",
                Namespaces.DEFAULT).stringValue();

        // Read bindings
        final String parameters = options.getOptionArg("B", String.class, "");
        final MapBindingSet bindings = new MapBindingSet();
        for (final String token : parameters.split("\\s+")) {
            final int index = token.indexOf('=');
            if (index >= 0) {
                bindings.addBinding(token.substring(0, index).trim(), Statements.parseValue(token
                        .substring(index + 1).trim(), Namespaces.DEFAULT));
            }
        }

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
        Ruleset ruleset = Ruleset.fromRDF(rulesetSource);

        // Transform ruleset
        ruleset = ruleset.transform(bindings);
        URI globalURI = null;
        if (options.hasOption("G")) {
            final String u = options.getOptionArg("G", String.class);
            globalURI = (URI) Statements.parseValue(u.contains(":") ? u //
                    : u + ":", Namespaces.DEFAULT);
        }
        final String mode = options.getOptionArg("g", String.class, "none").trim();
        if ("global".equalsIgnoreCase(mode)) {
            ruleset = ruleset.transformGlobalGM(globalURI);
        } else if ("separate".equalsIgnoreCase(mode)) {
            ruleset = ruleset.transformSeparateGM();
        } else if ("star".equalsIgnoreCase(mode)) {
            ruleset = ruleset.transformStarGM(globalURI);
        } else if (!"none".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("Unknown graph inference mode: " + mode);
        }

        // Read static closure settings
        boolean emitStatic = false;
        URI staticContext = null;
        if (options.hasOption("C")) {
            emitStatic = true;
        } else if (options.hasOption("c")) {
            emitStatic = true;
            final String ctx = options.getOptionArg("c", String.class);
            staticContext = (URI) Statements.parseValue(ctx.contains(":") ? ctx //
                    : ctx + ":", Namespaces.DEFAULT);
        }

        // Read bnode types settings
        final boolean dropBNodeTypes = options.hasOption("t");

        // Read Mapper for optional partitioning
        Mapper mapper = null;
        final String partitioning = options.getOptionArg("p", String.class, "none").trim();
        if ("entity".equalsIgnoreCase(partitioning)) {
            mapper = Mapper.concat(Mapper.select("s"), Mapper.select("o"));
        } else if ("graph".equalsIgnoreCase(partitioning)) {
            mapper = Mapper.select("c");
        } else if ("rules".equalsIgnoreCase(partitioning)) {
            throw new UnsupportedOperationException("Rule-based partitioning not yet implemented");
        } else if (!"none".equals(partitioning)) {
            throw new IllegalArgumentException("Unknown partitioning scheme: " + partitioning);
        }

        // Read static data, if any
        final String[] staticSpecs = options.getPositionalArgs(String.class)
                .toArray(new String[0]);
        final RDFSource staticData = staticSpecs.length == 0 ? null : RDFProcessors.track(
                new Tracker(LOGGER, null, "%d static triples read (%d tr/s avg)", //
                        "%d static triples read (%d tr/s, %d tr/s avg)")).wrap(
                RDFSources.read(true, preserveBNodes, base, null, staticSpecs));

        // Build processor
        return new RuleProcessor(ruleset, mapper, dropBNodeTypes, staticData, emitStatic,
                staticContext);
    }

    public RuleProcessor(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes) {
        this(ruleset, mapper, dropBNodeTypes, null, false, null);
    }

    public RuleProcessor(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, @Nullable final RDFSource staticData,
            final boolean emitStatic, @Nullable final URI staticContext) {

        // Setup the handler receiving closure of static data, if any.
        final List<Statement> staticClosure = new ArrayList<>();
        RDFHandler staticSink = RDFHandlers.NIL;
        if (staticData != null && emitStatic) {
            staticSink = RDFHandlers.wrap(staticClosure);
            if (staticContext != null) {
                final URI uri = staticContext.equals(SESAME.NIL) ? null : staticContext;
                staticSink = new AbstractRDFHandlerWrapper(staticSink) {

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        super.handleStatement(Statements.VALUE_FACTORY.createStatement(
                                stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), uri));
                    }

                };
            }
        }

        // Build the dynamic rule engine by closing static data and proprocessing rules
        final RuleEngine engine = preprocess(ruleset, staticData, staticSink);

        // Setup object
        this.engine = engine;
        this.mapper = mapper;
        this.staticClosure = RDFSources.wrap(staticClosure);
        this.dropBNodeTypes = dropBNodeTypes;
    }

    private static RuleEngine preprocess(final Ruleset ruleset,
            @Nullable final RDFSource staticData, final RDFHandler staticSink) {

        // Build the dynamic engine and compute the static closure by handling two cases
        LOGGER.info("Processing {} rules {} static data", ruleset.getRules().size(),
                staticData == null ? "without" : "with");
        final long ts = System.currentTimeMillis();
        if (staticData == null) {

            // (1) Static data not provided, use input rules without static/dynamic distinction
            final Ruleset mergedRuleset = ruleset.transformMergeHeads();
            final RuleEngine.Builder builder = RuleEngine.builder(null);
            for (final Rule rule : mergedRuleset.getRules()) {
                builder.addRule(rule.getID().stringValue(), rule.getHead(), rule.getBody());
            }
            final RuleEngine engine = builder.build();
            LOGGER.info("Rule engine initialized with {} rules in {} ms", mergedRuleset.getRules()
                    .size(), System.currentTimeMillis() - ts);
            return engine;

        } else {

            // (2) Static data provided, use dynamic rules obtained by preprocessing.
            // First build the static rule engine
            final RuleEngine.Builder staticBuilder = RuleEngine.builder(null);
            for (final Rule rule : ruleset.transformMergeHeads().getRules()) {
                staticBuilder.addRule(rule.getID().stringValue() + "__static", rule.getHead(),
                        rule.getBody());
            }
            for (final Rule rule : ruleset.getPreprocessingRuleset().getRules()) {
                staticBuilder.addRule(rule.getID().stringValue(), rule.getHead(), rule.getBody());
            }
            final RuleEngine staticEngine = staticBuilder.build();

            // Then run the static engine to compute the closure of static data and build the
            // dynamic ruleset based on bindings computed for preprocessing rules.
            final Map<URI, List<BindingSet>> bindings = new HashMap<>();
            final Map<String, List<BindingSet>> bindingsHelper = new HashMap<>();
            for (final Rule rule : ruleset.getPreprocessingRuleset().getRules()) {
                final List<BindingSet> list = new ArrayList<>();
                bindings.put(rule.getID(), list);
                bindingsHelper.put(rule.getID().stringValue(), list);
            }
            final RDFHandler handler = staticEngine.newSession(staticSink, new Callback() {

                @Override
                public boolean ruleTriggered(final RDFHandler handler, final String id,
                        final BindingSet bindings) {
                    final List<BindingSet> list = bindingsHelper.get(id);
                    if (list != null) {
                        list.add(bindings);
                    }
                    return true;
                }

            });
            try {
                staticData.emit(handler, 1);
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }
            final Ruleset dynamicRuleset = ruleset.getDynamicRuleset(bindings)
                    .transformMergeHeads();

            // Build the dynamic engine
            final RuleEngine.Builder dynamicBuilder = RuleEngine.builder(null);
            for (final Rule rule : dynamicRuleset.getRules()) {
                dynamicBuilder.addRule(rule.getID().stringValue(), rule.getHead(), rule.getBody());
            }
            final RuleEngine engine = dynamicBuilder.build();

            // Log results
            LOGGER.info("Rule engine initialized with {} dynamic rules in {} ms", dynamicRuleset
                    .getRules().size(), System.currentTimeMillis() - ts);
            return engine;
        }
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

}
