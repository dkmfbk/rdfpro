package eu.fbk.rdfpro.rules;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.SESAME;
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
import eu.fbk.rdfpro.rules.model.QuadModel;
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
    private final QuadModel staticClosure;

    private final boolean dropBNodeTypes;

    private final boolean deduplicate;

    static RDFProcessor create(final String name, final String... args) throws IOException,
            RDFHandlerException {

        // Validate and parse options
        final Options options = Options.parse("r!|B!|p!|g!|t|C|c!|b!|w|u|*", args);

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
        final List<String> rdfRulesetURLs = new ArrayList<>();
        final List<String> dlogRulesetURLs = new ArrayList<>();
        final String rulesetNames = options.getOptionArg("r", String.class);
        for (final String rulesetName : rulesetNames.split(",")) {
            String location = Environment.getProperty("rdfpro.rules." + rulesetName);
            location = location != null ? location : rulesetName;
            final String url = IO.extractURL(location).toString();
            (url.endsWith(".dlog") ? dlogRulesetURLs : rdfRulesetURLs).add(url);
        }
        Ruleset ruleset = null;
        if (!rdfRulesetURLs.isEmpty()) {
            final RDFSource rulesetSource = RDFSources.read(true, preserveBNodes, base, null,
                    rdfRulesetURLs.toArray(new String[rdfRulesetURLs.size()]));
            try {
                ruleset = Ruleset.fromRDF(rulesetSource);
            } catch (final Throwable ex) {
                LOGGER.error("Invalid ruleset", ex);
                throw ex;
            }
        }
        if (!dlogRulesetURLs.isEmpty()) {
            final List<Ruleset> rulesets = new ArrayList<>();
            if (ruleset != null) {
                rulesets.add(ruleset);
            }
            for (final String dlogRulesetURL : dlogRulesetURLs) {
                try (Reader dlogReader = IO.utf8Reader(IO.read(dlogRulesetURL))) {
                    final List<Rule> rules = Rule.fromDLOG(dlogReader);
                    rulesets.add(new Ruleset(rules, null));
                }
            }
            ruleset = Ruleset.merge(rulesets.toArray(new Ruleset[rulesets.size()]));
        }

        // Transform ruleset
        ruleset = ruleset.rewriteVariables(bindings);
        URI globalURI = null;
        if (options.hasOption("G")) {
            final String u = options.getOptionArg("G", String.class);
            globalURI = (URI) Statements.parseValue(u.contains(":") ? u //
                    : u + ":", Namespaces.DEFAULT);
        }
        final String mode = options.getOptionArg("g", String.class, "none").trim();
        if ("global".equalsIgnoreCase(mode)) {
            ruleset = ruleset.rewriteGlobalGM(globalURI);
        } else if ("separate".equalsIgnoreCase(mode)) {
            ruleset = ruleset.rewriteSeparateGM();
        } else if ("star".equalsIgnoreCase(mode)) {
            ruleset = ruleset.rewriteStarGM(globalURI);
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

        // Read deduplicate flag
        final boolean deduplicate = options.hasOption("u");

        // Build processor
        return new RuleProcessor(ruleset, mapper, dropBNodeTypes, deduplicate, staticData,
                emitStatic, staticContext);
    }

    public RuleProcessor(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, final boolean deduplicate) {
        this(ruleset, mapper, dropBNodeTypes, deduplicate, null, false, null);
    }

    public RuleProcessor(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, final boolean deduplicate,
            @Nullable final RDFSource staticData, final boolean emitStatic,
            @Nullable final URI staticContext) {

        // Process ruleset and static data
        LOGGER.debug("Processing {} rules {} static data", ruleset.getRules().size(),
                staticData == null ? "without" : "with");
        final long ts = System.currentTimeMillis();
        Ruleset processedRuleset = ruleset.mergeSameWhereExpr();
        RuleEngine engine = RuleEngine.create(processedRuleset);
        QuadModel staticClosure = null;
        if (staticData != null) {
            staticClosure = QuadModel.create();
            try {
                staticData.emit(RDFHandlers.synchronize(RDFHandlers.wrap(staticClosure)), 1);
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }
            engine.eval(staticClosure);
            processedRuleset = processedRuleset.getDynamicRuleset(staticClosure)
                    .mergeSameWhereExpr();
            engine = RuleEngine.create(processedRuleset);
            if (!emitStatic) {
                staticClosure = null;
            } else if (staticContext != null) {
                final URI ctx = staticContext.equals(SESAME.NIL) ? null : staticContext;
                final List<Statement> stmts = new ArrayList<>(staticClosure);
                staticClosure.clear();
                for (final Statement stmt : stmts) {
                    staticClosure.add(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                            ctx);
                }
            }
        }
        LOGGER.info("{} initialized with {} dynamic rules (from {} rules) in {} ms", engine,
                processedRuleset.getRules().size(), ruleset.getRules().size(),
                System.currentTimeMillis() - ts);

        // Setup object
        this.engine = engine;
        this.mapper = mapper;
        this.staticClosure = staticClosure;
        this.dropBNodeTypes = dropBNodeTypes;
        this.deduplicate = deduplicate;
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
            result = RDFProcessors.inject(RDFSources.wrap(this.staticClosure)).wrap(result);
        }

        // Add decoupler so to ensure that output is dispatched to parallel threads
        result = RDFHandlers.decouple(result);

        // Filter the handler to perform inference. Handle two cases.
        if (this.mapper == null) {

            // (1) No mapper: just invoke the rule engine
            result = this.engine.eval(result, this.deduplicate);

        } else {

            // (2) Mapper configured: perform map/reduce and do inference on reduce phase
            result = RDFProcessors.mapReduce(this.mapper, new Reducer() {

                @Override
                public void reduce(final Value key, final Statement[] stmts,
                        final RDFHandler handler) throws RDFHandlerException {
                    final RDFHandler session = RuleProcessor.this.engine.eval(
                            RDFHandlers.ignoreMethods(handler, RDFHandlers.METHOD_START_RDF
                                    | RDFHandlers.METHOD_END_RDF | RDFHandlers.METHOD_CLOSE),
                            RuleProcessor.this.deduplicate);
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
