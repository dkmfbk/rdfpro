/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2015 by Francesco Corcoglioniti with support by Alessio Palmero Aprosio and Marco
 * Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;

final class ProcessorRules implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorRules.class);

    private final RuleEngine engine;

    @Nullable
    private final Mapper mapper;

    @Nullable
    private final QuadModel tboxClosure;

    private final boolean dropBNodeTypes;

    private final boolean deduplicate;

    static RDFProcessor create(final String name, final String... args)
            throws IOException, RDFHandlerException {

        // Validate and parse options
        final Options options = Options.parse("r!|B!|p!|g!|t|C|c!|b!|w|u|*", args);

        // Read base and preserve BNodes settings
        final boolean preserveBNodes = !options.hasOption("w");
        String base = options.getOptionArg("b", String.class);
        base = base == null ? null
                : Statements.parseValue(base.contains(":") ? base : base + ":", Namespaces.DEFAULT)
                        .stringValue();

        // Read bindings
        final String parameters = options.getOptionArg("B", String.class, "");
        final MapBindingSet bindings = new MapBindingSet();
        for (final String token : parameters.split("\\s+")) {
            final int index = token.indexOf('=');
            if (index >= 0) {
                bindings.addBinding(token.substring(0, index).trim(), Statements
                        .parseValue(token.substring(index + 1).trim(), Namespaces.DEFAULT));
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
            final RDFSource rulesetSource = RDFSources.read(true, preserveBNodes, base, null, null,
                    true, rdfRulesetURLs.toArray(new String[rdfRulesetURLs.size()]));
            try {
                ruleset = Ruleset.fromRDF(rulesetSource);
            } catch (final Throwable ex) {
                ProcessorRules.LOGGER.error("Invalid ruleset", ex);
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
        IRI globalIRI = null;
        if (options.hasOption("G")) {
            final String u = options.getOptionArg("G", String.class);
            globalIRI = (IRI) Statements.parseValue(u.contains(":") ? u //
                    : u + ":", Namespaces.DEFAULT);
        }
        final String mode = options.getOptionArg("g", String.class, "none").trim();
        if ("global".equalsIgnoreCase(mode)) {
            ruleset = ruleset.rewriteGlobalGM(globalIRI);
        } else if ("separate".equalsIgnoreCase(mode)) {
            ruleset = ruleset.rewriteSeparateGM();
        } else if ("star".equalsIgnoreCase(mode)) {
            ruleset = ruleset.rewriteStarGM(globalIRI);
        } else if (!"none".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("Unknown graph inference mode: " + mode);
        }

        // Read TBox closure settings
        boolean emitTBox = false;
        IRI tboxContext = null;
        if (options.hasOption("C")) {
            emitTBox = true;
        } else if (options.hasOption("c")) {
            emitTBox = true;
            final String ctx = options.getOptionArg("c", String.class);
            tboxContext = (IRI) Statements.parseValue(ctx.contains(":") ? ctx //
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

        // Read TBox data, if any
        final String[] tboxSpecs = options.getPositionalArgs(String.class).toArray(new String[0]);
        final RDFSource tboxData = tboxSpecs.length == 0 ? null
                : RDFProcessors
                        .track(new Tracker(ProcessorRules.LOGGER, null,
                                "%d TBox triples read (%d tr/s avg)", //
                                "%d TBox triples read (%d tr/s, %d tr/s avg)"))
                        .wrap(RDFSources.read(true, preserveBNodes, base, null, null, true,
                                tboxSpecs));

        // Read deduplicate flag
        final boolean deduplicate = options.hasOption("u");

        // Build processor
        return new ProcessorRules(ruleset, mapper, dropBNodeTypes, deduplicate, tboxData, emitTBox,
                tboxContext);
    }

    public ProcessorRules(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, final boolean deduplicate) {
        this(ruleset, mapper, dropBNodeTypes, deduplicate, null, false, null);
    }

    public ProcessorRules(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, final boolean deduplicate,
            @Nullable final RDFSource tboxData, final boolean emitTBox,
            @Nullable final IRI tboxContext) {

        // Process ruleset and static data
        ProcessorRules.LOGGER.debug("Processing {} rules {} TBox data", ruleset.getRules().size(),
                tboxData == null ? "without" : "with");
        final long ts = System.currentTimeMillis();
        Ruleset processedRuleset = ruleset.mergeSameWhereExpr();
        RuleEngine engine = RuleEngine.create(processedRuleset);
        QuadModel tboxClosure = null;
        if (tboxData != null) {
            tboxClosure = QuadModel.create();
            try {
                tboxData.emit(RDFHandlers.synchronize(RDFHandlers.wrap(tboxClosure)), 1);
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }
            engine.eval(tboxClosure);
            processedRuleset = processedRuleset.getABoxRuleset(tboxClosure).mergeSameWhereExpr();
            engine = RuleEngine.create(processedRuleset);
            if (!emitTBox) {
                tboxClosure = null;
            } else if (tboxContext != null) {
                final IRI ctx = tboxContext.equals(SESAME.NIL) ? null : tboxContext;
                final List<Statement> stmts = new ArrayList<>(tboxClosure);
                tboxClosure.clear();
                for (final Statement stmt : stmts) {
                    tboxClosure.add(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), ctx);
                }
            }
        }
        ProcessorRules.LOGGER.info("{} initialized with {} ABox rules (from {} rules) in {} ms",
                engine, processedRuleset.getRules().size(), ruleset.getRules().size(),
                System.currentTimeMillis() - ts);

        // Setup object
        this.engine = engine;
        this.mapper = mapper;
        this.tboxClosure = tboxClosure;
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

        // If necessary, filter the handler so to inject the TBox closure (in parallel)
        if (this.tboxClosure != null) {
            result = RDFProcessors.inject(RDFSources.wrap(this.tboxClosure)).wrap(result);
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
                    final RDFHandler session = ProcessorRules.this.engine.eval(
                            RDFHandlers.ignoreMethods(handler,
                                    RDFHandlers.METHOD_START_RDF | RDFHandlers.METHOD_END_RDF
                                            | RDFHandlers.METHOD_CLOSE),
                            ProcessorRules.this.deduplicate);
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
