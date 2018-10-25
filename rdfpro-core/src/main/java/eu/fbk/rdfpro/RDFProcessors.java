/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2014 by Francesco Corcoglioniti and Alessio Palmero Aprosio with support by Marco
 * Amadori, Michele Mostarda and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Writer;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Algebra;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.KeyQuadIndex;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Scripting;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;
import eu.fbk.rdfpro.vocab.VOIDX;

/**
 * Utility methods dealing with {@code RDFProcessor}s.
 */
public final class RDFProcessors {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFProcessors.class);

    /** The null {@code RDFProcessor} that always produces an empty RDF stream. */
    public static final RDFProcessor NIL = new RDFProcessor() {

        @Override
        public RDFHandler wrap(final RDFHandler handler) {
            return RDFHandlers.ignoreMethods(Objects.requireNonNull(handler),
                    RDFHandlers.METHOD_HANDLE_COMMENT | RDFHandlers.METHOD_HANDLE_NAMESPACE
                            | RDFHandlers.METHOD_HANDLE_STATEMENT);
        }

    };

    /** The identity {@code RDFProcessor} that returns the input RDF stream unchanged. */
    public static final RDFProcessor IDENTITY = new RDFProcessor() {

        @Override
        public RDFHandler wrap(final RDFHandler handler) {
            return Objects.requireNonNull(handler);
        }

    };

    private RDFProcessors() {
    }

    private static String[] tokenize(final String spec) {

        final List<String> tokens = new ArrayList<String>();

        final StringBuilder builder = new StringBuilder();
        boolean quoted = false;
        boolean escaped = false;
        int start = -1;

        for (int i = 0; i < spec.length(); ++i) {
            final char ch = spec.charAt(i);
            final boolean ws = Character.isWhitespace(ch);
            if (ch == '\\' && !escaped) {
                escaped = true;
            } else {
                if (start < 0) {
                    if (!ws) {
                        start = i;
                        quoted = ch == '\'' || ch == '\"';
                        builder.setLength(0);
                        if (!quoted) {
                            builder.append(ch);
                        }
                    }
                } else {
                    final boolean tokenChar = escaped || quoted && ch != spec.charAt(start)
                            || !quoted && !ws;
                    if (tokenChar) {
                        builder.append(ch);
                    }
                    if (!tokenChar || i == spec.length() - 1) {
                        tokens.add(builder.toString());
                        start = -1;
                        quoted = false;
                    }
                }
                escaped = false;
            }
        }

        return tokens.toArray(new String[tokens.size()]);
    }

    @Nullable
    private static IRI parseIRI(@Nullable final String string) {
        if (string == null) {
            return null;
        } else if (!string.contains(":")) {
            return (IRI) Statements.parseValue(string + ":", Namespaces.DEFAULT);
        } else {
            return (IRI) Statements.parseValue(string, Namespaces.DEFAULT);
        }
    }

    static RDFProcessor create(final String name, final String... args) {

        switch (name) {
        case "r":
        case "read": {
            final Options options = Options.parse("b!|w|l|s|d|q|+", args);
            final String[] fileSpecs = options.getPositionalArgs(String.class)
                    .toArray(new String[0]);
            final boolean preserveBNodes = !options.hasOption("w");
            final boolean lenient = options.hasOption("l");
            final boolean sequential = options.hasOption("s");
            final boolean dumpErrors = options.hasOption("d");
            final boolean quiet = options.hasOption("q");
            final IRI base = RDFProcessors.parseIRI(options.getOptionArg("b", String.class));
            final Function<String, Writer> errorWriterSupplier = !dumpErrors ? null : loc -> {
                try {
                    final String origLoc = IO.extractURL(loc).toString();
                    final String origExt = IO.extractExtension(origLoc);
                    final String baseLoc = loc.substring(0, loc.length() - origExt.length());
                    final String errLoc = baseLoc + ".error" + origExt;
                    return IO.utf8Writer(IO.buffer(IO.write(errLoc)));
                } catch (final Throwable ex) {
                    throw new RuntimeException(ex);
                }
            };
            return RDFProcessors.read(!sequential, preserveBNodes,
                    base == null ? null : base.stringValue(), Statements.newParserConfig(lenient),
                    errorWriterSupplier, !quiet, fileSpecs);
        }

        case "w":
        case "write": {
            final Options options = Options.parse("c!|+", args);
            final int chunkSize = options.getOptionArg("c", Integer.class, 1);
            final String[] locations = options.getPositionalArgs(String.class)
                    .toArray(new String[0]);
            return RDFProcessors.write(null, chunkSize, locations);
        }

        case "tsv": {
            final Options options = Options.parse("p!|q!|!", args);
            final Mapper mapper = Mapper.parse(options.getOptionArg("p", String.class));
            final String query = options.getOptionArg("q", String.class,
                    "SELECT ?s ?p ?o ?c { GRAPH ?c { ?s ?p ?o } }");
            final String location = options.getPositionalArg(0, String.class);
            return RDFProcessors.tsv(mapper, query, location);
        }

        case "t":
        case "transform": {
            final Options options = Options.parse("+", args);
            final String spec = String.join(" ", options.getPositionalArgs(String.class));
            final Transformer transformer = Transformer.parse(spec);
            return RDFProcessors.transform(transformer);
        }

        case "u":
        case "unique": {
            final Options options = Options.parse("m", args);
            return RDFProcessors.unique(options.hasOption("m"));
        }

        case "p":
        case "prefix": {
            final Options options = Options.parse("f!", args);
            final String source = options.getOptionArg("f", String.class);
            Namespaces namespaces = Namespaces.DEFAULT;
            if (source != null) {
                try {
                    URL url;
                    final File file = new File(source);
                    if (file.exists()) {
                        url = file.toURI().toURL();
                    } else {
                        url = RDFProcessors.class.getClassLoader().getResource(source);
                    }
                    namespaces = Namespaces.load(Collections.singleton(url), false);
                } catch (final Throwable ex) {
                    throw new IllegalArgumentException(
                            "Cannot load prefix/namespace bindings from " + source + ": "
                                    + ex.getMessage(),
                            ex);
                }
            }
            return RDFProcessors.prefix(namespaces.prefixMap());
        }

        case "smush": {
            final Options options = Options.parse("x|s|c!|b!|w|*", args);
            final String[] namespaces = options.getPositionalArgs(String.class)
                    .toArray(new String[0]);
            RDFSource sameAsSource = null;
            final boolean emitSameAs = !options.hasOption("s");
            if (options.hasOption("c")) {
                final IRI base = RDFProcessors.parseIRI(options.getOptionArg("b", String.class));
                final boolean preserveBNodes = !options.hasOption("w");
                final String fileSpec = options.getOptionArg("c", String.class);
                sameAsSource = RDFProcessors
                        .track(new Tracker(RDFProcessors.LOGGER, null,
                                "%d smush triples read (%d tr/s avg)", //
                                "%d smush triples read (%d tr/s, %d tr/s avg)"))
                        .wrap(RDFSources.read(true, preserveBNodes,
                                base == null ? null : base.stringValue(), null, null, true,
                                fileSpec));
            }
            final boolean hasSmushEasterEgg = options.hasOption("x");
            if (hasSmushEasterEgg) {
                // Below you can find one of the most important contributions by Alessio :-)
                // (google for 'smush' if you wonder why we added this easter egg)
                final PrintStream p = System.out;
                p.println();
                p.println(".==================================================================.");
                p.println("||    ( )              ( )                ( )              ( )    ||");
                p.println("|'================================================================'|");
                p.println("||                                                                ||");
                p.println("||                                                                ||");
                p.println("||                                  .::::.                        ||");
                p.println("||                                .::::::::.                      ||");
                p.println("||                                :::::::::::                     ||");
                p.println("||                                ':::::::::::..                  ||");
                p.println("||                                 :::::::::::::::'               ||");
                p.println("||                                  ':::::::::::.                 ||");
                p.println("||                                    .::::::::::::::'            ||");
                p.println("||                                  .:::::::::::...               ||");
                p.println("||                                 ::::::::::::::''               ||");
                p.println("||                     .:::.       '::::::::''::::                ||");
                p.println("||                   .::::::::.      ':::::'  '::::               ||");
                p.println("||                  .::::':::::::.    :::::    '::::.             ||");
                p.println("||                .:::::' ':::::::::. :::::      ':::.            ||");
                p.println("||              .:::::'     ':::::::::.:::::       '::.           ||");
                p.println("||            .::::''         '::::::::::::::       '::.          ||");
                p.println("||           .::''              '::::::::::::         :::...      ||");
                p.println("||        ..::::                  ':::::::::'        .:' ''''     ||");
                p.println("||     ..''''':'                    ':::::.'                      ||");
                p.println("||                                                                ||");
                p.println("||                                                                ||");
                p.println("|'================================================================'|");
                p.println("||              __________________                                ||");
                p.println(
                        "||              | ___ \\  _  \\  ___|                               ||");
                p.println("||              | |_/ / | | | |_                                  ||");
                p.println("||              |    /| | | |  _|                                 ||");
                p.println(
                        "||              | |\\ \\| |/ /| |  ___  ___  ____                   ||");
                p.println(
                        "||              \\_| \\_|___/ \\_| / _ \\/ _ \\/ __ \\                  ||");
                p.println("||                             / ___/ , _/ /_/ /                  ||");
                p.println("||                            /_/  /_/|_|\\____/                   ||");
                p.println("||                                                                ||");
                p.println("'=============================================================LGB=='");
                p.println();
            }

            for (int i = 0; i < namespaces.length; ++i) {
                namespaces[i] = RDFProcessors.parseIRI(namespaces[i]).stringValue();
            }
            return RDFProcessors.smush(sameAsSource, emitSameAs, namespaces);
        }

        case "tbox": {
            Options.parse("", args);
            return RDFProcessors.tbox();
        }

        case "cbd": {
            final Options options = Options.parse("n!|s|i|c", args);
            final IRI namespace = RDFProcessors.parseIRI(options.getOptionArg("n", String.class));
            final boolean symmetric = options.hasOption("s");
            final boolean includeInstances = options.hasOption("i");
            final boolean includeContexts = options.hasOption("c");
            return RDFProcessors.cbd(namespace.stringValue(), symmetric, includeInstances,
                    includeContexts);
        }

        case "rdfs": {
            final Options options = Options.parse("d|e!|C|c!|b!|t|w|+", args);
            final IRI base = RDFProcessors.parseIRI(options.getOptionArg("b", String.class));
            final boolean preserveBNodes = !options.hasOption("w");
            final String[] fileSpecs = options.getPositionalArgs(String.class)
                    .toArray(new String[0]);
            final RDFSource tbox = RDFProcessors
                    .track(new Tracker(RDFProcessors.LOGGER, null,
                            "%d TBox triples read (%d tr/s avg)", //
                            "%d TBox triples read (%d tr/s, %d tr/s avg)"))
                    .wrap(RDFSources.read(true, preserveBNodes,
                            base == null ? null : base.stringValue(), null, null, true,
                            fileSpecs));
            final boolean decomposeOWLAxioms = options.hasOption("d");
            final boolean dropBNodeTypes = options.hasOption("t");
            String[] excludedRules = new String[0];
            if (options.hasOption("e")) {
                excludedRules = options.getOptionArg("e", String.class).split(",");
            }
            IRI context = null;
            if (options.hasOption("C")) {
                context = SESAME.NIL;
            } else if (options.hasOption("c")) {
                context = RDFProcessors.parseIRI(options.getOptionArg("c", String.class));
            }
            return RDFProcessors.rdfs(tbox, context, decomposeOWLAxioms, dropBNodeTypes,
                    excludedRules);
        }

        case "stats": {
            final Options options = Options.parse("n!|p!|c!|t!|o", args);
            final IRI namespace = RDFProcessors.parseIRI(options.getOptionArg("n", String.class));
            final IRI property = RDFProcessors.parseIRI(options.getOptionArg("p", String.class));
            final IRI context = RDFProcessors.parseIRI(options.getOptionArg("c", String.class));
            final Long threshold = options.getOptionArg("t", Long.class);
            final boolean processCooccurrences = options.hasOption("o");
            return RDFProcessors.stats(namespace == null ? null : namespace.stringValue(),
                    property, context, threshold, processCooccurrences);
        }

        case "download": {
            final Options options = Options.parse("w|q!|f!|!", args);
            final boolean preserveBNodes = !options.hasOption("w");
            final String endpointURL = options.getPositionalArg(0, String.class);
            String query = options.getOptionArg("q", String.class);
            if (query == null) {
                final String source = options.getOptionArg("f", String.class);
                if (source == null) {
                    query = "select ?s ?p ?o ?c { graph ?c { ?s ?p ?o } }";
                } else {
                    try {
                        final File file = new File(source);
                        URL url;
                        if (file.exists()) {
                            url = file.toURI().toURL();
                        } else {
                            url = RDFProcessors.class.getClassLoader().getResource(source);
                        }
                        final BufferedReader reader = new BufferedReader(
                                new InputStreamReader(url.openStream()));
                        try {
                            final StringBuilder builder = new StringBuilder();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                builder.append(line);
                            }
                            query = builder.toString();
                        } finally {
                            IO.closeQuietly(reader);
                        }
                    } catch (final Throwable ex) {
                        throw new IllegalArgumentException(
                                "Cannot load SPARQL query from " + source + ": " + ex.getMessage(),
                                ex);
                    }
                }
            }
            return RDFProcessors.download(true, preserveBNodes, endpointURL, query);
        }

        case "upload": {
            final Options options = Options.parse("!", args);
            final String endpointURL = RDFProcessors
                    .parseIRI(options.getPositionalArg(0, String.class)).stringValue();
            return RDFProcessors.upload(endpointURL);
        }

        case "mapreduce": {
            final Options options = Options.parse("b!|r!|e!|a!|u|+", args);
            final boolean deduplicate = options.hasOption("u");
            final String bypassExp = options.getOptionArg("b", String.class);
            final String existsExp = options.getOptionArg("e", String.class);
            final String forallExp = options.getOptionArg("a", String.class);
            final String reducerExp = options.getOptionArg("r", String.class);
            final Predicate<Statement> bypassPred = Statements.statementMatcher(bypassExp);
            final Predicate<Statement> existsPred = Statements.statementMatcher(existsExp);
            final Predicate<Statement> forallPred = Statements.statementMatcher(forallExp);
            Reducer reducer = reducerExp == null ? Reducer.IDENTITY //
                    : Reducer.parse(reducerExp);
            reducer = Reducer.filter(reducer, existsPred, forallPred);
            final List<Mapper> mappers = new ArrayList<>();
            for (final String mapperExp : options.getPositionalArgs(String.class)) {
                mappers.add(Mapper.parse(mapperExp));
            }
            Mapper mapper = Mapper.concat(mappers.toArray(new Mapper[mappers.size()]));
            if (bypassPred != null) {
                mapper = Mapper.bypass(mapper, bypassPred);
            }
            return RDFProcessors.mapReduce(mapper, reducer, deduplicate);
        }

        case "kvread": {
            final Options options = Options.parse("m!|r!|!", args);
            final File file = new File(options.getPositionalArg(0, String.class));
            final Mapper mapper = Mapper.parse(options.getOptionArg("m", String.class, "s"));
            Predicate<Value> recurseMatcher = null;
            if (options.hasOption("r")) {
                final String[] nss = options.getOptionArg("r", String.class, "").split("[\\s,;]+");
                recurseMatcher = v -> {
                    if (v instanceof IRI) {
                        final String s = v.stringValue();
                        for (final String ns : nss) {
                            if (s.startsWith(ns)) {
                                return true;
                            }
                        }
                    }
                    return false;
                };
            }
            return RDFProcessors.kvread(file, mapper, recurseMatcher);
        }

        case "kvwrite": {
            final Options options = Options.parse("m!|!", args);
            final File file = new File(options.getPositionalArg(0, String.class));
            final Mapper mapper = Mapper.parse(options.getOptionArg("m", String.class, "s"));
            return RDFProcessors.kvwrite(file, mapper);
        }

        default:
            throw new Error("Unsupported " + name + " processor, check properties file");
        }
    }

    /**
     * Creates an {@code RDFProcessor} by parsing the supplied specification string(s). The
     * specification can be already tokenized or the method can be asked by tokenize it itself
     * (set {@code tokenize = true}).
     *
     * @param tokenize
     *            true if input string(s) should be tokenized (again)
     * @param args
     *            the input string(s)
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor parse(final boolean tokenize, final String... args) {
        List<String> list;
        if (tokenize) {
            list = new ArrayList<>();
            for (final String arg : args) {
                list.addAll(Arrays.asList(RDFProcessors.tokenize(arg)));
            }
        } else {
            list = Arrays.asList(args);
        }
        return new Parser(list).parse();
    }

    /**
     * Returns an {@code RDFProcessor} performing the parallel composition of the processors
     * specified, using the given {@code SetOperator} to merge their results.
     *
     * @param operator
     *            the {@code SetOperator} to use for merging the results of composed processors,
     *            not null
     * @param processors
     *            the processors to compose in parallel
     * @return the resulting {@code RDFProcessor}
     */
    public static RDFProcessor parallel(final SetOperator operator,
            final RDFProcessor... processors) {

        Objects.requireNonNull(operator);

        if (processors.length == 0) {
            throw new IllegalArgumentException(
                    "At least one processor should be supplied " + "in a parallel composition");
        }

        int count = 0;
        for (final RDFProcessor processor : processors) {
            count = Math.max(count, processor.getExtraPasses());
        }
        final int extraPasses = count;

        return new RDFProcessor() {

            @Override
            public int getExtraPasses() {
                return extraPasses;
            }

            @Override
            public RDFHandler wrap(final RDFHandler handler) {

                Objects.requireNonNull(handler);

                final int numProcessors = processors.length;

                final int[] extraPasses = new int[numProcessors];
                final RDFHandler[] handlers = RDFHandlers.collect(handler, numProcessors,
                        operator);

                for (int i = 0; i < numProcessors; ++i) {
                    final RDFProcessor processor = processors[i];
                    extraPasses[i] = processor.getExtraPasses();
                    handlers[i] = processor.wrap(handlers[i]);
                }

                return RDFHandlers.dispatchAll(handlers, extraPasses);
            }

        };
    }

    /**
     * Returns an {@code RDFProcessor} performing the sequence composition of the supplied
     * {@code RDFProcessors}. In a sequence composition, the first processor is applied first to
     * the stream, with its output fed to the next processor and so on.
     *
     * @param processors
     *            the processor to compose in a sequence
     * @return the resulting {@code RDFProcessor}
     */
    public static RDFProcessor sequence(final RDFProcessor... processors) {

        if (processors.length == 0) {
            throw new IllegalArgumentException(
                    "At least one processor should be supplied " + "in a sequence composition");
        }

        if (processors.length == 1) {
            return Objects.requireNonNull(processors[0]);
        }

        int count = 0;
        for (final RDFProcessor processor : processors) {
            count += processor.getExtraPasses();
        }
        final int extraPasses = count;

        return new RDFProcessor() {

            @Override
            public int getExtraPasses() {
                return extraPasses;
            }

            @Override
            public RDFHandler wrap(final RDFHandler handler) {
                RDFHandler result = Objects.requireNonNull(handler);
                for (int i = processors.length - 1; i >= 0; --i) {
                    result = processors[i].wrap(result);
                }
                return result;
            }
        };
    }

    /**
     * Creates an {@code RDFProcessor} that processes the RDF stream in a MapReduce fashion. The
     * method is parameterized by a {@link Mapper} and a {@link Reducer} object, which perform the
     * actual computation, and a {@code deduplicate} flag that controls whether duplicate
     * statements mapped to the same key by the mapper should be merged. MapReduce is performed
     * relying on external sorting: input statements are mapped to a {@code Value} key, based on
     * which they are sorted (externally); each key partition is then fed to the reducer and the
     * reducer output emitted. Hadoop is not involved :-) - this scheme is limited to a single
     * machine environment on one hand; on the other it exploits this limitation by using
     * available memory to encode sorted data, thus limiting its volume and speeding up the
     * operation.
     *
     * @param mapper
     *            the mapper, not null
     * @param reducer
     *            the reducer, not null
     * @param deduplicate
     *            true if duplicate statements mapped to the same key should be merged
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor mapReduce(final Mapper mapper, final Reducer reducer,
            final boolean deduplicate) {
        return new ProcessorMapReduce(mapper, reducer, deduplicate);
    }

    /**
     * Creates an {@code RDFProcessor} that augments the RDF stream with prefix-to-namespace
     * bindings from the supplied map or from {@code prefix.cc}. NOTE: if a map is supplied, it is
     * important it is not changed externally while the produced {@code RDFProcessor} is in use,
     * as this will alter the RDF stream produced at each pass and may cause race conditions.
     *
     * @param nsToPrefixMap
     *            the prefix-to-namespace map to use; if null, a builtin map derived from data of
     *            {@code prefix.cc} will be used
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor prefix(@Nullable final Map<String, String> nsToPrefixMap) {
        return new ProcessorPrefix(nsToPrefixMap);
    }

    /**
     * Creates an {@code RDFProcessor} that computes the RDFS closure of the RDF stream based on
     * the TBox separately supplied.
     *
     * @param tbox
     *            a {@code RDFSource} providing access to TBox data, not null
     * @param tboxContext
     *            the context where to emit TBox data; if null TBox is not emitted (use
     *            {@link SESAME#NIL} for emitting data in the default context)
     * @param decomposeOWLAxioms
     *            true if simple OWL axioms mappable to RDFS (e.g. {@code owl:equivalentClass}
     *            should be decomposed to corresponding RDFS axioms (OWL axioms are otherwise
     *            ignored when computing the closure)
     * @param dropBNodeTypes
     *            true if {@code <x rdf:type _:b>} statements should not be emitted (as
     *            uninformative); note that this option does not prevent this statements to be
     *            used for inference (even if dropped), possibly leading to infer statements that
     *            are not dropped
     * @param excludedRules
     *            a vararg array with the names of the RDFS rule to exclude; if empty, all the
     *            RDFS rules will be used
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor rdfs(final RDFSource tbox, @Nullable final Resource tboxContext,
            final boolean decomposeOWLAxioms, final boolean dropBNodeTypes,
            final String... excludedRules) {
        return new ProcessorRDFS(tbox, tboxContext, decomposeOWLAxioms, dropBNodeTypes,
                excludedRules);
    }

    /**
     * Creates an {@code RDFProcessor} performing {@code owl:sameAs} smushing. A ranked list of
     * namespaces controls the selection of the canonical IRI for each coreferring IRI cluster.
     * {@code owl:sameAs} statements are emitted in output linking the selected canonical IRI to
     * the other entity aliases. An optional {@code RDFSource} parameter may be supplied to
     * provide access to the {@code owl:sameAs} statements resulting from a previous smush
     * operation (i.e, statements mapping canonical IRIs to all their possible aliases); if such
     * source is supplied, sameAs statements in the input stream will be ignored and only the
     * {@code owl:sameAs} information in the source will be used for smushing.
     *
     * @param sameAsSource
     *            an optional {@code RDFSource} providing access to the {@code owl:sameAs}
     *            statements from a previous smush operation
     * @param emitSameAs
     *            true if an {@code owl:sameAs} statement has to be emitted for each node being
     *            canonicalized
     * @param rankedNamespaces
     *            the ranked list of namespaces used to select canonical IRIs
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor smush(@Nullable final RDFSource sameAsSource,
            final boolean emitSameAs, final String... rankedNamespaces) {
        return new ProcessorSmush2(sameAsSource, emitSameAs, rankedNamespaces);
    }

    /**
     * Creates an {@code RDFProcessor} extracting VOID structural statistics from the RDF stream.
     * A VOID dataset is associated to the whole input and to each set of graphs associated to the
     * same 'source' IRI with a configurable property, specified by {@code sourceProperty}; if
     * parameter {@code sourceContext} is not null, these association statements are searched only
     * in the graph with the IRI specified. Class and property partitions are then generated for
     * each of these datasets, assigning them IRIs in the namespace given by
     * {@code outputNamespace} (if null, a default namespace is used). In addition to standard
     * VOID terms, the processor emits additional statements based on the {@link VOIDX} extension
     * vocabulary to express the number of TBox, ABox, {@code rdf:type} and {@code owl:sameAs}
     * statements, the average number of properties per entity and informative labels and examples
     * for each TBox term, which are then viewable in tools such as Protégé. Internally, the
     * processor makes use of external sorting to (conceptually) sort the RDF stream twice: first
     * based on the subject to group statements about the same entity and compute entity-based and
     * distinct subjects statistics; then based on the object to compute distinct objects
     * statistics. Therefore, computing VOID statistics is quite a slow operation.
     *
     * @param outputNamespace
     *            the namespace for generated IRIs (if null, a default is used)
     * @param sourceProperty
     *            the IRI of property linking graphs to sources (if null, sources will not be
     *            considered)
     * @param sourceContext
     *            the graph where to look for graph-to-source links (if null, will be searched in
     *            the whole RDF stream)
     * @param threshold
     *            the minimum number of statements or entities that a VOID partition must have in
     *            order to be emitted; this parameter allows to drop VOID partitions for
     *            infrequent concepts, sensibly reducing the output size
     * @param processCooccurrences
     *            true to enable analysis of co-occurrences for computing {@code void:classes} and
     *            {@code void:properties} statements
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor stats(@Nullable final String outputNamespace,
            @Nullable final IRI sourceProperty, @Nullable final IRI sourceContext,
            @Nullable final Long threshold, final boolean processCooccurrences) {
        return new ProcessorStats(outputNamespace, sourceProperty, sourceContext, threshold,
                processCooccurrences);
    }

    /**
     * Returns a {@code RDFProcessor} that extracts the TBox of data in the RDF stream.
     *
     * @return the TBox-extracting {@code RDFProcessor}
     */
    public static RDFProcessor tbox() {
        return ProcessorTBox.INSTANCE;
    }

    /**
     * Returns a {@code RDFProcessor} that extracts the CBD of matched entities in the RDF stream.
     * See https://www.w3.org/Submission/CBD/.
     *
     * @param namespace
     *            the namespace of the entities to match
     * @param symmetric
     *            whether a symmetric CBD has to be emitted
     * @param includeInstances
     *            whether to include instances of matched entities
     * @param includeContexts
     *            whether to include quads whose context is a matched entity
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor cbd(final String namespace, final boolean symmetric,
            final boolean includeInstances, final boolean includeContexts) {
        return new ProcessorCBD(ImmutableSet.of(namespace), symmetric, includeInstances,
                includeContexts);
    }

    /**
     * Returns a {@code RDFProcessor} that applies the supplied {@code Transformer} to each input
     * triple, producing the transformed triples in output.
     *
     * @param transformer
     *            the transformer, not null
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor transform(final Transformer transformer) {
        Objects.requireNonNull(transformer);
        return new RDFProcessor() {

            @Override
            public RDFHandler wrap(final RDFHandler handler) {
                return new AbstractRDFHandlerWrapper(Objects.requireNonNull(handler)) {

                    @Override
                    public void handleStatement(final Statement statement)
                            throws RDFHandlerException {
                        transformer.transform(statement, this.handler);
                    }

                };
            }

        };
    }

    /**
     * Creates an {@code RDFProcessor} that removes duplicate from the RDF stream, optionally
     * merging similar statements with different contexts in a unique statement.
     *
     * @param mergeContexts
     *            true if statements with same subject, predicate and object but different context
     *            should be merged in a single statement, whose context is a combination of the
     *            source contexts
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor unique(final boolean mergeContexts) {
        return new ProcessorUnique(mergeContexts);
    }

    /**
     * Creates an {@code RDFProcessor} that injects in the RDF stream the data loaded from the
     * specified {@code RDFSource}. Data is read and injected at every pass on the RDF stream.
     *
     * @param source
     *            the {@code RDFSource}, not null
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor inject(final RDFSource source) {
        Objects.requireNonNull(source);
        return new RDFProcessor() {

            @Override
            public RDFHandler wrap(final RDFHandler handler) {
                return new InjectSourceHandler(Objects.requireNonNull(handler), source, null);
            }

        };
    }

    /**
     * Creates an {@code RDFProcessor} that reads data from the files specified and inject it in
     * the RDF stream at each pass. This is a utility method that relies on
     * {@link #inject(RDFSource)}, on
     * {@link RDFSources#read(boolean, boolean, String, ParserConfig, Function, boolean, String...)}
     * and on {@link #track(Tracker)} for providing progress information on loaded statements.
     *
     * @param parallelize
     *            false if files should be parsed sequentially using only one thread
     * @param preserveBNodes
     *            true if BNodes in parsed files should be preserved, false if they should be
     *            rewritten on a per-file basis to avoid possible clashes
     * @param baseIRI
     *            the base IRI to be used for resolving relative IRIs, possibly null
     * @param config
     *            the optional {@code ParserConfig} for the fine tuning of the used RDF parser; if
     *            null a default, maximally permissive configuration will be used
     * @param errorWriterSupplier
     *            an optional function that maps a file location to a {@link Writer} where to emit
     *            malformed lines detected when reading that file
     * @param errorLogged
     *            false if errors in input files should be silently ignored
     * @param locations
     *            the locations of the RDF files to be read
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor read(final boolean parallelize, final boolean preserveBNodes,
            @Nullable final String baseIRI, @Nullable final ParserConfig config,
            @Nullable final Function<String, Writer> errorWriterSupplier,
            final boolean errorLogged, final String... locations) {
        final RDFProcessor tracker = RDFProcessors
                .track(new Tracker(RDFProcessors.LOGGER, null, "%d triples read (%d tr/s avg)", //
                        "%d triples read (%d tr/s, %d tr/s avg)"));
        final RDFSource source = RDFSources.read(parallelize, preserveBNodes, baseIRI, config,
                errorWriterSupplier, errorLogged, locations);
        return RDFProcessors.inject(tracker.wrap(source));
    }

    /**
     * Creates an {@code RDFProcessor} that retrieves data from a SPARQL endpoint and inject it in
     * the RDF stream at each pass. This is a utility method that relies on
     * {@link #inject(RDFSource)}, on {@link RDFSources#query(boolean, boolean, String, String)}
     * and on {@link #track(Tracker)} for providing progress information on fetched statements.
     * NOTE: as SPARQL does not provide any guarantee on the identifiers of returned BNodes, it
     * may happen that different BNodes are returned in different passes, causing the RDF stream
     * produced by this {@code RDFProcessor} to change from one pass to another.
     *
     * @param parallelize
     *            true if query results should be handled by multiple threads in parallel
     * @param endpointURL
     *            the URL of the SPARQL endpoint, not null
     * @param query
     *            the SPARQL query (CONSTRUCT or SELECT form) to submit to the endpoint
     * @param preserveBNodes
     *            true if BNodes in the query result should be preserved, false if they should be
     *            rewritten on a per-endpoint basis to avoid possible clashes
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor download(final boolean parallelize, final boolean preserveBNodes,
            final String endpointURL, final String query) {
        final RDFProcessor tracker = RDFProcessors
                .track(new Tracker(RDFProcessors.LOGGER, null, "%d triples queried (%d tr/s avg)", //
                        "%d triples queried (%d tr/s, %d tr/s avg)"));
        final RDFSource source = RDFSources.query(parallelize, preserveBNodes, endpointURL, query);
        return RDFProcessors.inject(tracker.wrap(source));
    }

    /**
     * Creates an {@code RDFProcessor} that duplicates data of the RDF stream to the
     * {@code RDFHandlers} specified. The produced processor can be used to 'peek' into the RDF
     * stream, possibly allowing to fork the stream. Note that RDF data is emitted to the supplied
     * handlers at each pass; if this is not the desired behavior, please wrap the handlers using
     * {@link RDFHandlers#ignorePasses(RDFHandler, int)}.
     *
     * @param handlers
     *            the handlers to duplicate RDF data to
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor tee(final RDFHandler... handlers) {
        if (handlers.length == 0) {
            return RDFProcessors.IDENTITY;
        }
        return new RDFProcessor() {

            @Override
            public RDFHandler wrap(final RDFHandler handler) {
                final RDFHandler[] allHandlers = new RDFHandler[handlers.length + 1];
                allHandlers[0] = Objects.requireNonNull(handler);
                System.arraycopy(handlers, 0, allHandlers, 1, handlers.length);
                return RDFHandlers.dispatchAll(allHandlers);
            }

        };
    }

    public static RDFProcessor tsv(@Nullable final Mapper mapper, final String query,
            final String location) {
        try {
            return new ProcessorTSV(Paths.get(location),
                    Algebra.parseTupleExpr(query, null, Namespaces.DEFAULT.uriMap()), mapper);
        } catch (final MalformedQueryException ex) {
            throw new IllegalArgumentException(
                    "Invalid query - " + ex.getMessage() + "\n" + query);
        }
    }

    /**
     * Creates an {@code RDFProcessor} that writes data of the RDF stream to the files specified.
     * This is a utility method that relies on {@link #tee(RDFHandler...)}, on
     * {@link RDFHandlers#write(WriterConfig, int, String...)} and on {@link #track(Tracker)} for
     * reporting progress information about written statements. Note that data is written only at
     * the first pass.
     *
     * @param config
     *            the optional {@code WriterConfig} for fine tuning the writing process; if null,
     *            a default configuration enabling pretty printing will be used
     * @param chunkSize
     *            the number of consecutive statements to be written as a single chunk to a single
     *            location (increase it to preserve locality)
     * @param locations
     *            the locations of the files to write
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor write(@Nullable final WriterConfig config, final int chunkSize,
            final String... locations) {
        if (locations.length == 0) {
            return RDFProcessors.IDENTITY;
        }
        final RDFHandler handler = RDFHandlers.write(config, chunkSize, locations);
        final RDFProcessor tracker = RDFProcessors.track(new Tracker(RDFProcessors.LOGGER, null, //
                "%d triples written (%d tr/s avg)", //
                "%d triples written (%d tr/s, %d tr/s avg)"));
        return RDFProcessors.tee(RDFHandlers.ignorePasses(tracker.wrap(handler), 1));
    }

    /**
     * Creates an {@code RDFProcessor} that uploads data of the RDF stream to the SPARQL endpoint
     * specified, using SPARQL Update INSERT DATA calls. This is a utility method that relies on
     * {@link #tee(RDFHandler...)}, on {@link RDFHandlers#update(String)} and on
     * {@link #track(Tracker)} for reporting progress information about uploaded statements. Note
     * that data is uploaded only at the first pass.
     *
     * @param endpointURL
     *            the URL of the SPARQL Update endpoint, not null
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor upload(final String endpointURL) {
        final RDFProcessor tracker = RDFProcessors.track(new Tracker(RDFProcessors.LOGGER, null, //
                "%d triples uploaded (%d tr/s avg)", //
                "%d triples uploaded (%d tr/s, %d tr/s avg)"));
        final RDFHandler handler = tracker.wrap(RDFHandlers.update(endpointURL));
        return RDFProcessors.tee(handler);
    }

    /**
     * Returns an {@code RDFProcessor} that tracks the number of statements flowing through it
     * using the supplied {@code Tracker} object.
     *
     * @param tracker
     *            the tracker object
     * @return an {@code RDFProcessor} that tracks the number of RDF statements passing through it
     */
    public static RDFProcessor track(final Tracker tracker) {

        Objects.requireNonNull(tracker);

        return new RDFProcessor() {

            @Override
            public RDFHandler wrap(final RDFHandler handler) {
                return new AbstractRDFHandlerWrapper(Objects.requireNonNull(handler)) {

                    @Override
                    public void startRDF() throws RDFHandlerException {
                        tracker.start();
                        super.startRDF();
                    }

                    @Override
                    public void handleStatement(final Statement statement)
                            throws RDFHandlerException {
                        super.handleStatement(statement);
                        tracker.increment();
                    }

                    @Override
                    public void endRDF() throws RDFHandlerException {
                        try {
                            super.endRDF();
                        } finally {
                            tracker.end();
                        }
                    }
                };
            }

        };
    }

    /**
     * Returns an {@code RDFProcessor} that applies the ruleset specified on input statements
     * either as a whole or partitioned based on an optional {@code Mapper}.
     *
     * @param ruleset
     *            the ruleset to apply
     * @param mapper
     *            the optional mapper for partitioning input statements, possibly null
     * @param dropBNodeTypes
     *            true to drop output {@code rdf:type} statements with a {@link BNode} object
     * @param deduplicate
     *            true to enforce that output statements do not contain duplicates (if false,
     *            duplicates might be returned if this enables the rule engine to operate faster)
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor rules(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, final boolean deduplicate) {
        return new ProcessorRules(ruleset, mapper, dropBNodeTypes, deduplicate, null, false, null);
    }

    /**
     * Returns an {@code RDFProcessor} that expands the ruleset based on the supplied TBox and
     * applies the resulting ruleset on input statements either as a whole or partitioned based on
     * an optional {@code Mapper}.
     *
     * @param ruleset
     *            the ruleset to apply
     * @param mapper
     *            the optional mapper for partitioning input statements, possibly null
     * @param dropBNodeTypes
     *            true to drop output {@code rdf:type} statements with a {@link BNode} object
     * @param deduplicate
     *            true to enforce that output statements do not contain duplicates (if false,
     *            duplicates might be returned if this enables the rule engine to operate faster)
     * @param tboxData
     *            the {@code RDFSource} of TBox data; null to disable TBox expansion
     * @param emitTBox
     *            true to emit TBox data (closed based on rules in the supplied {@code Ruleset})
     * @param tboxContext
     *            the context where to emit closed TBox data; null to emit TBox statements with
     *            their original contexts (use {@link SESAME#NIL} for emitting TBox data in the
     *            default context)
     * @return the created {@code RDFProcessor}
     */
    public static RDFProcessor rules(final Ruleset ruleset, @Nullable final Mapper mapper,
            final boolean dropBNodeTypes, final boolean deduplicate,
            @Nullable final RDFSource tboxData, final boolean emitTBox,
            @Nullable final IRI tboxContext) {
        return new ProcessorRules(ruleset, mapper, dropBNodeTypes, deduplicate, tboxData, emitTBox,
                tboxContext);
    }

    public static RDFProcessor kvread(final File file, final Mapper mapper,
            @Nullable final Predicate<Value> recurseMatcher) {

        Objects.requireNonNull(file);
        Objects.requireNonNull(mapper);

        return new RDFProcessor() {

            @Override
            public RDFHandler wrap(final RDFHandler handler) {

                final Value[] cache = new Value[32 * 1024];

                final Object[] locks = new Object[32];
                for (int i = 0; i < locks.length; ++i) {
                    locks[i] = new Object();
                }

                return new AbstractRDFHandlerWrapper(handler) {

                    private final KeyQuadIndex index = new KeyQuadIndex(file);

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        super.handleStatement(stmt);
                        for (final Value key : mapper.map(stmt)) {
                            enrich(key);
                        }
                    }

                    private void enrich(final Value key) {
                        final int hash = Math.abs(key.hashCode());
                        final int cacheIndex = hash % cache.length;
                        final int lockIndex = hash % locks.length;
                        synchronized (locks[lockIndex]) {
                            if (key.equals(cache[cacheIndex])) {
                                return;
                            }
                            cache[cacheIndex] = key;
                        }
                        if (recurseMatcher == null) {
                            this.index.get(key, this.handler);
                        } else {
                            this.index.getRecursive(ImmutableList.of(key), recurseMatcher,
                                    this.handler);
                        }
                    }

                    @Override
                    public void close() {
                        this.index.close();
                    }

                };
            }

        };
    }

    public static RDFProcessor kvwrite(final File file, final Mapper mapper) {
        Objects.requireNonNull(file);
        Objects.requireNonNull(mapper);
        return new RDFProcessor() {

            @Override
            public RDFHandler wrap(final RDFHandler handler) {
                return RDFHandlers.dispatchAll(handler, KeyQuadIndex.indexer(file, mapper));
            }

        };
    }

    private static class InjectSourceHandler extends AbstractRDFHandler {

        @Nullable
        private final RDFHandler handler;

        private final RDFSource source;

        @Nullable
        private final Tracker tracker;

        private RDFHandler sourceHandler;

        @Nullable
        private CountDownLatch latch;

        @Nullable
        private volatile Throwable exception;

        InjectSourceHandler(final RDFHandler handler, final RDFSource source,
                @Nullable final Tracker tracker) {
            this.handler = Objects.requireNonNull(handler);
            this.source = Objects.requireNonNull(source);
            this.tracker = tracker;
            this.sourceHandler = handler;
            this.latch = null;
            this.exception = null;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
            this.latch = new CountDownLatch(1);
            Environment.getPool().execute(new Runnable() {

                @Override
                public void run() {
                    InjectSourceHandler.this.inject();
                }

            });
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            checkNotFailed();
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            checkNotFailed();
            this.handler.handleNamespace(prefix, iri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            checkNotFailed();
            this.handler.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                if (this.latch != null) {
                    this.latch.await();
                }
            } catch (final InterruptedException ex) {
                this.exception = ex;
            }
            checkNotFailed();
            this.handler.endRDF();
        }

        @Override
        public void close() {
            IO.closeQuietly(this.handler);
            this.sourceHandler = null; // will ultimately stop the download process
        }

        private void inject() {
            try {
                InjectSourceHandler.this.source.emit(new AbstractRDFHandler() {

                    @Override
                    public void handleComment(final String comment) throws RDFHandlerException {
                        InjectSourceHandler.this.sourceHandler.handleComment(comment);
                    }

                    @Override
                    public void handleNamespace(final String prefix, final String iri)
                            throws RDFHandlerException {
                        InjectSourceHandler.this.sourceHandler.handleNamespace(prefix, iri);
                    }

                    @Override
                    public void handleStatement(final Statement statement)
                            throws RDFHandlerException {
                        InjectSourceHandler.this.sourceHandler.handleStatement(statement);
                        if (InjectSourceHandler.this.tracker != null) {
                            InjectSourceHandler.this.tracker.increment();
                        }
                    }

                }, 1);
            } catch (final Throwable ex) {
                if (this.sourceHandler != null) {
                    this.exception = ex;
                }
            } finally {
                this.latch.countDown();
            }
        }

        private void checkNotFailed() throws RDFHandlerException {
            if (this.exception != null) {
                if (this.exception instanceof RDFHandlerException) {
                    throw (RDFHandlerException) this.exception;
                } else if (this.exception instanceof RuntimeException) {
                    throw (RuntimeException) this.exception;
                } else if (this.exception instanceof Error) {
                    throw (Error) this.exception;
                }
                throw new RDFHandlerException(this.exception);
            }
        }

    }

    private static final class Parser {

        private static final int EOF = 0;

        private static final int COMMAND = 1;

        private static final int OPTION = 2;

        private static final int OPEN_BRACE = 3;

        private static final int COMMA = 4;

        private static final int CLOSE_BRACE = 5;

        private final List<String> tokens;

        private String token;

        private int type;

        private int pos;

        Parser(final List<String> tokens) {

            this.tokens = tokens;
            this.token = null;
            this.type = 0;
            this.pos = 0;

            next();
        }

        RDFProcessor parse() {
            final RDFProcessor processor = parseSequence();
            if (this.type != Parser.EOF) {
                syntaxError("<EOF>");
            }
            return processor;
        }

        private RDFProcessor parseSequence() {
            final List<RDFProcessor> processors = new ArrayList<RDFProcessor>();
            do {
                if (this.type == Parser.COMMAND) {
                    processors.add(parseCommand());
                } else if (this.type == Parser.OPEN_BRACE) {
                    processors.add(parseParallel());
                } else {
                    syntaxError("'@command' or '{'");
                }
            } while (this.type == Parser.COMMAND || this.type == Parser.OPEN_BRACE);
            return RDFProcessors.sequence(processors.toArray(new RDFProcessor[processors.size()]));
        }

        private RDFProcessor parseParallel() {
            final List<RDFProcessor> processors = new ArrayList<RDFProcessor>();
            do {
                next();
                processors.add(parseSequence());
            } while (this.type == Parser.COMMA);
            if (this.type != Parser.CLOSE_BRACE) {
                syntaxError("'}x'");
            }
            final String mod = this.token.length() == 1 ? "a" : this.token.substring(1);
            final SetOperator merging = SetOperator.valueOf(mod);
            next();
            return RDFProcessors.parallel(merging,
                    processors.toArray(new RDFProcessor[processors.size()]));
        }

        private RDFProcessor parseCommand() {
            final String command = this.token.substring(1).toLowerCase();
            final List<String> args = new ArrayList<String>();
            while (next() == Parser.OPTION) {
                args.add(this.token);
            }
            return Environment.newPlugin(RDFProcessor.class, command,
                    args.toArray(new String[args.size()]));
        }

        private void syntaxError(final String expected) {
            throw new IllegalArgumentException("Invalid specification. Expected " + expected
                    + ", found '" + this.token + "'");
        }

        private int next() {
            if (this.pos == this.tokens.size()) {
                this.token = "<EOF>";
                this.type = Parser.EOF;
            } else {
                this.token = this.tokens.get(this.pos++);
                final char ch = this.token.charAt(0);
                if (ch == '@') {
                    this.type = Parser.COMMAND;
                } else if (ch == '}') {
                    this.type = Parser.CLOSE_BRACE;
                } else if ("{".equals(this.token)) {
                    this.type = Parser.OPEN_BRACE;
                } else if (",".equals(this.token)) {
                    this.type = Parser.COMMA;
                } else {
                    this.type = Parser.OPTION;
                }
            }
            return this.type;
        }

    }

}
