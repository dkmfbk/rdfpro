/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti <francesco.corcoglioniti@gmail.com> with support by
 * Marco Rospocher, Marco Amadori and Michele Mostarda.
 * 
 * To the extent possible under law, the author has dedicated all copyright and related and
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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

// assumptions
// - lifecycle: start, followed by handleXXX in parallel, followed by end
// - on error, further invocation to API method will result in exceptions
// - no specific way to interrupt computation (CTRL-C from outside)

public abstract class RDFProcessor {

    public abstract int getExtraPasses();

    public final RDFHandler getHandler() {
        return getHandler(Handlers.nop());
    }

    public abstract RDFHandler getHandler(RDFHandler sink);

    public static RDFProcessor parallel(final Merging merging, final RDFProcessor... processors) {
        return new ParallelProcessor(merging, Util.checkNotNull(processors));
    }

    public static RDFProcessor sequence(final RDFProcessor... processors) {
        return new SequenceProcessor(Util.checkNotNull(processors));
    }

    // TODO: should add a filter taking a Guava predicate or equivalent for programmatic use

    public static RDFProcessor transform(@Nullable final String groovyExpressionOrFile,
            final String... groovyArgs) {
        return new GroovyFilterProcessor(groovyExpressionOrFile, groovyArgs);
    }

    public static RDFProcessor filter(@Nullable final String matchSpec,
            @Nullable final String replaceSpec, final boolean keep) {
        return new FilterProcessor(matchSpec, replaceSpec, keep);
    }

    // empty rule array = all rules

    public static RDFProcessor inferencer(final Iterable<? extends Statement> tbox,
            @Nullable final Resource tboxContext, final boolean decomposeOWLAxioms,
            final String... enabledRules) {
        return new InferencerProcessor(tbox, tboxContext, decomposeOWLAxioms,
                Util.checkNotNull(enabledRules));
    }

    public static RDFProcessor smusher(final long bufferSize, final String... rankedNamespaces) {
        return new SmusherProcessor(bufferSize, Util.checkNotNull(rankedNamespaces));
    }

    public static RDFProcessor statisticsExtractor(@Nullable final String outputNamespace,
            @Nullable final URI sourceProperty, @Nullable final URI sourceContext,
            final boolean processCooccurrences) {
        return new StatisticsProcessor(outputNamespace, sourceProperty, sourceContext,
                processCooccurrences);
    }

    public static RDFProcessor tboxExtractor() {
        return new TBoxProcessor();
    }

    // Note: supplied map should not be changed externally; make a defensive copy otherwise
    public static RDFProcessor namespaceEnhancer(@Nullable final Map<String, String> nsToPrefixMap) {
        return new NamespaceProcessor(nsToPrefixMap != null ? nsToPrefixMap
                : Util.NS_TO_PREFIX_MAP);
    }

    public static RDFProcessor reader(final boolean rewriteBNodes, @Nullable final String base,
            final String... fileSpecs) {
        return new ReaderProcessor(rewriteBNodes, base, Util.checkNotNull(fileSpecs));
    }

    public static RDFProcessor writer(final String... fileSpecs) {
        return new WriterProcessor(Util.checkNotNull(fileSpecs));
    }

    public static RDFProcessor unique(final boolean merge) {
        return new ParallelProcessor(merge ? Merging.UNION_TRIPLES : Merging.UNION_QUADS, nop());
    }

    public static RDFProcessor download(final boolean rewriteBNodes, final String endpointURL,
            final String query) {
        return new DownloadProcessor(rewriteBNodes, endpointURL, query);
    }

    public static RDFProcessor upload(final int chunkSize, final String endpointURL) {
        return new UploadProcessor(endpointURL, chunkSize);
    }

    public static RDFProcessor nop() {
        return NOPProcessor.INSTANCE;
    }

    public static RDFProcessor parse(final String... args) {
        return new Parser(Arrays.asList(args)).parse();
    }

    public static RDFProcessor parse(final String spec) {

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

        return new Parser(tokens).parse();
    }

    public enum Merging {

        UNION_ALL,

        UNION_QUADS,

        UNION_TRIPLES,

        INTERSECTION,

        DIFFERENCE

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
            if (this.type != EOF) {
                syntaxError("<EOF>");
            }
            return processor;
        }

        private RDFProcessor parseSequence() {
            final List<RDFProcessor> processors = new ArrayList<RDFProcessor>();
            do {
                if (this.type == COMMAND) {
                    processors.add(parseCommand());
                } else if (this.type == OPEN_BRACE) {
                    processors.add(parseParallel());
                } else {
                    syntaxError("'@command' or '{'");
                }
            } while (this.type == COMMAND || this.type == OPEN_BRACE);
            return sequence(processors.toArray(new RDFProcessor[processors.size()]));
        }

        private RDFProcessor parseParallel() {
            final List<RDFProcessor> processors = new ArrayList<RDFProcessor>();
            do {
                next();
                processors.add(parseSequence());
            } while (this.type == COMMA);
            if (this.type != CLOSE_BRACE) {
                syntaxError("'}x'");
            }
            final char mod = this.token.length() == 1 ? 'a' : this.token.charAt(1);
            final Merging merging = mergingFor(mod);
            next();
            return parallel(merging, processors.toArray(new RDFProcessor[processors.size()]));
        }

        private RDFProcessor parseCommand() {
            final String command = this.token.substring(1).toLowerCase();
            final List<String> options = new ArrayList<String>();
            while (next() == OPTION) {
                options.add(this.token);
            }
            if ("r".equals(command) || "read".equals(command)) {
                return newReader(parseOptions(options, "b", "w", Integer.MAX_VALUE));
            }
            if ("w".equals(command) || "write".equals(command)) {
                return newWriter(parseOptions(options, "", "", Integer.MAX_VALUE));
            }
            if ("d".equals(command) || "download".equals(command)) {
                return newDownload(parseOptions(options, "qf", "w", 1));
            }
            if ("l".equals(command) || "upload".equals(command)) {
                return newUpload(parseOptions(options, "s", "", 1));
            }
            if ("r".equals(command) || "transform".equals(command)) {
                return newTransform(options);
            }
            if ("f".equals(command) || "filter".equals(command)) {
                return newFilter(parseOptions(options, "r", "k", 1));
            }
            if ("s".equals(command) || "smush".equals(command)) {
                return newSmusher(parseOptions(options, "s", "", Integer.MAX_VALUE));
            }
            if ("i".equals(command) || "infer".equals(command)) {
                return newInferencer(parseOptions(options, "bcr", "Cdw", Integer.MAX_VALUE));
            }
            if ("t".equals(command) || "tbox".equals(command)) {
                return newTBoxExtractor(parseOptions(options, "", "", 0));
            }
            if ("x".equals(command) || "stats".equals(command)) {
                return newStatisticsExtractor(parseOptions(options, "npc", "o", 0));
            }
            if ("p".equals(command) || "prefix".equals(command)) {
                return newNamespaceEnhancer(parseOptions(options, "f", "", 0));
            }
            if ("u".equals(command) || "unique".equals(command)) {
                return newUnique(parseOptions(options, "", "m", 0));
            }
            throw new IllegalArgumentException("Invalid command @" + command);
        }

        private Map<String, Object> parseOptions(final List<? extends String> options,
                final String argOptions, final String noArgOptions, final int maxNonOptions) {

            final Map<String, Object> result = new HashMap<String, Object>();
            final List<String> args = new ArrayList<String>();
            int index = 0;
            final int length = options.size();
            while (index < length) {
                final String option = options.get(index++);
                if (option.startsWith("-") && option.length() == 2) {
                    final String name = option.substring(1);
                    if (noArgOptions.contains(name)) {
                        result.put(name, null);
                    } else if (argOptions.contains(name)) {
                        final String value = index < length ? options.get(index++) : null;
                        if (value == null || value.startsWith("-") && value.length() == 2) {
                            throw new IllegalArgumentException(
                                    "Missing required argument of option " + option);
                        }
                        result.put(name, value);
                    } else {
                        throw new IllegalArgumentException("Invalid option " + option);
                    }
                } else {
                    args.add(option);
                }
            }
            if (args.size() > maxNonOptions) {
                throw new IllegalArgumentException("Invalid number of arguments: expected (max) "
                        + maxNonOptions + ", got " + args.size());
            }
            result.put(null, args.toArray(new String[args.size()]));
            return result;
        }

        private String parseNamespace(final String string) {
            if (string.contains(":")) {
                return string;
            }
            final String namespace = Util.PREFIX_TO_NS_MAP.get(string);
            if (namespace == null) {
                throw new IllegalArgumentException("Invalid namespace: " + string);
            }
            return namespace;
        }

        private URI parseURI(final String string) {
            try {
                return (URI) Util.parseValue(string);
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Invalid URI '" + string + "': "
                        + ex.getMessage(), ex);
            }
        }

        private long parseLong(final String string) {
            long multiplier = 1;
            if (string.endsWith("k") || string.endsWith("K")) {
                multiplier = 1024;
            } else if (string.endsWith("m") || string.endsWith("M")) {
                multiplier = 1024 * 1024;
            } else if (string.endsWith("g") || string.endsWith("G")) {
                multiplier = 1024 * 1024 * 1024;
            }
            return Long.parseLong(multiplier == 1 ? string : string.substring(0,
                    string.length() - 1)) * multiplier;
        }

        private RDFProcessor newReader(final Map<String, Object> args) {
            final String[] fileSpecs = (String[]) args.get(null);
            final String base = (String) args.get("b");
            final boolean rewriteBNodes = args.containsKey("w");
            return reader(rewriteBNodes, base, fileSpecs);
        }

        private RDFProcessor newWriter(final Map<String, Object> args) {
            final String[] fileSpecs = (String[]) args.get(null);
            return writer(fileSpecs);
        }

        private RDFProcessor newDownload(final Map<String, Object> args) {
            final boolean rewriteBNodes = args.containsKey("w");
            final String endpointURL = ((String[]) args.get(null))[0];
            String query = (String) args.get("q");
            if (query == null) {
                final String source = (String) args.get("f");
                try {
                    final File file = new File(source);
                    URL url;
                    if (file.exists()) {
                        url = file.toURI().toURL();
                    } else {
                        url = getClass().getClassLoader().getResource(source);
                    }
                    final BufferedReader reader = new BufferedReader(new InputStreamReader(
                            url.openStream()));
                    try {
                        final StringBuilder builder = new StringBuilder();
                        String line;
                        while ((line = reader.readLine()) != null) {
                            builder.append(line);
                        }
                        query = builder.toString();
                    } finally {
                        Util.closeQuietly(reader);
                    }
                } catch (final Throwable ex) {
                    throw new IllegalArgumentException("Cannot load SPARQL query from " + source
                            + ": " + ex.getMessage(), ex);
                }
            }
            return download(rewriteBNodes, endpointURL, query);
        }

        private RDFProcessor newUpload(final Map<String, Object> args) {
            int chunkSize = 1024;
            if (args.containsKey("s")) {
                try {
                    chunkSize = (int) parseLong((String) args.get("s"));
                } catch (final Throwable ex) {
                    throw new IllegalArgumentException("Invalid buffer size: " + args.get("s"));
                }
            }
            final String endpointURL = ((String[]) args.get(null))[0];
            return upload(chunkSize, endpointURL);
        }

        private RDFProcessor newFilter(final Map<String, Object> args) {

            final String[] specs = (String[]) args.get(null);
            final String matchSpec = specs.length == 0 ? null : Util.join(" ",
                    Arrays.asList(specs));

            final String replaceSpec = (String) args.get("r");

            final boolean keep = args.containsKey("k");

            return filter(matchSpec, replaceSpec, keep);
        }

        private RDFProcessor newTransform(final List<String> args) {

            if (args.isEmpty()) {
                throw new IllegalArgumentException(
                        "Missing filter script expression or file reference");
            }

            final String groovyExpressionOrFile = args.get(0);
            final String[] groovyArgs = args.subList(1, args.size()).toArray(
                    new String[args.size() - 1]);

            return transform(groovyExpressionOrFile, groovyArgs);
        }

        private RDFProcessor newSmusher(final Map<String, Object> args) {

            final String[] namespaces = (String[]) args.get(null);
            for (int i = 0; i < namespaces.length; ++i) {
                namespaces[i] = parseNamespace(namespaces[i]);
            }

            Long bufferSize = null;
            if (args.containsKey("s")) {
                try {
                    bufferSize = parseLong((String) args.get("s"));
                } catch (final Throwable ex) {
                    throw new IllegalArgumentException("Invalid buffer size: " + args.get("S"));
                }
            }

            return smusher(bufferSize, namespaces);
        }

        private RDFProcessor newInferencer(final Map<String, Object> args) {

            final List<Statement> tbox = new ArrayList<Statement>();
            try {
                final String base = (String) args.get("b");
                final boolean rewriteBNodes = args.containsKey("w");
                final String[] fileSpecs = (String[]) args.get(null);
                final RDFHandler handler = reader(rewriteBNodes, base, fileSpecs).getHandler(
                        Handlers.collect(tbox, true));
                handler.startRDF();
                handler.endRDF();
            } catch (final RDFHandlerException ex) {
                throw new IllegalArgumentException("Cannot load TBox data: " + ex.getMessage(), ex);
            }

            final boolean decomposeOWLAxioms = args.containsKey("d");

            String[] rules = new String[0];
            if (args.containsKey("r")) {
                rules = ((String) args.get("r")).split(",");
            }

            URI context = null;
            if (args.containsKey("C")) {
                context = SESAME.NIL;
            } else if (args.containsKey("c")) {
                context = parseURI((String) args.get("c"));
            }

            return inferencer(tbox, context, decomposeOWLAxioms, rules);
        }

        private RDFProcessor newStatisticsExtractor(final Map<String, Object> args) {

            String namespace = null;
            if (args.containsKey("n")) {
                namespace = parseNamespace((String) args.get("n"));
            }

            URI property = null;
            if (args.containsKey("p")) {
                property = parseURI((String) args.get("p"));
            }

            URI context = null;
            if (args.containsKey("c")) {
                context = parseURI((String) args.get("c"));
            }

            final boolean processCooccurrences = args.containsKey("o");

            return statisticsExtractor(namespace, property, context, processCooccurrences);
        }

        private RDFProcessor newTBoxExtractor(final Map<String, Object> args) {
            return tboxExtractor();
        }

        private RDFProcessor newNamespaceEnhancer(final Map<String, Object> args) {

            Map<String, String> nsToPrefixMap = null;
            final String source = (String) args.get("f");
            if (source != null) {
                try {
                    nsToPrefixMap = new HashMap<String, String>();
                    URL url;
                    final File file = new File(source);
                    if (file.exists()) {
                        url = file.toURI().toURL();
                    } else {
                        url = getClass().getClassLoader().getResource(source);
                    }
                    Util.parseNamespaces(url, nsToPrefixMap, null);
                } catch (final Throwable ex) {
                    throw new IllegalArgumentException(
                            "Cannot load prefix/namespace bindings from " + source + ": "
                                    + ex.getMessage(), ex);
                }
            }

            return namespaceEnhancer(nsToPrefixMap);
        }

        private RDFProcessor newUnique(final Map<String, Object> args) {
            final boolean merge = args.containsKey("m");
            return unique(merge);
        }

        private Merging mergingFor(final char ch) {
            switch (ch) {
            case 'a':
                return Merging.UNION_ALL;
            case 'u':
                return Merging.UNION_QUADS;
            case 'U':
                return Merging.UNION_TRIPLES;
            case 'i':
                return Merging.INTERSECTION;
            case 'd':
                return Merging.DIFFERENCE;
            }
            throw new IllegalArgumentException("Unknown merging strategy: " + ch);
        }

        private void syntaxError(final String expected) {
            throw new IllegalArgumentException("Invalid specification. Expected " + expected
                    + ", found '" + this.token + "'");
        }

        private int next() {
            if (this.pos == this.tokens.size()) {
                this.token = "<EOF>";
                this.type = EOF;
            } else {
                this.token = this.tokens.get(this.pos++);
                final char ch = this.token.charAt(0);
                if (ch == '@') {
                    this.type = COMMAND;
                } else if (ch == '}' && this.token.length() <= 2) {
                    this.type = CLOSE_BRACE;
                } else if ("{".equals(this.token)) {
                    this.type = OPEN_BRACE;
                } else if (",".equals(this.token)) {
                    this.type = COMMA;
                } else {
                    this.type = OPTION;
                }
            }
            return this.type;
        }

    }

}
