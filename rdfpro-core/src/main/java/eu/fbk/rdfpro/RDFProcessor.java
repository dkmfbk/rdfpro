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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Util;

// assumptions
// - lifecycle: start, followed by handleXXX in parallel, followed by end
// - on error, further invocation to API method will result in exceptions
// - no specific way to interrupt computation (CTRL-C from outside)

public abstract class RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFProcessor.class);

    private static final String CREATE_METHOD_NAME = "doCreate";

    private static final Map<String, Method> PROCESSORS_FACTORIES;

    private static final String PROCESSORS_HELP;

    static {
        // PROCESSORS_CLASSES = new HashMap<String, Class<? extends RDFProcessor>>();
        PROCESSORS_FACTORIES = new HashMap<String, Method>();

        final Map<String, String> helpTexts = new HashMap<String, String>();
        try {
            final Enumeration<URL> e = RDFProcessor.class.getClassLoader().getResources(
                    "META-INF/rdfpro.properties");
            while (e.hasMoreElements()) {
                final URL url = e.nextElement();
                final Properties properties = new Properties();
                final Reader in = new InputStreamReader(url.openStream(), Charset.forName("UTF-8"));
                try {
                    properties.load(in);
                    for (final Object key : properties.keySet()) {
                        try {
                            final String className = key.toString();
                            final Class<? extends RDFProcessor> clazz = Class.forName(className)
                                    .asSubclass(RDFProcessor.class);
                            final Method method = clazz.getDeclaredMethod(CREATE_METHOD_NAME,
                                    String[].class);
                            if (!RDFProcessor.class.isAssignableFrom(method.getReturnType())) {
                                throw new Error("Invalid return type for " + method
                                        + " - should be RDFProcessor");
                            }
                            method.setAccessible(true);
                            final String value = properties.getProperty(className);
                            int index = value.indexOf(' ');
                            index = index > 0 ? index : value.length();
                            final String[] tokens = value.substring(0, index).split(",");
                            for (int i = 0; i < tokens.length; ++i) {
                                PROCESSORS_FACTORIES.put(tokens[i].trim(), method);
                            }
                            helpTexts.put(clazz.getName(), value.substring(index).trim());
                        } catch (final Throwable ex) {
                            LOGGER.warn("Invalid metadata entry '" + key + "' in file '" + url
                                    + "' - ignoring", ex);
                        }
                    }
                } catch (final Throwable ex) {
                    LOGGER.warn("Could not load metadata file '" + url + "' - ignoring", ex);
                } finally {
                    in.close();
                }
            }
        } catch (final IOException ex) {
            LOGGER.warn("Could not complete loading of metadata from classpath resources", ex);
        }

        final List<String> sortedClassNames = new ArrayList<String>(helpTexts.keySet());
        Collections.sort(sortedClassNames);
        final StringBuilder helpBuilder = new StringBuilder();
        for (final String className : sortedClassNames) {
            helpBuilder.append(helpBuilder.length() == 0 ? "" : "\n\n");
            helpBuilder.append(helpTexts.get(className));
        }

        PROCESSORS_HELP = helpBuilder.toString();
    }

    @Nullable
    private static String readVersion(final String groupId, final String artifactId,
            @Nullable final String defaultValue) {

        Util.checkNotNull(groupId);
        Util.checkNotNull(artifactId);

        final URL url = RDFProcessor.class.getClassLoader().getResource(
                "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties");

        if (url != null) {
            try {
                final InputStream stream = url.openStream();
                try {
                    final Properties properties = new Properties();
                    properties.load(stream);
                    return properties.getProperty("version").trim();
                } finally {
                    stream.close();
                }

            } catch (final IOException ex) {
                LOGGER.warn("Could not parse version string in " + url);
            }
        }

        return defaultValue;
    }

    private static String readResource(final URL url) {
        Util.checkNotNull(url);
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(
                    url.openStream(), Charset.forName("UTF-8")));
            final StringBuilder builder = new StringBuilder();
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    builder.append(line).append("\n");
                }
            } finally {
                reader.close();
            }
            return builder.toString();
        } catch (final Throwable ex) {
            throw new Error("Could not load resource " + url);
        }
    }

    public static void main(final String... args) {

        try {
            Class.forName("eu.fbk.rdfpro.tql.TQL");
        } catch (final Throwable ex) {
            // ignore - TQL will not be supported
        }

        try {
            Class.forName("eu.fbk.rdfpro.tool.GeonamesRDF");
        } catch (final Throwable ex) {
            // ignore - Geonames format will not be supported
        }

        boolean showHelp = false;
        boolean showVersion = false;
        boolean verbose = false;

        int index = 0;
        while (index < args.length) {
            final String arg = args[index];
            if (arg.startsWith("{") || arg.startsWith("@") || !arg.startsWith("-")) {
                break;
            }
            showHelp |= arg.equals("-h");
            showVersion |= arg.equals("-v");
            verbose |= arg.equals("-V");
            ++index;
        }
        showHelp |= index == args.length;

        if (verbose) {
            try {
                final Logger root = LoggerFactory.getLogger("eu.fbk.rdfpro");
                final Class<?> levelClass = Class.forName("ch.qos.logback.classic.Level");
                final Class<?> loggerClass = Class.forName("ch.qos.logback.classic.Logger");
                final Object level = levelClass.getDeclaredMethod("valueOf", String.class).invoke(
                        null, "DEBUG");
                loggerClass.getDeclaredMethod("setLevel", levelClass).invoke(root, level);
            } catch (final Throwable ex) {
                // ignore - no control on logging level
            }
        }

        if (showVersion) {
            System.out.println(String.format("RDF Processor Tool (RDFP) %s\nJava %s bit (%s) %s\n"
                    + "This is free software released into the public domain",
                    readVersion("eu.fbk.rdfpro", "rdfpro-core", "unknown version"),
                    System.getProperty("sun.arch.data.model"), System.getProperty("java.vendor"),
                    System.getProperty("java.version")));
            System.exit(0);
        }

        if (showHelp) {
            System.out.println(String.format(
                    readResource(RDFProcessor.class.getResource("RDFProcessor.help")),
                    readVersion("eu.fbk.rdfpro", "rdfpro-core", "unknown version"),
                    PROCESSORS_HELP));
            System.exit(0);
        }

        RDFProcessor processor = null;
        try {
            processor = RDFProcessor.parse(Arrays.copyOfRange(args, index, args.length));
        } catch (final IllegalArgumentException ex) {
            System.err.println("INVOCATION ERROR. " + ex.getMessage() + "\n");
            System.exit(1);
        }

        try {
            final long ts = System.currentTimeMillis();
            final RDFHandler handler = processor.getHandler(Handlers.nop());
            final int repetitions = processor.getExtraPasses() + 1;
            for (int i = 0; i < repetitions; ++i) {
                if (repetitions > 1) {
                    LOGGER.info("Pass {} of {}", i + 1, repetitions);
                }
                handler.startRDF();
                handler.endRDF();
            }
            LOGGER.info("Done in {} s", (System.currentTimeMillis() - ts) / 1000);
            System.exit(0);

        } catch (final Throwable ex) {
            System.err.println("EXECUTION FAILED. " + ex.getMessage() + "\n");
            ex.printStackTrace();
            System.exit(2);
        }
    }

    public static RDFProcessor parse(final String... args) {
        return new Parser(Arrays.asList(args)).parse();
    }

    public static RDFProcessor create(final String name, final String... args) {
        final Method factory = PROCESSORS_FACTORIES.get(name);
        if (factory == null) {
            Util.checkNotNull(name);
            throw new IllegalArgumentException("Unknown processor name '" + name + "'");
        }
        try {
            return (RDFProcessor) factory.invoke(null, (Object) args);
        } catch (final IllegalAccessException ex) {
            throw new Error("Unexpected error (!)", ex); // already checked when loading metadata
        } catch (final InvocationTargetException ex) {
            final Throwable cause = ex.getCause();
            throw cause instanceof RuntimeException ? (RuntimeException) cause
                    : new RuntimeException(ex);
        }
    }

    public static RDFProcessor parallel(final SetOperation merging,
            final RDFProcessor... processors) {
        Util.checkNotNull(merging);
        Util.checkNotNull(processors);
        if (processors.length >= 1) {
            return new ParallelProcessor(merging, processors);
        }
        throw new IllegalArgumentException("At least one processor should be supplied "
                + "in a parallel composition");
    }

    public static RDFProcessor sequence(final RDFProcessor... processors) {
        Util.checkNotNull(processors);
        if (processors.length == 1) {
            return processors[0];
        } else if (processors.length > 1) {
            return new SequenceProcessor(processors);
        }
        throw new IllegalArgumentException("At least one processor should be supplied "
                + "in a sequence composition");
    }

    public static RDFProcessor nop() {
        return NOPProcessor.INSTANCE;
    }

    public int getExtraPasses() {
        return 0; // may be overridden
    }

    public abstract RDFHandler getHandler(@Nullable RDFHandler handler);

    public void process(@Nullable final Iterable<Statement> input,
            @Nullable final RDFHandler output) throws RDFHandlerException {
        final RDFHandler handler = getHandler(output != null ? output : Handlers.nop());
        try {
            final int passes = 1 + getExtraPasses();
            for (int i = 0; i < passes; ++i) {
                handler.startRDF();
                if (input != null) {
                    for (final Statement statement : input) {
                        handler.handleStatement(statement);
                    }
                }
                handler.endRDF();
            }
        } finally {
            Util.closeQuietly(handler);
        }
    }

    public final CompletableFuture<Void> processAsync(@Nullable final Iterable<Statement> input,
            @Nullable final RDFHandler output) {
        final CompletableFuture<Void> future = new CompletableFuture<Void>();
        Util.getPool().execute(new Runnable() {

            @Override
            public void run() {
                if (!future.isDone()) {
                    try {
                        process(input, output);
                        future.complete(null);
                    } catch (final Throwable ex) {
                        future.completeExceptionally(ex);
                    }
                }
            }

        });
        return future;
    }

    public enum SetOperation {

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
            final SetOperation merging = mergingFor(mod);
            next();
            return parallel(merging, processors.toArray(new RDFProcessor[processors.size()]));
        }

        private RDFProcessor parseCommand() {
            final String command = this.token.substring(1).toLowerCase();
            final List<String> args = new ArrayList<String>();
            while (next() == OPTION) {
                args.add(this.token);
            }
            return RDFProcessor.create(command, args.toArray(new String[args.size()]));
        }

        private SetOperation mergingFor(final char ch) {
            switch (ch) {
            case 'a':
                return SetOperation.UNION_ALL;
            case 'u':
                return SetOperation.UNION_QUADS;
            case 'U':
                return SetOperation.UNION_TRIPLES;
            case 'i':
                return SetOperation.INTERSECTION;
            case 'd':
                return SetOperation.DIFFERENCE;
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
