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
package eu.fbk.rdfpro.base;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.RDFJSONParserSettings;
import org.openrdf.rio.helpers.TriXParserSettings;
import org.openrdf.rio.helpers.XMLParserSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Streams;
import eu.fbk.rdfpro.util.Tracker;
import eu.fbk.rdfpro.util.Util;

public final class ReadProcessor extends RDFProcessor {

    public static final boolean ENABLE_PARALLEL_READ = Boolean.parseBoolean(System.getProperty(
            "rdfpro.parallel.read", "true"));

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadProcessor.class);

    private final String[] fileSpecs;

    private final boolean rewriteBNodes;

    @Nullable
    private final String base;

    static ReadProcessor doCreate(final String... args) {
        final Options options = Options.parse("b|w|+", args);
        final String[] fileSpecs = options.getPositionalArgs(String.class).toArray(new String[0]);
        final boolean rewriteBNodes = options.hasOption("w");
        final String base = options.getOptionArg("b", String.class);
        return new ReadProcessor(rewriteBNodes, base, fileSpecs);
    }

    public ReadProcessor(final boolean rewriteBNodes, @Nullable final String base,
            final String... fileSpecs) {
        this.fileSpecs = fileSpecs.clone();
        this.rewriteBNodes = rewriteBNodes;
        this.base = base;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {

        // Check that all filenames denote existing files for which an RDF format is usable
        final StringBuilder builder = new StringBuilder();
        for (final String fileSpec : this.fileSpecs) {
            final File file = Statements.toRDFFile(fileSpec);
            final RDFFormat format = Statements.toRDFFormat(fileSpec);
            if (file != null && !file.exists()) {
                builder.append("- " + file + " does not exist");
            } else if (file != null && file.isDirectory()) {
                builder.append("- " + file + " is a directory");
            } else if (format == null) {
                builder.append("- " + file + " has unknown RDF format");
            }
        }
        if (builder.length() > 0) {
            throw new IllegalArgumentException("Invalid file list:\n" + builder.toString());
        }

        // Sort the files based on decreasing size to improve throughput
        final String[] fileSpecs = this.fileSpecs.clone();
        Arrays.sort(fileSpecs, new Comparator<String>() {

            @Override
            public int compare(final String first, final String second) {
                final File firstFile = Statements.toRDFFile(first);
                final File secondFile = Statements.toRDFFile(second);
                return firstFile == null ? -1 : secondFile == null ? 1 //
                        : (int) (secondFile.length() - firstFile.length());
            }

        });

        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        return new Handler(Handlers.decouple(sink), this.rewriteBNodes, this.base, fileSpecs);
    }

    @Override
    public String toString() {
        String string = "read " + String.join(", ", this.fileSpecs);
        if (string.length() > 40) {
            string = string.substring(0, 37) + "...";
        }
        return string;
    }

    private static RDFParser newRDFParser(final RDFFormat format) {

        final RDFParser parser = Rio.createParser(format);
        parser.setValueFactory(Statements.VALUE_FACTORY);
        final ParserConfig config = parser.getParserConfig();

        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
        config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
        config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
        config.set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, true);
        config.set(BasicParserSettings.NORMALIZE_LANGUAGE_TAGS, true);
        config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);

        if (format.equals(RDFFormat.NTRIPLES)) {
            config.set(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES, false);

        } else if (format.equals(RDFFormat.RDFJSON)) {
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_DATATYPES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_LANGUAGES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_TYPES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_VALUES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_UNKNOWN_PROPERTY, false);
            config.set(RDFJSONParserSettings.SUPPORT_GRAPHS_EXTENSION, true);

        } else if (format.equals(RDFFormat.TRIX)) {
            config.set(TriXParserSettings.FAIL_ON_TRIX_INVALID_STATEMENT, false);
            config.set(TriXParserSettings.FAIL_ON_TRIX_MISSING_DATATYPE, false);

        } else if (format.equals(RDFFormat.RDFXML)) {
            config.set(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID, false);
            config.set(XMLParserSettings.FAIL_ON_INVALID_NCNAME, false);
            config.set(XMLParserSettings.FAIL_ON_INVALID_QNAME, false);
            config.set(XMLParserSettings.FAIL_ON_MISMATCHED_TAGS, false);
            config.set(XMLParserSettings.FAIL_ON_NON_STANDARD_ATTRIBUTES, false);
            config.set(XMLParserSettings.FAIL_ON_SAX_NON_FATAL_ERRORS, false);
        }

        return parser;
    }

    private static final class Handler extends HandlerWrapper {

        private final String[] fileSpecs;

        private final boolean rewriteBNodes;

        private final String base;

        private final int parallelism;

        private final RDFHandler logHandler;

        private final RDFHandler parseHandler;

        private List<String> pendingFileSpecs;

        private List<Closeable> pendingStreams;

        private List<RDFHandler> pendingHandlers;

        private Map<Closeable, RDFFormat> openStreams;

        @Nullable
        private CountDownLatch latch;

        @Nullable
        private volatile Throwable exception;

        public Handler(final RDFHandler handler, final boolean rewriteBNodes,
                @Nullable final String base, final String... fileSpecs) {

            super(handler);

            int parallelism = 0;
            for (final String fileSpec : fileSpecs) {
                final RDFFormat format = Statements.toRDFFormat(fileSpec);
                if (Statements.isRDFFormatLineBased(format)) {
                    parallelism = Util.CORES;
                } else {
                    ++parallelism;
                }
                if (parallelism == Util.CORES) {
                    break;
                }
            }

            this.fileSpecs = fileSpecs;
            this.rewriteBNodes = rewriteBNodes;
            this.base = base;
            this.parallelism = parallelism;
            this.logHandler = Handlers.track(handler, new Tracker(LOGGER, null, //
                    "%d triples parsed (%d tr/s avg)", //
                    "%d triples parsed (%d tr/s, %d tr/s avg)"));
            this.parseHandler = Handlers.ignoreStartEnd(this.logHandler);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.logHandler.startRDF();
            if (this.parallelism > 0) {
                this.pendingFileSpecs = new ArrayList<String>(Arrays.asList(this.fileSpecs));
                this.pendingStreams = new ArrayList<Closeable>();
                this.pendingHandlers = new ArrayList<RDFHandler>();
                this.openStreams = new IdentityHashMap<Closeable, RDFFormat>();
                this.exception = null;
                this.latch = new CountDownLatch(this.parallelism);
                for (int i = 0; i < this.parallelism; ++i) {
                    Util.getPool().execute(new Runnable() {

                        @Override
                        public void run() {
                            final String threadName = Thread.currentThread().getName();
                            LOGGER.debug("Begin parsing in thread {}", threadName);
                            parseLoop();
                            LOGGER.debug("Done parsing in thread {}", threadName);
                        }

                    });
                }
            }
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
            this.logHandler.endRDF();
        }

        @Override
        public void close() {
            parseHalt();
            super.close();
        }

        private void checkNotFailed() throws RDFHandlerException {
            if (this.exception != null) {
                Util.propagateIfPossible(this.exception, RDFHandlerException.class);
                throw new RDFHandlerException(this.exception);
            }
        }

        private void parseHalt() {
            synchronized (this.pendingStreams) {
                this.pendingStreams.clear();
                this.pendingHandlers.clear();
                this.pendingFileSpecs.clear();
                for (final Closeable stream : this.openStreams.keySet()) {
                    Util.closeQuietly(stream);
                }
                this.openStreams.clear();
            }
        }

        private void parseLoop() {
            String fileSpec = null;
            try {
                while (true) {
                    Closeable streamOrReader = null;
                    RDFHandler handler = null;
                    try {
                        final RDFFormat format;
                        synchronized (this.pendingStreams) {
                            if (this.pendingStreams.isEmpty() && !this.pendingFileSpecs.isEmpty()) {
                                fileSpec = this.pendingFileSpecs.remove(0);
                                final File file = Statements.toRDFFile(fileSpec);
                                final RDFFormat fileFormat = Statements.toRDFFormat(fileSpec);
                                final InputStream fileStream = Streams.read(file);
                                final RDFHandler h = !this.rewriteBNodes ? this.parseHandler
                                        : Handlers.rewriteBNodes(this.parseHandler,
                                                Util.murmur3str(file.getAbsolutePath()));
                                if (ENABLE_PARALLEL_READ
                                        && Statements.isRDFFormatLineBased(fileFormat)) {
                                    LOGGER.debug("Using parallel {} text parsing for {}",
                                            fileFormat.getName(), fileSpec);
                                    final Reader fileReader = new InputStreamReader(fileStream,
                                            Charset.forName("UTF-8"));
                                    for (int i = 0; i < Util.CORES; ++i) {
                                        final Reader r = Streams.parallelBuffer(fileReader, '\n');
                                        this.pendingStreams.add(r);
                                        this.pendingHandlers.add(h);
                                        this.openStreams.put(r, fileFormat);
                                    }
                                } else {
                                    final boolean textFormat = Statements
                                            .isRDFFormatTextBased(fileFormat);
                                    LOGGER.debug("Using sequential {} {} parsing for {}",
                                            fileFormat.getName(), textFormat ? "text" : "binary",
                                            fileSpec);
                                    this.pendingStreams.add(fileStream);
                                    this.pendingHandlers.add(h);
                                    this.openStreams.put(fileStream, fileFormat);
                                }
                            }
                            if (this.pendingStreams.isEmpty()) {
                                break;
                            }
                            streamOrReader = this.pendingStreams.remove(0);
                            handler = this.pendingHandlers.remove(0);
                            format = this.openStreams.get(streamOrReader);
                        }
                        final String base = this.base == null ? "" : this.base;
                        final RDFParser parser = newRDFParser(format);
                        parser.setRDFHandler(handler);
                        if (streamOrReader instanceof Reader) {
                            parser.parse((Reader) streamOrReader, base); // already buffered
                        } else if (!Statements.isRDFFormatTextBased(format)) {
                            parser.parse(Streams.buffer((InputStream) streamOrReader), base);
                        } else {
                            final Reader reader = Streams.buffer(new InputStreamReader(
                                    (InputStream) streamOrReader, Charset.forName("UTF-8")));
                            parser.parse(reader, base);
                        }
                    } finally {
                        Util.closeQuietly(streamOrReader);
                        synchronized (this.pendingStreams) {
                            this.openStreams.remove(streamOrReader);
                        }
                    }
                }
            } catch (final Throwable ex) {
                final Throwable wrappedEx = new RDFHandlerException("Parsing of " + fileSpec
                        + " failed: " + ex.getMessage(), ex);
                if (this.exception == null) {
                    this.exception = wrappedEx; // report only first exception
                }
                parseHalt();
            } finally {
                this.latch.countDown();
            }
        }

    }

}
