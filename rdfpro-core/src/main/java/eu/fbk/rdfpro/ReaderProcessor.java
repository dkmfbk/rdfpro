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

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ReaderProcessor extends RDFProcessor {

    public static final boolean ENABLE_PARALLEL_READ = Boolean.parseBoolean(System.getProperty(
            "rdfp.parallel.read", "true"));

    private static final Logger LOGGER = LoggerFactory.getLogger(ReaderProcessor.class);

    private final String[] fileSpecs;

    private final boolean rewriteBNodes;

    @Nullable
    private final String base;

    public ReaderProcessor(final boolean rewriteBNodes, @Nullable final String base,
            final String... fileSpecs) {
        this.rewriteBNodes = rewriteBNodes;
        this.fileSpecs = fileSpecs.clone();
        this.base = base;
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {

        Util.checkNotNull(handler);

        // Check that all filenames denote existing files for which an RDF format is usable
        final StringBuilder builder = new StringBuilder();
        for (final String fileSpec : this.fileSpecs) {
            final File file = Util.toRDFFile(fileSpec);
            final RDFFormat format = Util.toRDFFormat(fileSpec);
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
                final File firstFile = Util.toRDFFile(first);
                final File secondFile = Util.toRDFFile(second);
                return firstFile == null ? -1 : secondFile == null ? 1 //
                        : (int) (secondFile.length() - firstFile.length());
            }

        });

        return new Handler(Handlers.decouple(handler), this.rewriteBNodes, this.base, fileSpecs);
    }

    @Override
    public String toString() {
        String string = "read " + Util.join(", ", Arrays.asList(this.fileSpecs));
        if (string.length() > 40) {
            string = string.substring(0, 37) + "...";
        }
        return string;
    }

    private static final class Handler implements RDFHandler, Closeable {

        private final String[] fileSpecs;

        private final boolean rewriteBNodes;

        private final String base;

        private final int parallelism;

        private final RDFHandler sink;

        private final RDFHandler logSink;

        private final RDFHandler parseSink;

        private List<String> pendingFileSpecs;

        private List<Closeable> pendingStreams;

        private List<RDFHandler> pendingHandlers;

        private Map<Closeable, RDFFormat> openStreams;

        @Nullable
        private CountDownLatch latch;

        @Nullable
        private volatile Throwable exception;

        public Handler(final RDFHandler sink, final boolean rewriteBNodes,
                @Nullable final String base, final String... fileSpecs) {

            int parallelism = 0;
            for (final String fileSpec : fileSpecs) {
                final RDFFormat format = Util.toRDFFormat(fileSpec);
                if (Util.isRDFFormatLineBased(format)) {
                    parallelism = Threads.CORES;
                } else {
                    ++parallelism;
                }
                if (parallelism == Threads.CORES) {
                    break;
                }
            }

            this.fileSpecs = fileSpecs;
            this.rewriteBNodes = rewriteBNodes;
            this.base = base;
            this.parallelism = parallelism;
            this.sink = sink;
            this.logSink = Handlers.track(sink, LOGGER, null, //
                    "%d triples parsed (%d tr/s avg)", "0" + toString(), //
                    "%d triples parsed (%d tr/s, %d tr/s avg)");
            this.parseSink = Handlers.dropStartEnd(this.logSink);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.logSink.startRDF();
            if (this.parallelism > 0) {
                this.pendingFileSpecs = new ArrayList<String>(Arrays.asList(this.fileSpecs));
                this.pendingStreams = new ArrayList<Closeable>();
                this.pendingHandlers = new ArrayList<RDFHandler>();
                this.openStreams = new IdentityHashMap<Closeable, RDFFormat>();
                this.exception = null;
                this.latch = new CountDownLatch(this.parallelism);
                for (int i = 0; i < this.parallelism; ++i) {
                    Threads.getMainPool().execute(new Runnable() {

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
        public void handleComment(final String comment) throws RDFHandlerException {
            checkNotFailed();
            this.sink.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            checkNotFailed();
            this.sink.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            checkNotFailed();
            this.sink.handleStatement(statement);
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
            this.logSink.endRDF();
        }

        @Override
        public void close() {
            parseHalt();
            Util.closeQuietly(this.sink);
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
                                final File file = Util.toRDFFile(fileSpec);
                                final RDFFormat fileFormat = Util.toRDFFormat(fileSpec);
                                final InputStream fileStream = Streams.read(file);
                                final RDFHandler h = !this.rewriteBNodes ? this.parseSink
                                        : Handlers.rewriteBNodes(this.parseSink, Util
                                                .toString(Util.murmur3(file.getAbsolutePath())));
                                // TODO
                                // h = Handlers.track(h, LOGGER, null, file.getAbsolutePath()
                                // + " parsed (%d triples)", null, null);
                                if (ENABLE_PARALLEL_READ && Util.isRDFFormatLineBased(fileFormat)) {
                                    LOGGER.debug("Using parallel {} text parsing for {}",
                                            fileFormat.getName(), fileSpec);
                                    final Reader fileReader = new InputStreamReader(fileStream,
                                            Charset.forName("UTF-8"));
                                    for (int i = 0; i < Threads.CORES; ++i) {
                                        final Reader r = Streams.buffer(fileReader, '\n');
                                        this.pendingStreams.add(r);
                                        this.pendingHandlers.add(h);
                                        this.openStreams.put(r, fileFormat);
                                    }
                                } else {
                                    final boolean textFormat = Util
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
                        final RDFParser parser = Util.newRDFParser(format);
                        parser.setRDFHandler(handler);
                        if (streamOrReader instanceof Reader) {
                            parser.parse((Reader) streamOrReader, base); // already buffered
                        } else if (!Util.isRDFFormatTextBased(format)) {
                            parser.parse(Streams.buffer((InputStream) streamOrReader), base);
                        } else {
                            final Reader reader = Streams.buffer(new InputStreamReader(
                                    (InputStream) streamOrReader, Charset.forName("UTF-8")), null);
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
