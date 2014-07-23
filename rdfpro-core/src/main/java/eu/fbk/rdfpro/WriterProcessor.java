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
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class WriterProcessor extends RDFProcessor {

    public static final boolean ENABLE_PARALLEL_WRITE = Boolean.parseBoolean(System.getProperty(
            "rdfp.parallel.write", "true"));

    private static final Logger LOGGER = LoggerFactory.getLogger(WriterProcessor.class);

    private final String[] fileSpecs;

    WriterProcessor(final String... fileSpecs) {
        this.fileSpecs = fileSpecs.clone();
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        final List<RDFHandler> handlers = new ArrayList<RDFHandler>();
        try {
            for (final String fileSpec : this.fileSpecs) {
                final File file = Util.toRDFFile(fileSpec);
                final RDFFormat format = Util.toRDFFormat(fileSpec);
                final OutputStream stream = Streams.write(file);
                if (ENABLE_PARALLEL_WRITE && Util.isRDFFormatLineBased(format)) {
                    LOGGER.debug("Using parallel {} writing for {}", format.getName(), fileSpec);
                    handlers.add(new ParallelHandler(new OutputStreamWriter(stream, Charset
                            .forName("UTF-8")), format));
                } else {
                    LOGGER.debug("Using sequential {} writing for {}", format.getName(), fileSpec);
                    handlers.add(new SequentialHandler(stream, format));
                }
            }
        } catch (final Throwable ex) {
            for (final RDFHandler h : handlers) {
                Util.closeQuietly(h);
            }
            Util.propagate(ex);
        }
        final RDFHandler writer = Handlers.track(Handlers.decouple(Handlers.roundRobin(handlers)),
                LOGGER, null, "%d triples written (%d tr/s avg)", "9" + toString(),
                "%d triples written (%d tr/s, %d tr/s avg)");
        return Handlers.duplicate(handler, Handlers.dropExtraPasses(writer));
    }

    @Override
    public String toString() {
        String string = "write " + Util.join(", ", Arrays.asList(this.fileSpecs));
        if (string.length() > 40) {
            string = string.substring(0, 37) + "...";
        }
        return string;
    }

    private static final class SequentialHandler implements RDFHandler, Closeable {

        private final Closeable streamOrWriter;

        private final RDFHandler sink;

        SequentialHandler(final OutputStream stream, final RDFFormat format) {
            if (Util.isRDFFormatTextBased(format)) {
                this.streamOrWriter = Streams.buffer(
                        new OutputStreamWriter(stream, Charset.forName("UTF-8")), null);
                this.sink = Util.newRDFWriter(format, this.streamOrWriter);
            } else {
                this.streamOrWriter = Streams.buffer(stream);
                this.sink = Util.newRDFWriter(format, this.streamOrWriter);
            }
        }

        @Override
        public synchronized void startRDF() throws RDFHandlerException {
            this.sink.startRDF();
        }

        @Override
        public synchronized void handleComment(final String comment) throws RDFHandlerException {
            this.sink.handleComment(comment);
        }

        @Override
        public synchronized void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.sink.handleNamespace(prefix, uri);
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            this.sink.handleStatement(statement);
        }

        @Override
        public synchronized void endRDF() throws RDFHandlerException {
            this.sink.endRDF();
        }

        @Override
        public void close() {
            Util.closeQuietly(this.sink);
            Util.closeQuietly(this.streamOrWriter);
        }

    }

    private static final class ParallelHandler implements RDFHandler, Closeable {

        private Writer writer;

        private RDFFormat format;

        @Nullable
        private List<Writer> partialWriters;

        @Nullable
        private List<RDFHandler> partialHandlers;

        @Nullable
        private ThreadLocal<RDFHandler> localHandler;

        ParallelHandler(final Writer writer, final RDFFormat format) throws IOException {
            this.writer = writer;
            this.format = format;
            this.partialWriters = new ArrayList<Writer>();
            this.partialHandlers = new ArrayList<RDFHandler>();
            this.localHandler = new ThreadLocal<RDFHandler>() {

                @Override
                protected RDFHandler initialValue() {
                    final Writer writer = Streams.buffer(ParallelHandler.this.writer, '\n');
                    final RDFHandler handler = Util.newRDFWriter(ParallelHandler.this.format,
                            writer);
                    try {
                        handler.startRDF();
                    } catch (final RDFHandlerException ex) {
                        Util.propagate(ex);
                    }
                    synchronized (ParallelHandler.this.partialWriters) {
                        ParallelHandler.this.partialWriters.add(writer);
                        ParallelHandler.this.partialHandlers.add(handler);
                    }
                    return handler;
                }

            };
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            // not used
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // discarded
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            // discarded
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            final RDFHandler handler = this.localHandler.get();
            handler.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            for (final RDFHandler handler : this.partialHandlers) {
                handler.endRDF();
            }
            try {
                for (final Writer partialWriter : this.partialWriters) {
                    partialWriter.close();
                }
                this.writer.close();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void close() {
            Util.closeQuietly(this.writer);
            this.writer = null;
            this.format = null;
            this.partialWriters.clear();
            this.partialHandlers.clear();
            this.localHandler = null;
        }

    }

}
