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
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerBase;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Streams;
import eu.fbk.rdfpro.util.Tracker;
import eu.fbk.rdfpro.util.Util;

public final class WriteProcessor extends RDFProcessor {

    public static final boolean ENABLE_PARALLEL_WRITE = Boolean.parseBoolean(System.getProperty(
            "rdfp.parallel.write", "true"));

    private static final Logger LOGGER = LoggerFactory.getLogger(WriteProcessor.class);

    private final String[] fileSpecs;

    static WriteProcessor doCreate(final String... args) {
        final Options options = Options.parse("+", args);
        final String[] fileSpecs = options.getPositionalArgs(String.class).toArray(new String[0]);
        return new WriteProcessor(fileSpecs);
    }

    public WriteProcessor(final String... fileSpecs) {
        this.fileSpecs = fileSpecs.clone();
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final int count = this.fileSpecs.length;
        final RDFHandler[] handlers = new RDFHandler[count];
        try {
            for (int i = 0; i < count; ++i) {
                final String fileSpec = this.fileSpecs[i];
                final File file = Statements.toRDFFile(fileSpec);
                final RDFFormat format = Statements.toRDFFormat(fileSpec);
                final OutputStream stream = Streams.write(file);
                if (ENABLE_PARALLEL_WRITE && Statements.isRDFFormatLineBased(format)) {
                    LOGGER.debug("Using parallel {} writing for {}", format.getName(), fileSpec);
                    handlers[i] = new ParallelHandler(new OutputStreamWriter(stream,
                            Charset.forName("UTF-8")), format);
                } else {
                    LOGGER.debug("Using sequential {} writing for {}", format.getName(), fileSpec);
                    final Closeable streamOrWriter = Statements.isRDFFormatTextBased(format) ? Streams
                            .buffer(new OutputStreamWriter(stream, Charset.forName("UTF-8")))
                            : Streams.buffer(stream);
                    handlers[i] = new SequentialHandler(streamOrWriter, format);
                }
            }
        } catch (final Throwable ex) {
            for (final RDFHandler h : handlers) {
                Util.closeQuietly(h);
            }
            Util.propagate(ex);
        }
        final RDFHandler writer = Handlers.track(Handlers.decouple(Handlers
                .dispatchRoundRobin(handlers)), new Tracker(LOGGER, null, //
                "%d triples written (%d tr/s avg)", //
                "%d triples written (%d tr/s, %d tr/s avg)"));
        return Handlers.dispatchAll(handler != null ? handler : Handlers.nop(),
                Handlers.ignoreExtraPasses(writer));
    }

    public static RDFWriter newRDFWriter(final RDFFormat format, final Closeable streamOrWriter) {

        RDFWriter writer;
        if (streamOrWriter instanceof OutputStream) {
            writer = Rio.createWriter(format, (OutputStream) streamOrWriter);
        } else {
            writer = Rio.createWriter(format, (Writer) streamOrWriter);
        }

        final WriterConfig config = writer.getWriterConfig();
        config.set(BasicWriterSettings.PRETTY_PRINT, true);
        config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);
        config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
        return writer;
    }

    @Override
    public String toString() {
        String string = "write " + String.join(", ", Arrays.asList(this.fileSpecs));
        if (string.length() > 40) {
            string = string.substring(0, 37) + "...";
        }
        return string;
    }

    private static final class SequentialHandler extends HandlerWrapper {

        private final Closeable streamOrWriter;

        SequentialHandler(final Closeable streamOrWriter, final RDFFormat format) {
            super(newRDFWriter(format, streamOrWriter));
            this.streamOrWriter = streamOrWriter;
        }

        @Override
        public void close() {
            super.close();
            Util.closeQuietly(this.streamOrWriter);
        }

    }

    private static final class ParallelHandler extends HandlerBase {

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
                    final Writer writer = Streams
                            .parallelBuffer(ParallelHandler.this.writer, '\n');
                    final RDFHandler handler = newRDFWriter(ParallelHandler.this.format, writer);
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
