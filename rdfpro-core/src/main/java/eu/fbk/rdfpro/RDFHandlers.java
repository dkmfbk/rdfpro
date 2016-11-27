/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Sorter;
import eu.fbk.rdfpro.util.Statements;

/**
 * Utility methods dealing with {@code RDFHandler}s.
 */
public final class RDFHandlers {

    /** The {@code int} value for method {@link RDFHandler#startRDF()}. */
    public static final int METHOD_START_RDF = 0x01;

    /** The {@code int} value for method {@link RDFHandler#handleComment(String)}. */
    public static final int METHOD_HANDLE_COMMENT = 0x02;

    /** The {@code int} value for method {@link RDFHandler#handleNamespace(String, String)}. */
    public static final int METHOD_HANDLE_NAMESPACE = 0x04;

    /** The {@code int} value for method {@link RDFHandler#handleStatement(Statement)}. */
    public static final int METHOD_HANDLE_STATEMENT = 0x08;

    /** The {@code int} value for method {@link RDFHandler#endRDF()}. */
    public static final int METHOD_END_RDF = 0x10;

    /** The {@code int} value for method {@link AutoCloseable#close()}. */
    public static final int METHOD_CLOSE = 0x20;

    /** The null {@code RDFHandler} that does nothing. */
    public static final AbstractRDFHandler NIL = new AbstractRDFHandler() {};

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFHandlers.class);

    private static final WriterConfig DEFAULT_WRITER_CONFIG;

    static {
        final WriterConfig config = new WriterConfig();
        config.set(BasicWriterSettings.PRETTY_PRINT, true);
        config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);
        config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
        DEFAULT_WRITER_CONFIG = config;
    }

    private RDFHandlers() {
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied statement collection. If the
     * collection is a {@link Model} or a {@link QuadModel}, it is also populated with namespaces.
     * If you don't want to populate namespaces, use {@link #wrap(Collection, Collection)} passing
     * null as second argument. Note that access to the collection is not synchronized, so it must
     * be thread-safe.
     *
     * @param statements
     *            the statement collection to populate, not null
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Collection<? super Statement> statements) {
        Objects.requireNonNull(statements);
        return new WrapHandler(statements,
                statements instanceof Model || statements instanceof QuadModel ? statements
                        : null);
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied statement and namespace
     * collections. Access to the two collections is not synchronized, so they MUST be thread safe
     * (may use {@link Collections#synchronizedCollection(Collection)}).
     *
     * @param statements
     *            the statement collection to populate, not null
     * @param namespaces
     *            the namespace collection to populate, or null to discard namespaces
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Collection<? super Statement> statements,
            @Nullable final Collection<? super Namespace> namespaces) {
        Objects.requireNonNull(statements);
        return new WrapHandler(statements, namespaces);
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied statement collection and
     * prefix-to-namespace-iri map. Access to the collection and map is not synchronized, so they
     * MUST be thread-safe (may use {@link Collections#synchronizedCollection(Collection)} and
     * {@link Collections#synchronizedMap(Map)}).
     *
     * @param statements
     *            the statement collection to populate, not null
     * @param namespaces
     *            the prefix-to-namespace-iri map to populate, or null to discard namespaces
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Collection<? super Statement> statements,
            @Nullable final Map<? super String, ? super String> namespaces) {
        Objects.requireNonNull(statements);
        return new WrapHandler(statements, namespaces);
    }

    /**
     * Returns an {@code RDFHandler} that writes data to the files at the locations specified.
     * Each location is either a file path or a full URL, possibly prefixed with an {@code .ext:}
     * fragment that overrides the file extension used to detect RDF format and compression.
     * Currently, URL different from {@code file://} are not supported, i.e., only local files can
     * be written. If more locations are specified, statements are divided among them evenly (in
     * chunks of configurable size). If no locations are given, the {@link #NIL} handler is
     * returned. Note that data is written at each pass, so you may consider filtering out
     * multiple passes to avoid writing the same data again.
     *
     * @param config
     *            the optional {@code WriterConfig} for fine tuning the writing process; if null,
     *            a default configuration enabling pretty printing will be used
     * @param chunkSize
     *            the number of consecutive statements to write as a block to a single location
     *            (at least 1)
     * @param locations
     *            the locations of the files to write
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler write(@Nullable final WriterConfig config, final int chunkSize,
            final String... locations) {
        final WriterConfig actualConfig = config != null ? config
                : RDFHandlers.DEFAULT_WRITER_CONFIG;
        final RDFHandler[] handlers = new RDFHandler[locations.length];
        for (int i = 0; i < locations.length; ++i) {
            final String location = locations[i];
            final RDFFormat format = Statements.toRDFFormat(location);
            final boolean parallel = Statements.isRDFFormatLineBased(format);
            handlers[i] = parallel ? new ParallelWriteHandler(actualConfig, location)
                    : new SequentialWriteHandler(actualConfig, location);
        }
        return handlers.length == 0 ? RDFHandlers.NIL
                : handlers.length == 1 ? handlers[0]
                        : RDFHandlers.dispatchRoundRobin(chunkSize, handlers);
    }

    /**
     * Returns an {@code RDFHandler} that uploads data to a SPARQL endpoint via SPARQL Update
     * INSERT DATA calls. Note that data is written at each pass, so you may consider filtering
     * out multiple passes to avoid writing the same data again.
     *
     * @param endpointURL
     *            the URL of the SPARQL Update endpoint, not null
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler update(final String endpointURL) {
        return new UpdateHandler(endpointURL, null);
    }

    /**
     * Wraps the supplied {@code RDFHandler} ignoring calls to the methods specified by the given
     * bitmap. Calling an ignored method on the returned {@code RDFHandler} produces no effect,
     * while calls to other method are forwarded to the wrapped {@code handler}. Use constants
     * {@code METHOD_XXX} in this class (OR-ed together) to define which methods to ignore.
     *
     * @param handler
     *            the handler to wrap, not null
     * @param ignoredMethods
     *            a bitmap specifying which methods to ignore
     * @return the created {@code RDFHandler} wrapper
     */
    public static RDFHandler ignoreMethods(final RDFHandler handler, final int ignoredMethods) {

        Objects.requireNonNull(handler);

        if (ignoredMethods == 0 || handler == RDFHandlers.NIL) {
            return handler;

        } else if ((ignoredMethods & RDFHandlers.METHOD_HANDLE_STATEMENT) == 0) {
            return new IgnoreMethodHandler(handler, ignoredMethods) {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    super.handleStatement(statement);
                }

            };

        } else {
            return new IgnoreMethodHandler(handler, ignoredMethods) {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    // Discard
                }

            };
        }
    }

    /**
     * Wraps the supplied {@code RDFHandler} discarding any method call after the specified number
     * of passes has been performed. Calls to {@code close()} are always propagated, however.
     *
     * @param handler
     *            the handler to wrap, not null
     * @param maxPasses
     *            the maximum number of passes to perform; additional passes will be ignored
     * @return the created {@code RDFHandler} wrapper
     */
    public static RDFHandler ignorePasses(final RDFHandler handler, final int maxPasses) {
        if (handler == RDFHandlers.NIL) {
            return handler;
        }
        return new AbstractRDFHandlerWrapper(handler) {

            private RDFHandler passHandler = null;

            private int pass = 0;

            @Override
            public void startRDF() throws RDFHandlerException {
                this.passHandler = this.pass < maxPasses ? this.handler : RDFHandlers.NIL;
                this.passHandler.startRDF();
            }

            @Override
            public void handleComment(final String comment) throws RDFHandlerException {
                this.passHandler.handleComment(comment);
            }

            @Override
            public void handleNamespace(final String prefix, final String iri)
                    throws RDFHandlerException {
                this.passHandler.handleNamespace(prefix, iri);
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                this.passHandler.handleStatement(statement);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                try {
                    this.passHandler.endRDF();
                } finally {
                    ++this.pass;
                }
            }

        };
    }

    /**
     * Returns an {@code RDFHandler} that dispatches calls to all the {@code RDFHandler}s
     * supplied. If no {@code RDFHandler} is supplied, {@link #NIL} is returned.
     *
     * @param handlers
     *            the {@code RDFHandler}s to forward calls to
     * @return the created {@code RDFHandler} dispatcher
     */
    public static RDFHandler dispatchAll(final RDFHandler... handlers) {
        return RDFHandlers.dispatchAll(handlers, new int[handlers.length]);
    }

    /**
     * Returns an {@code RDFHandler} that dispatches calls to all the {@code RDFHandler}s
     * supplied, optionally performing more passes on selected handlers. Argument
     * {@code extraPasses} controls how many passes a supplied handler should perform w.r.t. other
     * handlers. Operatively, if N is the maximum number in {@code extraPasses}, at pass I &lt; N
     * the dispatcher will forward calls only to handles whose extra passes value is greater than
     * N - I; in passes I &gt;= N all handlers will be called. This mechanism allows for selected
     * handlers to receive the additional passes they need for their initialization, without
     * performing these passes also on other handlers that do not need them. Note that if no
     * {@code RDFHandler} is supplied, {@link #NIL} is returned.
     *
     * @param handlers
     *            the {@code RDFHandler}s to forward calls to
     * @param extraPasses
     *            the number of extra passes for each supplied handler
     * @return the created {@code RDFHandler} dispatcher
     */
    public static RDFHandler dispatchAll(final RDFHandler[] handlers, final int[] extraPasses) {
        Objects.requireNonNull(extraPasses);
        if (Arrays.asList(handlers).contains(null)) {
            throw new NullPointerException();
        }
        if (handlers.length == 0) {
            return RDFHandlers.NIL;
        } else if (handlers.length == 1) {
            return handlers[0];
        } else if (handlers.length == 2 && extraPasses[0] == extraPasses[1]) {
            return new DispatchTwoHandler(handlers[0], handlers[1]);
        } else {
            return new DispatchAllHandler(handlers, extraPasses);
        }
    }

    /**
     * Returns an {@code RDFHandler} that dispatches calls to one of {@code RDFHandler}s supplied
     * chosen in a round robin fashion. More precisely, calls to methods
     * {@link RDFHandler#handleStatement(Statement)} and {@link RDFHandler#handleComment(String)}
     * are forwarded in a round robin fashion, with each chunk of {@code chunkSize >= 1}
     * consecutive calls dispatched to a certain handler. Other methods are always forwarded to
     * all the handlers. If no {@code RDFHandler} is supplied, {@link #NIL} is returned.
     *
     * @param chunkSize
     *            the chunk size, greater than or equal to 1; you may use this parameter to keep
     *            triples that are received consecutively together (as far as possible) when
     *            propagated to wrapped handlers, e.g., because in this way they compress better
     *            when written to a file (assuming input triples are somehow sorted)
     * @param handlers
     *            the {@code RDFHandler}s to forward calls to, in a round robin fashion
     * @return the created {@code RDFHandler} dispatcher
     */
    public static RDFHandler dispatchRoundRobin(final int chunkSize,
            final RDFHandler... handlers) {
        if (Arrays.asList(handlers).contains(null)) {
            throw new NullPointerException();
        }
        if (handlers.length == 0) {
            return RDFHandlers.NIL;
        } else if (handlers.length == 1) {
            return handlers[0];
        } else {
            return new DispatchRoundRobinHandler(chunkSize, handlers);
        }
    }

    /**
     * Returns an {@code RDFHandler} that collects multiple streams of RDF data, merging them by
     * means in a unique stream using a {@code SetOperator}. This method accepts as arguments the
     * sink where to forward the merged RDF stream, the {@code SetOperator} to apply and the
     * number of input streams to collect. The method returns an array of {@code RDFHandler}, one
     * for each input stream to be collected, that can be used by the method caller to provide the
     * data to be merged.
     *
     * @param handler
     *            the handler to decompose, not null
     * @param count
     *            the number of streams to collect and consequently the number of
     *            {@code RDFHandler}s to return (at least 1)
     * @param operation
     *            the set operation to apply to merge input RDF streams
     * @return the created {@code RDFHandler}s
     */
    public static RDFHandler[] collect(final RDFHandler handler, final int count,
            final SetOperator operation) {

        Objects.requireNonNull(handler);
        Objects.requireNonNull(operation);

        if (count < 1) {
            throw new IllegalArgumentException();
        }

        final RDFHandler[] result = new RDFHandler[count];

        if (count == 1 && (operation == SetOperator.SUM_MULTISET //
                || operation == SetOperator.UNION_MULTISET //
                || operation == SetOperator.INTERSECTION_MULTISET //
                /*    */ || operation == SetOperator.DIFFERENCE_MULTISET)) {
            result[0] = handler;
        } else if (count == 1 && operation == SetOperator.SYMMETRIC_DIFFERENCE
                || operation == SetOperator.SYMMETRIC_DIFFERENCE_MULTISET) {
            result[0] = RDFHandlers.NIL;
        } else if (operation == SetOperator.SUM_MULTISET) {
            Arrays.fill(result, new CollectMergerHandler(handler, count));
        } else if (operation == SetOperator.UNION) {
            Arrays.fill(result, new CollectSorterHandler(handler, count, true, true));
        } else {
            final CollectSetOperatorHandler sink;
            sink = new CollectSetOperatorHandler(handler, count, operation);
            for (int i = 0; i < count; ++i) {
                result[i] = new CollectLabellerHandler(sink, i);
            }
        }

        return result;
    }

    /**
     * Wraps the supplied {@code RDFHandler} (if necessary) so to buffer incoming statements and
     * use additional threads for their processing. An {@code RDFHandler} that has already been
     * decoupled is not wrapped again.
     *
     * @param handler
     *            the handler to wrap
     * @return the (possibly) wrapped handler
     */
    public static RDFHandler decouple(final RDFHandler handler) {
        if (handler == RDFHandlers.NIL || handler instanceof DecoupleHandler) {
            return handler;
        }
        return new DecoupleHandler(handler);
    }

    /**
     * Wraps the supplied {@code RDFHandelr} (if necessary) using a queue to decouple threads
     * submitting statements from threads consuming them. A positive number of consumer threads
     * should be specified. Wrapping does not occur if the handler is already decoupled using a
     * queue.
     *
     * @param handler
     *            the handler to wrap
     * @param numConsumerThreads
     *            the number of consumer threads, positive
     * @return the (possibly) wrapped handler
     */
    public static RDFHandler decouple(final RDFHandler handler, final int numConsumerThreads) {
        if (numConsumerThreads <= 0) {
            throw new IllegalArgumentException(
                    "Invalid number of consumer threads: " + numConsumerThreads);
        }
        if (handler == RDFHandlers.NIL || handler instanceof DecoupleQueueHandler) {
            return handler;
        }
        return new DecoupleQueueHandler(handler, numConsumerThreads);
    }

    /**
     * Wraps the supplied {@code RDFHandler} (if necessary) so that {@code handleXXX()} calls are
     * invoked in a mutually exclusive way. This method can be used with {@code RDFHandler}s that
     * are not thread-safe.
     *
     * @param handler
     *            the handler to wrap
     * @return the (possibly) wrapped handler
     */
    public static RDFHandler synchronize(final RDFHandler handler) {
        if (handler == RDFHandlers.NIL || handler instanceof SynchronizeHandler) {
            return handler;
        }
        return new SynchronizeHandler(handler);
    }

    private static final class WrapHandler extends AbstractRDFHandler {

        private final Collection<? super Statement> statementSink;

        private final Object namespaceSink;

        public WrapHandler(final Collection<? super Statement> statementSink,
                @Nullable final Object namespaceSink) {
            this.statementSink = statementSink;
            this.namespaceSink = namespaceSink;
        }

        @SuppressWarnings("unchecked")
        @Override
        public synchronized void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            if (this.namespaceSink instanceof Model) {
                ((Model) this.namespaceSink).setNamespace(prefix, iri);
            } else if (this.namespaceSink instanceof QuadModel) {
                ((QuadModel) this.namespaceSink).setNamespace(prefix, iri);
            } else if (this.namespaceSink instanceof Collection<?>) {
                ((Collection<Namespace>) this.namespaceSink).add(new SimpleNamespace(prefix, iri));
            } else if (this.namespaceSink instanceof Map<?, ?>) {
                ((Map<String, String>) this.namespaceSink).put(prefix, iri);
            }
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            this.statementSink.add(statement);
        }

    }

    private static final class SequentialWriteHandler extends AbstractRDFHandler {

        private final WriterConfig config;

        private final String location;

        @Nullable
        private Closeable out;

        @Nullable
        private RDFWriter writer;

        SequentialWriteHandler(final WriterConfig config, final String location) {
            this.config = config;
            this.location = location;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            try {
                final RDFFormat format = Statements.toRDFFormat(this.location);
                RDFHandlers.LOGGER.debug("Starting sequential {} writing of {}", format,
                        this.location);
                final OutputStream stream = IO.write(this.location);
                if (Statements.isRDFFormatTextBased(format)) {
                    this.out = IO.buffer(new OutputStreamWriter(stream, Charset.forName("UTF-8")));
                    this.writer = Rio.createWriter(format, (Writer) this.out);
                } else {
                    this.out = IO.buffer(stream);
                    this.writer = Rio.createWriter(format, (OutputStream) this.out);
                }
                this.writer.setWriterConfig(this.config);
                this.writer.startRDF();
            } catch (final IOException ex) {
                throw new RDFHandlerException("Could not write to " + this.location);
            }
            super.startRDF();
        }

        @Override
        public synchronized void handleComment(final String comment) throws RDFHandlerException {
            this.writer.handleComment(comment);
        }

        @Override
        public synchronized void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            this.writer.handleNamespace(prefix, iri);
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            this.writer.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.writer.endRDF();
            try {
                this.out.close();
            } catch (final IOException ex) {
                throw new RDFHandlerException("Unable to properly close " + this.location, ex);
            }
        }

        @Override
        public void close() {
            IO.closeQuietly(this.out); // should be already closed
            this.out = null;
            this.writer = null;
        }

    }

    private static final class ParallelWriteHandler extends AbstractRDFHandler {

        private final WriterConfig config;

        private final String location;

        @Nullable
        private OutputStream out;

        @Nullable
        private List<Writer> partialOuts;

        @Nullable
        private List<RDFWriter> partialWriters;

        @Nullable
        private ThreadLocal<RDFWriter> threadWriter;

        ParallelWriteHandler(final WriterConfig config, final String location) {
            this.config = config;
            this.location = location;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            try {
                RDFHandlers.LOGGER.debug("Starting parallel {} writing of {}",
                        Statements.toRDFFormat(this.location).getName(), this.location);
                this.out = IO.write(this.location);
                this.partialOuts = new ArrayList<Writer>();
                this.partialWriters = new ArrayList<RDFWriter>();
                this.threadWriter = new ThreadLocal<RDFWriter>() {

                    @Override
                    protected RDFWriter initialValue() {
                        return ParallelWriteHandler.this.newWriter();
                    }

                };

            } catch (final IOException ex) {
                throw new RDFHandlerException("Could not write to " + this.location);
            }
            super.startRDF();
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.threadWriter.get().handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            for (final RDFHandler partialWriter : this.partialWriters) {
                partialWriter.endRDF();
            }
            try {
                for (final Writer partialOut : this.partialOuts) {
                    partialOut.close();
                }
                this.out.close();
            } catch (final IOException ex) {
                throw new RDFHandlerException("Unable to properly close " + this.location, ex);
            }
        }

        @Override
        public void close() {
            IO.closeQuietly(this.out);
            this.out = null;
            this.partialOuts = null;
            this.partialWriters = null;
            this.threadWriter = null;
        }

        private RDFWriter newWriter() {
            final Writer partialOut = IO.utf8Writer(IO.parallelBuffer(this.out, (byte) '\n'));
            final RDFFormat format = Statements.toRDFFormat(this.location);
            final RDFWriter partialWriter = Rio.createWriter(format, partialOut);
            partialWriter.setWriterConfig(this.config);
            synchronized (this.partialOuts) {
                this.partialOuts.add(partialOut);
                this.partialWriters.add(partialWriter);
            }
            try {
                partialWriter.startRDF();
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }
            return partialWriter;
        }

    }

    private final static class UpdateHandler extends AbstractRDFHandler {

        private static final int DEFAULT_CHUNK_SIZE = 1024;

        private static final String HEAD = "INSERT DATA {\n";

        private final String endpointURL;

        private final int chunkSize;

        private final StringBuilder builder;

        private Resource lastCtx;

        private Resource lastSubj;

        private IRI lastPred;

        private int count;

        UpdateHandler(final String endpointURL, @Nullable final Integer chunkSize) {
            this.endpointURL = endpointURL;
            this.chunkSize = chunkSize != null ? chunkSize : UpdateHandler.DEFAULT_CHUNK_SIZE;
            this.builder = new StringBuilder();
            this.lastCtx = null;
            this.lastSubj = null;
            this.lastPred = null;
            this.count = 0;
            this.builder.append(UpdateHandler.HEAD);
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {

            final boolean sameCtx = Objects.equals(this.lastCtx, statement.getContext());
            final boolean sameSubj = sameCtx && statement.getSubject().equals(this.lastSubj);
            final boolean samePred = sameSubj && statement.getPredicate().equals(this.lastPred);

            if (this.lastSubj != null) {
                if (!sameSubj) {
                    this.builder.append(" .\n");
                }
                if (!sameCtx && this.lastCtx != null) {
                    this.builder.append("}\n");
                }
            }

            if (!sameCtx && statement.getContext() != null) {
                this.builder.append("GRAPH ");
                this.emit(statement.getContext());
                this.builder.append(" {\n");
            }

            if (!samePred) {
                if (!sameSubj) {
                    this.emit(statement.getSubject());
                    this.builder.append(" ");
                } else {
                    this.builder.append(" ; ");
                }
                this.emit(statement.getPredicate());
                this.builder.append(" ");
            } else {
                this.builder.append(" , ");
            }

            this.emit(statement.getObject());

            this.lastCtx = statement.getContext();
            this.lastSubj = statement.getSubject();
            this.lastPred = statement.getPredicate();

            ++this.count;
            if (this.count == this.chunkSize) {
                this.flush();
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.flush();
        }

        private void emit(final Value value) {
            try {
                Statements.formatValue(value, null, this.builder);
            } catch (final IOException ex) {
                throw new Error("Unexpected exception (!)", ex);
            }
        }

        private void flush() throws RDFHandlerException {
            if (this.count > 0) {
                if (this.lastSubj != null && this.lastCtx != null) {
                    this.builder.append("}");
                }
                this.builder.append("}");
                final String update = this.builder.toString();
                this.builder.setLength(UpdateHandler.HEAD.length());
                this.count = 0;
                try {
                    this.sendUpdate(update);
                } catch (final Throwable ex) {
                    throw new RDFHandlerException(ex);
                }
            }
        }

        private void sendUpdate(final String update) throws IOException {

            final byte[] requestBody = ("update=" + URLEncoder.encode(update, "UTF-8"))
                    .getBytes(Charset.forName("UTF-8"));

            final URL url = new URL(this.endpointURL);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded; charset=utf-8");
            connection.setRequestProperty("Content-Length", Integer.toString(requestBody.length));

            connection.connect();

            try {
                final DataOutputStream out = new DataOutputStream(connection.getOutputStream());
                out.write(requestBody);
                out.close();

                final int httpCode = connection.getResponseCode();
                if (httpCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException(
                            "Upload to '" + this.endpointURL + "' failed (HTTP " + httpCode + ")");
                }

            } finally {
                connection.disconnect();
            }
        }

    }

    private static abstract class IgnoreMethodHandler extends AbstractRDFHandlerWrapper {

        private final boolean forwardStartRDF;

        private final boolean forwardHandleComment;

        private final boolean forwardHandleNamespace;

        private final boolean forwardEndRDF;

        private final boolean forwardClose;

        IgnoreMethodHandler(final RDFHandler handler, final int ignoredMethods) {
            super(handler);
            this.forwardStartRDF = (ignoredMethods & RDFHandlers.METHOD_START_RDF) == 0;
            this.forwardHandleComment = (ignoredMethods & RDFHandlers.METHOD_HANDLE_COMMENT) == 0;
            this.forwardHandleNamespace = (ignoredMethods
                    & RDFHandlers.METHOD_HANDLE_NAMESPACE) == 0;
            this.forwardEndRDF = (ignoredMethods & RDFHandlers.METHOD_END_RDF) == 0;
            this.forwardClose = (ignoredMethods & RDFHandlers.METHOD_CLOSE) == 0;
        }

        @Override
        public final void startRDF() throws RDFHandlerException {
            if (this.forwardStartRDF) {
                super.startRDF();
            }
        }

        @Override
        public final void handleComment(final String comment) throws RDFHandlerException {
            if (this.forwardHandleComment) {
                super.handleComment(comment);
            }
        }

        @Override
        public final void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            if (this.forwardHandleNamespace) {
                super.handleNamespace(prefix, iri);
            }
        }

        @Override
        public final void endRDF() throws RDFHandlerException {
            if (this.forwardEndRDF) {
                super.endRDF();
            }
        }

        @Override
        public final void close() {
            if (this.forwardClose) {
                super.close();
            }
        }

    }

    private static final class DispatchTwoHandler extends AbstractRDFHandler {

        private final RDFHandler first;

        private final RDFHandler second;

        DispatchTwoHandler(final RDFHandler first, final RDFHandler second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.first.startRDF();
            this.second.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.first.handleComment(comment);
            this.second.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            this.first.handleNamespace(prefix, iri);
            this.second.handleNamespace(prefix, iri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.first.handleStatement(statement);
            this.second.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.first.endRDF();
            this.second.endRDF();
        }

        @Override
        public void close() {
            IO.closeQuietly(this.first);
            IO.closeQuietly(this.second);
        }

    }

    private static final class DispatchAllHandler extends AbstractRDFHandler {

        private final RDFHandler[] handlers;

        private final int[] extraPasses;

        private RDFHandler[] passHandlers;

        private int passIndex; // counting downward to zero

        DispatchAllHandler(final RDFHandler[] handlers, final int[] extraPasses) {

            int maxExtraPasses = 0;
            for (int i = 0; i < extraPasses.length; ++i) {
                maxExtraPasses = Math.max(maxExtraPasses, extraPasses[i]);
            }

            this.handlers = handlers;
            this.extraPasses = extraPasses;
            this.passHandlers = null;
            this.passIndex = maxExtraPasses;
        }

        @Override
        public void startRDF() throws RDFHandlerException {

            if (this.passIndex == 0) {
                this.passHandlers = this.handlers;
            } else {
                final List<RDFHandler> list = new ArrayList<RDFHandler>();
                for (int i = 0; i < this.handlers.length; ++i) {
                    if (this.extraPasses[i] >= this.passIndex) {
                        list.add(this.handlers[i]);
                    }
                }
                this.passHandlers = list.toArray(new RDFHandler[list.size()]);
                --this.passIndex;
            }

            for (final RDFHandler handler : this.passHandlers) {
                handler.startRDF();
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleComment(comment);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleNamespace(prefix, iri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleStatement(statement);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.endRDF();
            }
        }

        @Override
        public void close() {
            for (final RDFHandler handler : this.handlers) {
                IO.closeQuietly(handler);
            }
        }

    }

    private static final class DispatchRoundRobinHandler extends AbstractRDFHandler {

        private final AtomicLong counter = new AtomicLong(0);

        private final int chunkSize;

        private final RDFHandler[] handlers;

        DispatchRoundRobinHandler(final int chunkSize, final RDFHandler[] handlers) {
            this.chunkSize = chunkSize;
            this.handlers = handlers;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            for (final RDFHandler handler : this.handlers) {
                handler.startRDF();
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.pickHandler().handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            for (final RDFHandler handler : this.handlers) {
                handler.handleNamespace(prefix, iri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.pickHandler().handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            Throwable exception = null;
            for (final RDFHandler handler : this.handlers) {
                try {
                    handler.endRDF();
                } catch (final Throwable ex) {
                    if (exception == null) {
                        exception = ex;
                    } else {
                        exception.addSuppressed(ex);
                    }
                }
            }
            if (exception != null) {
                if (exception instanceof RuntimeException) {
                    throw (RDFHandlerException) exception;
                } else if (exception instanceof Error) {
                    throw (Error) exception;
                }
                throw (RDFHandlerException) exception;
            }
        }

        @Override
        public void close() {
            for (final RDFHandler handler : this.handlers) {
                IO.closeQuietly(handler);
            }
        }

        private RDFHandler pickHandler() {
            return this.handlers[(int) (this.counter.getAndIncrement() //
                    / this.chunkSize % this.handlers.length)];
        }

    }

    private static final class CollectLabellerHandler extends AbstractRDFHandlerWrapper {

        private final CollectMergerHandler collector;

        private final int label;

        CollectLabellerHandler(final CollectMergerHandler handler, final int label) {
            super(handler);
            this.collector = handler;
            this.label = label;
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.collector.handleStatement(statement, this.label);
        }

    }

    private static class CollectMergerHandler extends AbstractRDFHandlerWrapper {

        private final int size;

        private int pending;

        CollectMergerHandler(final RDFHandler handler, final int size) {
            super(handler);
            this.size = size;
            this.pending = 0;
        }

        @Override
        public final void startRDF() throws RDFHandlerException {
            if (this.pending <= 0) {
                this.pending = this.size;
                super.startRDF();
                this.doStartRDF();
            }
        }

        @Override
        public final void handleStatement(final Statement statement) throws RDFHandlerException {
            this.doHandleStatement(statement, 0);
        }

        public final void handleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            this.doHandleStatement(statement, label);
        }

        @Override
        public final void endRDF() throws RDFHandlerException {
            --this.pending;
            if (this.pending == 0) {
                this.doEndRDF();
                super.endRDF();
            }
        }

        @Override
        public void close() {
            super.close();
            this.doClose();
        }

        void doStartRDF() throws RDFHandlerException {
        }

        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            super.handleStatement(statement);
        }

        void doEndRDF() throws RDFHandlerException {
        }

        void doClose() {
        }

    }

    private static class CollectSorterHandler extends CollectMergerHandler {

        private final boolean deduplicate;

        private final boolean parallelize;

        private Sorter<Object[]> sorter;

        CollectSorterHandler(final RDFHandler handler, final int size, final boolean deduplicate,
                final boolean parallelize) {
            super(handler, size);
            this.deduplicate = deduplicate;
            this.parallelize = parallelize;
            this.sorter = null;
        }

        @Override
        void doStartRDF() throws RDFHandlerException {
            this.sorter = Sorter.newTupleSorter(true, Statement.class, Long.class);
            try {
                this.sorter.start(this.deduplicate);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            try {
                this.sorter.emit(new Object[] { statement, label });
            } catch (final Throwable ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doEndRDF() throws RDFHandlerException {
            try {
                this.sorter.end(this.parallelize, new Consumer<Object[]>() {

                    @Override
                    public void accept(final Object[] record) {
                        try {
                            final Statement statement = (Statement) record[0];
                            final int label = ((Long) record[1]).intValue();
                            CollectSorterHandler.this.doHandleStatementSorted(statement, label);
                        } catch (final RDFHandlerException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                });
                this.sorter.close();
                this.sorter = null;
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doClose() {
            IO.closeQuietly(this.sorter);
        }

        void doHandleStatementSorted(final Statement statement, final int label)
                throws RDFHandlerException {
            this.handler.handleStatement(statement);
        }

    }

    private static class CollectSetOperatorHandler extends CollectSorterHandler {

        private final SetOperator operator;

        private final int[] multiplicities;

        private Statement statement;

        CollectSetOperatorHandler(final RDFHandler handler, final int size,
                final SetOperator operator) {
            super(handler, size, false, false);
            this.operator = operator;
            this.multiplicities = new int[size];
            this.statement = null;
        }

        @Override
        void doHandleStatementSorted(final Statement statement, final int label)
                throws RDFHandlerException {

            if (!statement.equals(this.statement)
                    || !Objects.equals(statement.getContext(), this.statement.getContext())) {
                this.flush();
                this.statement = statement;
                Arrays.fill(this.multiplicities, 0);
            }
            ++this.multiplicities[label];
        }

        @Override
        void doEndRDF() throws RDFHandlerException {
            super.doEndRDF();
            this.flush();
        }

        private void flush() throws RDFHandlerException {
            if (this.statement != null) {
                final int multiplicity = this.operator.apply(this.multiplicities);
                for (int i = 0; i < multiplicity; ++i) {
                    this.handler.handleStatement(this.statement);
                }
            }
        }

    }

    private static final class DecoupleHandler extends AbstractRDFHandlerWrapper {

        private static final int BUFFER_SIZE = 4 * 1024;

        private final int numCores;

        private final Set<Thread> incomingThreads;

        private final List<Future<?>> futures;

        private final AtomicInteger counter;

        private Throwable exception;

        private Statement[] buffer;

        private int size;

        private int mask;

        private boolean disabled;

        DecoupleHandler(final RDFHandler handler) {
            super(handler);
            this.numCores = Environment.getCores();
            this.incomingThreads = new HashSet<Thread>(); // equals/hashCode based on identity
            this.futures = new LinkedList<Future<?>>();
            this.counter = new AtomicInteger(0);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.incomingThreads.clear();
            this.futures.clear();
            this.exception = null;
            this.buffer = new Statement[DecoupleHandler.BUFFER_SIZE];
            this.size = 0;
            this.mask = 0;
            this.disabled = false;
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            // Most compact implementation tackling frequent case decoupler is disabled
            if (this.disabled) {
                super.handleStatement(statement);
            } else {
                this.handleStatementHelper(statement);
            }
        }

        private void handleStatementHelper(final Statement statement) throws RDFHandlerException {
            if ((this.counter.getAndIncrement() & this.mask) != 0) {
                super.handleStatement(statement);
            } else {
                this.handleStatementInBackground(statement);
            }
        }

        private void handleStatementInBackground(final Statement statement)
                throws RDFHandlerException {

            Statement[] fullBuffer = null;
            synchronized (this) {
                this.buffer[this.size++] = statement;
                if (this.size == DecoupleHandler.BUFFER_SIZE) {
                    fullBuffer = this.buffer;
                    this.buffer = new Statement[DecoupleHandler.BUFFER_SIZE];
                    this.size = 0;
                    this.incomingThreads.add(Thread.currentThread());
                    this.checkNotFailed();
                    this.calibrateMask();
                    fullBuffer = this.handleStatementsInBackground(fullBuffer);
                }
            }

            if (fullBuffer != null) {
                for (final Statement stmt : fullBuffer) {
                    super.handleStatement(stmt);
                }
            }
        }

        private Statement[] handleStatementsInBackground(final Statement[] buffer) {

            // Update the list of pending futures. Abort if too many tasks pending.
            for (final Iterator<Future<?>> i = this.futures.iterator(); i.hasNext();) {
                final Future<?> future = i.next();
                if (future.isDone()) {
                    i.remove();
                }
            }

            // If there are too many tasks (futures) pending, do not use background processing
            if (this.futures.size() >= this.numCores) {
                return buffer;
            }

            // Schedule a background task, properly managing exceptions it may throw
            this.futures.add(Environment.getPool().submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        for (final Statement statement : buffer) {
                            DecoupleHandler.super.handleStatement(statement);
                        }
                    } catch (final Throwable ex) {
                        @SuppressWarnings("resource")
                        final DecoupleHandler h = DecoupleHandler.this;
                        synchronized (h) {
                            if (h.exception == null) {
                                h.exception = ex;
                            } else {
                                h.exception.addSuppressed(ex);
                            }
                            for (final Future<?> future : h.futures) {
                                future.cancel(false);
                            }
                        }
                    }
                }

            }));

            // Nothing left to be processed
            return null;
        }

        private void calibrateMask() {

            // Here we adapt the fraction of incoming statements that are buffered, based on the
            // numbers of threads we detected entered the decoupler. The fraction is chosen so to
            // divide the work evenly between N incoming threads and #CORES - N background
            // threads. In practice, the fraction moves quickly to 1000 (multiple threads in)
            // or to 1 (single thread in).
            final int numCores = Environment.getCores();
            final int numThreads = this.incomingThreads.size();
            if (numCores > numThreads) {
                this.mask = Integer.highestOneBit(numCores / (numCores - numThreads)) - 1;
            } else {
                this.mask = 0xFFFFFFFF;
                this.disabled = true;
                RDFHandlers.LOGGER.debug("Decoupler disabled");
            }

            // note: we do not declare mask as volatile, so the change will be picked up by
            // threads only at a later time, but this is not a problem
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

        @Override
        public void endRDF() throws RDFHandlerException {

            // Handle remaining buffered statements
            for (int i = 0; i < this.size; ++i) {
                super.handleStatement(this.buffer[i]);
            }

            // Wait for completion of pending tasks
            List<Future<?>> futuresToWaitFor;
            synchronized (this) {
                futuresToWaitFor = new ArrayList<Future<?>>(this.futures);
            }
            for (final Future<?> future : futuresToWaitFor) {
                while (!future.isDone()) {
                    try {
                        future.get();
                    } catch (final Throwable ex) {
                        // Ignore
                    }
                }
            }

            // Check there were no errors in background processing
            this.checkNotFailed();

            // Propagate
            super.endRDF();
        }

        @Override
        public void close() {
            super.close();
            synchronized (this) {
                for (final Future<?> future : this.futures) {
                    future.cancel(false);
                }
            }
        }

    }

    private static final class SynchronizeHandler extends AbstractRDFHandlerWrapper {

        SynchronizeHandler(final RDFHandler delegate) {
            super(delegate);
        }

        @Override
        public synchronized void handleComment(final String comment) throws RDFHandlerException {
            super.handleComment(comment);
        }

        @Override
        public synchronized void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            super.handleNamespace(prefix, iri);
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            super.handleStatement(statement);
        }

    }

    private static final class DecoupleQueueHandler extends AbstractRDFHandlerWrapper {

        private static final int CAPACITY = 4 * 1024;

        private static final Object EOF = new Object();

        private final int numConsumers;

        private AtomicReference<Throwable> exception;

        private BlockingQueue<Object> queue;

        private List<Future<?>> futures;

        DecoupleQueueHandler(final RDFHandler delegate, final int numConsumers) {
            super(delegate);
            this.numConsumers = numConsumers;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.exception = new AtomicReference<>(null);
            this.queue = new ArrayBlockingQueue<>(DecoupleQueueHandler.CAPACITY);
            this.futures = Lists.newArrayList();
            for (int i = 0; i < this.numConsumers; ++i) {
                this.futures.add(Environment.getPool().submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            final RDFHandler handler = DecoupleQueueHandler.this.handler;
                            while (true) {
                                if (DecoupleQueueHandler.this.exception.get() != null) {
                                    break;
                                }
                                final Object element = DecoupleQueueHandler.this.queue.take();
                                if (element instanceof Statement) {
                                    handler.handleStatement((Statement) element);
                                } else if (element instanceof Namespace) {
                                    final Namespace ns = (Namespace) element;
                                    handler.handleNamespace(ns.getPrefix(), ns.getName());
                                } else if (element instanceof String) {
                                    handler.handleComment((String) element);
                                } else if (element == DecoupleQueueHandler.EOF) {
                                    break;
                                }
                            }
                        } catch (final Throwable ex) {
                            DecoupleQueueHandler.this.exception.set(ex);
                            DecoupleQueueHandler.this.queue.clear();
                        }
                    }

                }));
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.check();
            this.put(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            this.check();
            this.put(new SimpleNamespace(prefix, iri));
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.check();
            this.put(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.check();
                this.put(DecoupleQueueHandler.EOF);
                for (final Future<?> future : this.futures) {
                    try {
                        future.get();
                    } catch (final Throwable ex) {
                        this.exception.compareAndSet(null, ex);
                    }
                }
                this.check();
                super.endRDF();
            } finally {
                this.exception = null;
                this.queue = null;
                this.futures = null;
            }
        }

        private void put(final Object object) throws RDFHandlerException {
            try {
                this.queue.put(object);
            } catch (final InterruptedException ex) {
                this.exception.set(ex);
                throw new RDFHandlerException(ex);
            }
        }

        private void check() throws RDFHandlerException {
            final Throwable ex = this.exception.get();
            if (ex != null) {
                Throwables.propagateIfPossible(ex, RDFHandlerException.class);
                throw new RDFHandlerException(ex);
            }
        }

    }

}
