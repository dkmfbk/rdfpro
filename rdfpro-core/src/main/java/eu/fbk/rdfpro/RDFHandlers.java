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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.openrdf.model.Model;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
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
     * Returns an {@code RDFHandler} that populates the supplied {@code Model} with received
     * statements and namespaces. Access to the model is externally synchronized, so it does not
     * need to be thread-safe. If you don't want to populate namespaces, cast the model to a
     * {@code Collection} so that method {@link #wrap(Collection)} is instead called.
     *
     * @param model
     *            the model to populate, not null
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Model model) {
        Objects.requireNonNull(model);
        return new AbstractRDFHandler() {

            @Override
            public void handleNamespace(final String prefix, final String uri) {
                model.setNamespace(prefix, uri);
            }

            @Override
            public void handleStatement(final Statement statement) {
                model.add(statement);
            }

        };
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied statement collection. Namespaces
     * and comments are dropped. Access to the collection is externally synchronized, so it does
     * not need to be thread-safe.
     *
     * @param statements
     *            the statement collection to populate, not null
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Collection<? super Statement> statements) {
        Objects.requireNonNull(statements);
        return new AbstractRDFHandler() {

            @Override
            public void handleStatement(final Statement statement) {
                statements.add(statement);
            }

        };
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied statement and namespace
     * collections. Access to the two collections is externally synchronized, so they do not need
     * to be thread-safe.
     *
     * @param statements
     *            the statement collection to populate, not null
     * @param namespaces
     *            the namespace collection to populate, not null
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Collection<? super Statement> statements,
            final Collection<? super Namespace> namespaces) {
        Objects.requireNonNull(statements);
        Objects.requireNonNull(namespaces);
        return new AbstractRDFHandler() {

            @Override
            public synchronized void handleNamespace(final String prefix, final String uri) {
                namespaces.add(new NamespaceImpl(prefix, uri));
            }

            @Override
            public synchronized void handleStatement(final Statement statement) {
                statements.add(statement);
            }

        };
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied statement collection and
     * prefix-to-namespace-uri map. Access to the collection and map is externally synchronized,
     * so they do not need to be thread-safe.
     *
     * @param statements
     *            the statement collection to populate, not null
     * @param namespaces
     *            the prefix-to-namespace-uri map to populate, not null
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler wrap(final Collection<? super Statement> statements,
            final Map<? super String, ? super String> namespaces) {
        Objects.requireNonNull(statements);
        Objects.requireNonNull(namespaces);
        return new AbstractRDFHandler() {

            @Override
            public synchronized void handleNamespace(final String prefix, final String uri) {
                namespaces.put(prefix, uri);
            }

            @Override
            public synchronized void handleStatement(final Statement statement) {
                statements.add(statement);
            }

        };
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
        final WriterConfig actualConfig = config != null ? config : DEFAULT_WRITER_CONFIG;
        final RDFHandler[] handlers = new RDFHandler[locations.length];
        for (int i = 0; i < locations.length; ++i) {
            final String location = locations[i];
            final RDFFormat format = Statements.toRDFFormat(location);
            final boolean parallel = Statements.isRDFFormatLineBased(format);
            handlers[i] = parallel ? new ParallelWriteHandler(actualConfig, location)
                    : new SequentialWriteHandler(actualConfig, location);
        }
        return handlers.length == 0 ? NIL : handlers.length == 1 ? handlers[0]
                : dispatchRoundRobin(chunkSize, handlers);
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

        if (ignoredMethods == 0 || handler == NIL) {
            return handler;

        } else if ((ignoredMethods & METHOD_HANDLE_STATEMENT) == 0) {
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
        if (handler == NIL) {
            return handler;
        }
        return new AbstractRDFHandlerWrapper(handler) {

            private RDFHandler passHandler = null;

            private int pass = 0;

            @Override
            public void startRDF() throws RDFHandlerException {
                this.passHandler = this.pass < maxPasses ? this.handler : NIL;
                this.passHandler.startRDF();
            }

            @Override
            public void handleComment(final String comment) throws RDFHandlerException {
                this.passHandler.handleComment(comment);
            }

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException {
                this.passHandler.handleNamespace(prefix, uri);
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
        return dispatchAll(handlers, new int[handlers.length]);
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
            return NIL;
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
    public static RDFHandler dispatchRoundRobin(final int chunkSize, final RDFHandler... handlers) {
        if (Arrays.asList(handlers).contains(null)) {
            throw new NullPointerException();
        }
        if (handlers.length == 0) {
            return NIL;
        } else if (handlers.length == 1) {
            return handlers[1];
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
        /*    */|| operation == SetOperator.DIFFERENCE_MULTISET)) {
            result[0] = handler;
        } else if (count == 1 && operation == SetOperator.SYMMETRIC_DIFFERENCE
                || operation == SetOperator.SYMMETRIC_DIFFERENCE_MULTISET) {
            result[0] = NIL;
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
        if (handler == NIL || handler instanceof DecoupleHandler) {
            return handler;
        }
        return new DecoupleHandler(handler);
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
                LOGGER.debug("Starting sequential {} writing of {}", format, this.location);
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
        public synchronized void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.writer.handleNamespace(prefix, uri);
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
                LOGGER.debug("Starting parallel {} writing of {}",
                        Statements.toRDFFormat(this.location).getName(), this.location);
                this.out = IO.write(this.location);
                this.partialOuts = new ArrayList<Writer>();
                this.partialWriters = new ArrayList<RDFWriter>();
                this.threadWriter = new ThreadLocal<RDFWriter>() {

                    @Override
                    protected RDFWriter initialValue() {
                        return newWriter();
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

        private URI lastPred;

        private int count;

        UpdateHandler(final String endpointURL, @Nullable final Integer chunkSize) {
            this.endpointURL = endpointURL;
            this.chunkSize = chunkSize != null ? chunkSize : DEFAULT_CHUNK_SIZE;
            this.builder = new StringBuilder();
            this.lastCtx = null;
            this.lastSubj = null;
            this.lastPred = null;
            this.count = 0;
            this.builder.append(HEAD);
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
                emit(statement.getContext());
                this.builder.append(" {\n");
            }

            if (!samePred) {
                if (!sameSubj) {
                    emit(statement.getSubject());
                    this.builder.append(" ");
                } else {
                    this.builder.append(" ; ");
                }
                emit(statement.getPredicate());
                this.builder.append(" ");
            } else {
                this.builder.append(" , ");
            }

            emit(statement.getObject());

            this.lastCtx = statement.getContext();
            this.lastSubj = statement.getSubject();
            this.lastPred = statement.getPredicate();

            ++this.count;
            if (this.count == this.chunkSize) {
                flush();
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            flush();
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
                this.builder.setLength(HEAD.length());
                this.count = 0;
                try {
                    sendUpdate(update);
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
                    throw new IOException("Upload to '" + this.endpointURL + "' failed (HTTP "
                            + httpCode + ")");
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
            this.forwardStartRDF = (ignoredMethods & METHOD_START_RDF) == 0;
            this.forwardHandleComment = (ignoredMethods & METHOD_HANDLE_COMMENT) == 0;
            this.forwardHandleNamespace = (ignoredMethods & METHOD_HANDLE_NAMESPACE) == 0;
            this.forwardEndRDF = (ignoredMethods & METHOD_END_RDF) == 0;
            this.forwardClose = (ignoredMethods & METHOD_CLOSE) == 0;
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
        public final void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            if (this.forwardHandleNamespace) {
                super.handleNamespace(prefix, uri);
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
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.first.handleNamespace(prefix, uri);
            this.second.handleNamespace(prefix, uri);
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
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleNamespace(prefix, uri);
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
            pickHandler().handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            for (final RDFHandler handler : this.handlers) {
                handler.handleNamespace(prefix, uri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            pickHandler().handleStatement(statement);
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
                doStartRDF();
            }
        }

        @Override
        public final void handleStatement(final Statement statement) throws RDFHandlerException {
            doHandleStatement(statement, 0);
        }

        public final void handleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            doHandleStatement(statement, label);
        }

        @Override
        public final void endRDF() throws RDFHandlerException {
            --this.pending;
            if (this.pending == 0) {
                doEndRDF();
                super.endRDF();
            }
        }

        @Override
        public void close() {
            super.close();
            doClose();
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
                            doHandleStatementSorted(statement, label);
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
                flush();
                this.statement = statement;
                Arrays.fill(this.multiplicities, 0);
            }
            ++this.multiplicities[label];
        }

        @Override
        void doEndRDF() throws RDFHandlerException {
            super.doEndRDF();
            flush();
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
            this.buffer = new Statement[BUFFER_SIZE];
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
                handleStatementHelper(statement);
            }
        }

        private void handleStatementHelper(final Statement statement) throws RDFHandlerException {
            if ((this.counter.getAndIncrement() & this.mask) != 0) {
                super.handleStatement(statement);
            } else {
                handleStatementInBackground(statement);
            }
        }

        private void handleStatementInBackground(final Statement statement)
                throws RDFHandlerException {

            Statement[] fullBuffer = null;
            synchronized (this) {
                this.buffer[this.size++] = statement;
                if (this.size == BUFFER_SIZE) {
                    fullBuffer = this.buffer;
                    this.buffer = new Statement[BUFFER_SIZE];
                    this.size = 0;
                    this.incomingThreads.add(Thread.currentThread());
                    checkNotFailed();
                    calibrateMask();
                    fullBuffer = handleStatementsInBackground(fullBuffer);
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
                LOGGER.debug("Decoupler disabled");
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
            checkNotFailed();

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

}
