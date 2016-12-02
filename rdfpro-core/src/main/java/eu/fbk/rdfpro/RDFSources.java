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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.rio.ParseErrorListener;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.ParseErrorLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;

/**
 * Utility methods dealing with {@code RDFSource}s.
 */
public final class RDFSources {

    private static final Logger LOGGER = LoggerFactory.getLogger(RDFSources.class);

    /** The null {@code RDFSource} that returns no statements, namespaces and comments. */
    public static final RDFSource NIL = new RDFSource() {

        @Override
        public void emit(final RDFHandler handler, final int passes)
                throws RDFSourceException, RDFHandlerException {
            Objects.requireNonNull(handler);
            try {
                for (int i = 0; i < passes; ++i) {
                    handler.startRDF();
                    handler.endRDF();
                }
            } finally {
                IO.closeQuietly(handler);
            }
        }

        @Override
        public Spliterator<Statement> spliterator() {
            return Spliterators.emptySpliterator();
        }

    };

    /**
     * Returns an {@code RDFSource} providing access to the statements in the supplied collection.
     * If the collection is a {@link Model} or {@link QuadModel}, also its namespaces are wrapped
     * and returned when reading from the source. If you don't want namespaces to be read, use
     * {@link #wrap(Iterable, Iterable)} passing null as second argument. The collection MUST NOT
     * be changed while the returned source is being used, as this will cause the source to return
     * different statements in different passes. Access to the collection is sequential, so it
     * does not need to be thread-safe.
     *
     * @param statements
     *            the statements collection, not null (can also be another {@code RDFSource})
     * @return the created {@code RDFSource}.
     */
    public static RDFSource wrap(final Iterable<? extends Statement> statements) {
        if (statements instanceof Model) {
            return RDFSources.wrap(statements, ((Model) statements).getNamespaces());
        } else if (statements instanceof QuadModel) {
            return RDFSources.wrap(statements, ((QuadModel) statements).getNamespaces());
        } else {
            return RDFSources.wrap(statements, Collections.emptyList());
        }
    }

    /**
     * Returns an {@code RDFSource} providing access to the statements and namespaces in the
     * supplied collections. The two collections MUST NOT be changed while the returned source is
     * being used, as this will cause the source to return different data in different passes.
     * Access to the collections is sequential, so they do not need to be thread-safe.
     *
     * @param statements
     *            the statements collection, not null (can also be another {@code RDFSource})
     * @param namespaces
     *            the namespaces collection, possibly null
     * @return the created {@code RDFSource}.
     */
    public static RDFSource wrap(final Iterable<? extends Statement> statements,
            @Nullable final Iterable<? extends Namespace> namespaces) {

        Objects.requireNonNull(statements);

        return new RDFSource() {

            @Override
            public void emit(final RDFHandler handler, final int passes)
                    throws RDFSourceException, RDFHandlerException {

                Objects.requireNonNull(handler);

                if (statements instanceof RDFSource) {
                    ((RDFSource) statements).emit(new AbstractRDFHandlerWrapper(handler) {

                        @Override
                        public void startRDF() throws RDFHandlerException {
                            super.startRDF();
                            if (namespaces != null) {
                                for (final Namespace ns : namespaces) {
                                    this.handler.handleNamespace(ns.getPrefix(), ns.getName());
                                }
                            }
                        }

                        @Override
                        public void handleComment(final String comment)
                                throws RDFHandlerException {
                            // discard
                        }

                        @Override
                        public void handleNamespace(final String prefix, final String iri)
                                throws RDFHandlerException {
                            // discard
                        }

                    }, passes);

                } else {
                    try {
                        for (int i = 0; i < passes; ++i) {
                            handler.startRDF();
                            if (namespaces != null) {
                                for (final Namespace ns : namespaces) {
                                    handler.handleNamespace(ns.getPrefix(), ns.getName());
                                }
                            }
                            for (final Statement statement : statements) {
                                handler.handleStatement(statement);
                            }
                            handler.endRDF();
                        }
                    } finally {
                        IO.closeQuietly(handler);
                    }
                }
            }

            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public Iterator<Statement> iterator() {
                return (Iterator) statements.iterator();
            }

            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public Spliterator<Statement> spliterator() {
                return (Spliterator) statements.spliterator();
            }

        };
    }

    /**
     * Returns an {@code RDFSource} providing access to the statements and namespaces in the
     * supplied collection and map. The collection and map MUST NOT be changed while the returned
     * source is being used, as this will cause the source to return different data in different
     * passes. Access to the collection and map is sequential, so they do not need to be
     * thread-safe.
     *
     * @param statements
     *            the statements collection, not null (can also be another {@code RDFSource})
     * @param namespaces
     *            the namespaces map, possibly null
     * @return the created {@code RDFSource}
     */
    public static RDFSource wrap(final Iterable<? extends Statement> statements,
            @Nullable final Map<String, String> namespaces) {

        List<Namespace> list;
        if (namespaces == null || namespaces.isEmpty()) {
            list = Collections.emptyList();
        } else {
            list = new ArrayList<Namespace>(namespaces.size());
            for (final Map.Entry<String, String> entry : namespaces.entrySet()) {
                list.add(new SimpleNamespace(entry.getKey(), entry.getValue()));
            }
        }

        return RDFSources.wrap(statements, list);
    }

    /**
     * Returns an {@code RDFSource} that reads files from the locations specified. Each location
     * is either a file path or a full URL, possibly prefixed with an {@code .ext:} fragment that
     * overrides the file extension used to detect RDF format and compression. Files are read
     * sequentially if {@code parallelize == false}, otherwise multiple files can be parsed in
     * parallel and files in a line-oriented RDF format can be parsed using multiple threads, in
     * both cases the effect being a greater throughput but no guarantee on the statements order,
     * however. Arguments {@code preserveBNodes}, {@code baseIRI} and {@code config} control the
     * parsing process.
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
     * @param skipBadStatements
     *            true if statements affected by errors in read RDF data (e.g., syntactically
     *            invalid URIs) have not to be injected in the output stream of the processor
     * @param dumpBadStatements
     *            true if statements affected by errors in read RDF data should be written on disk
     *            in a file named as the input file but with a ".error" qualifier
     * @param quiet
     *            true if warnings related to errors in read RDF data should not be emitted
     * @param locations
     *            the locations of the RDF files to be read
     * @return the created {@code RDFSource}
     */
    public static RDFSource read(final boolean parallelize, final boolean preserveBNodes,
            @Nullable final String baseIRI, @Nullable final ParserConfig config,
            @Nullable final Function<String, Writer> errorWriterSupplier,
            final boolean errorLogged, final String... locations) {
        return new FileSource(parallelize, preserveBNodes, baseIRI, config, errorWriterSupplier,
                errorLogged, locations);
    }

    /**
     * Returns an {@code RDFSource} that retrieves data from a SPARQL endpoint using SPARQL
     * CONSTRUCT or SELECT queries. CONSTRUCT queries are limited (due to SPARQL) to return only
     * triples in a default graph. SELECT query should return bindings for variables {@code s},
     * {@code p}, {@code o} and {@code c}, which are used as subject, predicate, object and
     * context of returned statements; incomplete or invalid bindings (e.g., bindings where the
     * subject is bound to a literal) are silently ignored.
     *
     * @param parallelize
     *            true to use multiple threads should for handling parsed triples
     * @param preserveBNodes
     *            true if BNodes in the query result should be preserved, false if they should be
     *            rewritten on a per-endpoint basis to avoid possible clashes
     * @param endpointURL
     *            the URL of the SPARQL endpoint, not null
     * @param query
     *            the SPARQL query (CONSTRUCT or SELECT form) to submit to the endpoint
     * @return the created {@code RDFSource}
     */
    public static RDFSource query(final boolean parallelize, final boolean preserveBNodes,
            final String endpointURL, final String query) {
        return new SparqlSource(parallelize, preserveBNodes, endpointURL, query);
    }

    private RDFSources() {
    }

    private static RDFHandler rewriteBNodes(final RDFHandler handler, final String suffix) {
        Objects.requireNonNull(suffix);
        return handler == RDFHandlers.NIL ? handler : new AbstractRDFHandlerWrapper(handler) {

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {

                if (statement.getSubject() instanceof BNode
                        || statement.getObject() instanceof BNode
                        || statement.getContext() instanceof BNode) {

                    final Resource s = (Resource) this.rewrite(statement.getSubject());
                    final IRI p = statement.getPredicate();
                    final Value o = this.rewrite(statement.getObject());
                    final Resource c = (Resource) this.rewrite(statement.getContext());

                    final ValueFactory vf = Statements.VALUE_FACTORY;
                    if (c == null) {
                        super.handleStatement(vf.createStatement(s, p, o));
                    } else {
                        super.handleStatement(vf.createStatement(s, p, o, c));
                    }

                } else {
                    super.handleStatement(statement);
                }
            }

            @Nullable
            private Value rewrite(@Nullable final Value value) {
                if (!(value instanceof BNode)) {
                    return value;
                }
                final String oldID = ((BNode) value).getID();
                final String newID = Hash.murmur3(oldID, suffix).toString();
                return Statements.VALUE_FACTORY.createBNode(newID);
            }

        };
    }

    @Nullable
    private static String getBaseIri(final RDFParser parser, @Nullable final String fallback) {
        Objects.requireNonNull(parser);
        try {
            final Field field = AbstractRDFParser.class.getDeclaredField("baseURI");
            field.setAccessible(true);
            final Object baseIri = field.get(parser);
            return baseIri == null ? null : baseIri.toString();
        } catch (final Throwable ex) {
            RDFSources.LOGGER.error(
                    "Could not extract base IRI from parser " + parser.getClass().getName(), ex);
        }
        return fallback;
    }

    private static class FileSource implements RDFSource {

        private final boolean parallelize;

        private final boolean preserveBNodes;

        private final String base;

        @Nullable
        private final Function<String, Writer> errorWriterSupplier;

        private final boolean errorLogged;

        private final AtomicLong errorCounter;

        private final AtomicLong errorLinesCounter;

        private final ParserConfig config;

        private final String[] locations;

        public FileSource(final boolean parallelize, final boolean preserveBNodes,
                @Nullable final String baseIRI, @Nullable final ParserConfig config,
                @Nullable final Function<String, Writer> errorWriterSupplier,
                final boolean errorLogged, final String... locations) {

            this.parallelize = parallelize;
            this.preserveBNodes = preserveBNodes;
            this.base = Strings.nullToEmpty(baseIRI);
            this.config = config != null ? config : Statements.newParserConfig(false);
            this.errorWriterSupplier = errorWriterSupplier;
            this.errorLogged = errorLogged;
            this.errorCounter = new AtomicLong();
            this.errorLinesCounter = new AtomicLong();
            this.locations = locations;
        }

        @Override
        public void emit(final RDFHandler handler, final int passes)
                throws RDFSourceException, RDFHandlerException {

            Objects.requireNonNull(handler);

            RDFHandler sink = handler;
            if (this.parallelize) {
                sink = RDFHandlers.decouple(sink);
            }

            final RDFHandler wrappedSink = RDFHandlers.ignoreMethods(sink,
                    RDFHandlers.METHOD_START_RDF | RDFHandlers.METHOD_END_RDF);

            try {
                for (int i = 0; i < passes; ++i) {
                    sink.startRDF();
                    this.parse(wrappedSink);
                    if (this.errorCounter.get() > 0) {
                        RDFSources.LOGGER.info("{} lines with parse errors/warnings ({} total)",
                                this.errorLinesCounter, this.errorCounter);
                        this.errorCounter.set(0);
                    }
                    sink.endRDF();
                }
            } catch (RuntimeException | Error ex) {
                throw ex;
            } catch (final Throwable ex) {
                throw new RDFSourceException(ex);
            } finally {
                IO.closeQuietly(handler);
            }
        }

        private void parse(final RDFHandler handler) throws Throwable {

            // Sort the locations based on decreasing size to improve throughput
            final String[] sortedLocations = this.locations.clone();
            Arrays.sort(sortedLocations, new Comparator<String>() {

                @Override
                public int compare(final String first, final String second) {
                    final URL firstURL = IO.extractURL(first);
                    final URL secondURL = IO.extractURL(second);
                    final boolean firstIsFile = "file".equals(firstURL.getProtocol());
                    final boolean secondIsFile = "file".equals(secondURL.getProtocol());
                    if (firstIsFile && secondIsFile) {
                        try {
                            final File firstFile = new File(firstURL.toURI());
                            final File secondFile = new File(secondURL.toURI());
                            return secondFile.length() > firstFile.length() ? 1 : -1;
                        } catch (final Throwable ex) {
                            // ignore
                        }
                    } else if (firstIsFile) {
                        return 1;
                    } else if (secondIsFile) {
                        return -1;
                    }
                    return firstURL.toString().compareTo(secondURL.toString());
                }

            });

            // Define file jobs
            final List<FileJob> fileJobs = Lists.newArrayList();
            for (final String location : this.locations) {
                fileJobs.add(new FileJob(location));
            }

            // Get runnable tasks for each job lazily, and execute them using multiple threads
            try {
                Environment.run(FluentIterable.from(fileJobs)
                        .transformAndConcat(job -> job.start(handler)));
            } finally {
                for (final FileJob fileJob : fileJobs) {
                    fileJob.stop();
                }
            }
        }

        private final class FileJob implements ParseErrorListener {

            private final String location;

            private final String locationAbbreviated;

            private final RDFFormat format;

            private final Set<Namespace> namespaces;

            @Nullable
            private String baseIri;

            @Nullable
            private List<Closeable> streams;

            @Nullable
            private final Set<Long> errorLines;

            FileJob(final String location) {

                // Compute abbreviated location (filename only)
                final int index = location.lastIndexOf('/');
                final String locationAbbreviated = index < 0 ? location
                        : location.substring(index + 1);

                // Determine RDF format based on supplied location
                final RDFFormat format = Rio
                        .getParserFormatForFileName("test" + IO.extractExtension(location))
                        .orElse(null);

                // Initialize state
                this.location = location;
                this.locationAbbreviated = locationAbbreviated;
                this.format = format;
                this.namespaces = Sets.newHashSet();
                this.streams = null;
                this.baseIri = null;
                this.errorLines = Sets.newHashSet();
            }

            synchronized List<Runnable> start(final RDFHandler handler) {

                // Check argument and state
                Preconditions.checkNotNull(handler);
                Preconditions.checkState(this.streams == null);

                // Wrap the supplied handler to rewrite BNodes at a per-file level, if requested
                final RDFHandler wrappedHandler = FileSource.this.preserveBNodes ? handler
                        : RDFSources.rewriteBNodes(handler,
                                Hash.murmur3(this.location).toString());

                try {
                    // Open streams
                    this.streams = Lists.newArrayList();
                    if (!Statements.isRDFFormatTextBased(this.format)) {
                        this.streams.add(IO.buffer(IO.read(this.location)));
                    } else if (!FileSource.this.parallelize
                            || !Statements.isRDFFormatLineBased(this.format)) {
                        this.streams.add(IO.utf8Reader(IO.buffer(IO.read(this.location))));
                    } else {
                        final InputStream s = IO.read(this.location);
                        for (int i = 0; i < Environment.getCores(); ++i) {
                            this.streams.add(IO.utf8Reader(IO.parallelBuffer(s, (byte) '\n')));
                        }
                    }

                    // Log beginning of parse operation
                    if (RDFSources.LOGGER.isDebugEnabled()) {
                        RDFSources.LOGGER.debug("Starting {} {} {} parsing for {}",
                                this.streams.size() == 1 ? "sequential" : "parallel",
                                this.streams.get(0) instanceof InputStream ? "binary" : "text",
                                this.location);
                    }

                    // Allocate a lock object used to synchronize concurrent parse threads
                    final ReentrantLock lock = new ReentrantLock();

                    // Create runnable parse tasks
                    final List<Runnable> parseJobs = new ArrayList<>();
                    for (final Closeable stream : this.streams) {
                        parseJobs.add(() -> {

                            // Acquire a lock to check if we are the first thread parsing the file
                            // (check done on baseIri being assigned). If not, unlock immediately
                            lock.lock();
                            final boolean firstThread = this.baseIri == null;
                            if (!firstThread) {
                                lock.unlock();
                            }

                            try {
                                // Get the parser config. The first thread may use the supplied
                                // config unchanged, while other threads have to clone it and
                                // inject namespaces parsed by first thread into it
                                ParserConfig config = FileSource.this.config;
                                if (!firstThread) {
                                    config = Statements.newParserConfig(config);
                                    config.set(BasicParserSettings.NAMESPACES, this.namespaces);
                                }

                                // Build the RDF parser. Warning and errors are routed to the
                                // FileJob object, while a special RDFHandler is needed for the
                                // first thread in order to intercept namespaces and baseIri
                                // changes in the preamble of the RDF file
                                final RDFParser parser = Rio.createParser(this.format);
                                parser.setParserConfig(config);
                                parser.setValueFactory(Statements.VALUE_FACTORY);
                                parser.setParseErrorListener(this);
                                parser.setRDFHandler(!firstThread ? wrappedHandler
                                        : new AbstractRDFHandlerWrapper(wrappedHandler) {

                                            private boolean insidePreamble = true;

                                            @Override
                                            public void handleNamespace(final String prefix,
                                                    final String uri) {
                                                // Propagate and store namespace, if in preamble
                                                super.handleNamespace(prefix, uri);
                                                if (this.insidePreamble) {
                                                    FileJob.this.namespaces
                                                            .add(new SimpleNamespace(prefix, uri));
                                                }
                                            }

                                            @Override
                                            public void handleStatement(final Statement stmt) {
                                                // Propagate and, at first statement (= end of
                                                // preamble) store baseIri and release lock
                                                super.handleStatement(stmt);
                                                if (this.insidePreamble) {
                                                    this.insidePreamble = false;
                                                    FileJob.this.baseIri = RDFSources.getBaseIri(
                                                            parser, FileSource.this.base);
                                                    lock.unlock();
                                                }
                                            }

                                        });

                                // Assign the initial value of the baseIri in the first thread
                                if (firstThread) {
                                    this.baseIri = FileSource.this.base;
                                }

                                // Perform parsing
                                if (stream instanceof InputStream) {
                                    parser.parse((InputStream) stream, this.baseIri);
                                } else {
                                    parser.parse((Reader) stream, this.baseIri);
                                }

                            } catch (final Throwable ex) {
                                // Wrap and propagate only if the FileJob is not being stopped
                                synchronized (FileJob.this) {
                                    if (this.streams != null) {
                                        final String m = "Parsing of " + this.location + " failed";
                                        if (ex instanceof RDFHandlerException) {
                                            throw new RDFHandlerException(m, ex);
                                        } else {
                                            throw new RDFSourceException(m, ex);
                                        }
                                    }
                                }

                            } finally {
                                // Make sure the lock is always released by this thread
                                if (lock.isHeldByCurrentThread()) {
                                    lock.unlock();
                                }

                                // Close the stream and remove it from the list of pending
                                // streams. If empty, close the job (nothing else to do)
                                IO.closeQuietly(stream);
                                synchronized (FileJob.this) {
                                    this.streams.remove(stream);
                                    if (this.streams.size() == 0) {
                                        this.stop();
                                    }
                                }
                            }

                        });
                    }
                    return parseJobs;

                } catch (final Throwable ex) {
                    // On failure, close everything, wrap if necessary and propagate
                    this.stop();
                    Throwables.throwIfUnchecked(ex);
                    throw new RuntimeException(ex);
                }
            }

            synchronized void stop() {

                // Abort if already closed
                if (this.streams == null) {
                    return;
                }

                // Close all opened streams and mark the object as closed
                for (final Closeable stream : this.streams) {
                    IO.closeQuietly(stream);
                }
                this.streams = null;

                // Dump bad lines, if recorded (in this case we assume text format)
                if (!this.errorLines.isEmpty() && FileSource.this.errorWriterSupplier != null) {
                    try {
                        // Get the writer where to emit bad lines. Abort if no writer returned
                        final Writer writer = FileSource.this.errorWriterSupplier
                                .apply(this.location);
                        if (writer == null) {
                            return;
                        }

                        // Emit the base IRI, if defined
                        if (!Strings.isNullOrEmpty(this.baseIri)) {
                            writer.write("@base ");
                            Statements.formatValue(
                                    Statements.VALUE_FACTORY.createIRI(this.baseIri), null,
                                    writer);
                            writer.write(" .\n\n");
                        }

                        // Emit namespaces, if defined
                        if (!this.namespaces.isEmpty()) {
                            final String originalLoc = IO.extractURL(this.location).toString();
                            final RDFFormat format = Rio.getParserFormatForFileName(originalLoc)
                                    .orElse(this.format);
                            final RDFWriter rdfWriter = Rio.createWriter(format, writer);
                            rdfWriter.startRDF();
                            for (final Namespace ns : this.namespaces) {
                                rdfWriter.handleNamespace(ns.getPrefix(), ns.getName());
                            }
                            rdfWriter.endRDF();
                            writer.write("\n");
                        }

                        // Emit bad lines, retrieving them from the input file
                        try (BufferedReader in = new BufferedReader(
                                IO.utf8Reader(IO.buffer(IO.read(this.location))))) {
                            long counter = 0;
                            String line;
                            while ((line = in.readLine()) != null) {
                                ++counter;
                                if (this.errorLines.contains(counter)) {
                                    writer.write(line);
                                    writer.write("\n");
                                }
                            }
                        } finally {
                            IO.closeQuietly(writer);
                        }

                    } catch (final Throwable ex) {
                        // On error, log and ignore
                        RDFSources.LOGGER.error("Failed to dump bad lines for " + this.location,
                                ex);
                    }
                }
            }

            @Override
            public void warning(final String msg, final long line, final long col) {
                this.recordError(line);
                if (FileSource.this.errorLogged) {
                    RDFSources.LOGGER.warn(this.errorMessageFor(msg, line, col));
                }
            }

            @Override
            public void error(final String msg, final long line, final long col) {
                this.recordError(line);
                if (FileSource.this.errorLogged) {
                    RDFSources.LOGGER.error(this.errorMessageFor(msg, line, col));
                }
            }

            @Override
            public void fatalError(final String msg, final long line, final long col) {
                this.recordError(line);
                if (FileSource.this.errorLogged) {
                    RDFSources.LOGGER.error(this.errorMessageFor(msg, line, col) + " (fatal)");
                }
            }

            private void recordError(final long line) {
                FileSource.this.errorCounter.incrementAndGet();
                if (line >= 0) {
                    synchronized (this.errorLines) {
                        final boolean added = this.errorLines.add(line);
                        if (added) {
                            FileSource.this.errorLinesCounter.incrementAndGet();
                        }
                    }
                }
            }

            private String errorMessageFor(final String msg, final long line, final long col) {
                return "PARSE ERROR [" + this.locationAbbreviated + ":" + line
                        + (col >= 0 ? "," + col : "") + "] " + msg.replaceAll("\\s+", " ");
            }

        }

    }

    private static class SparqlSource implements RDFSource {

        private final boolean parallelize;

        private final boolean preserveBNodes;

        private final String endpointURL;

        private final String query;

        private final boolean isSelect;

        SparqlSource(final boolean parallelize, final boolean preserveBNodes,
                final String endpointURL, final String query) {

            this.parallelize = parallelize;
            this.preserveBNodes = preserveBNodes;
            this.endpointURL = Objects.requireNonNull(endpointURL);
            this.query = Objects.requireNonNull(query);
            this.isSelect = SparqlSource.isSelectQuery(query);
        }

        @Override
        public void emit(final RDFHandler handler, final int passes)
                throws RDFSourceException, RDFHandlerException {

            // different BNodes may be returned each time the query is evaluated;
            // to allow preserving their identities, we should store the query result and
            // read from it from disk the next times
            Objects.requireNonNull(handler);

            RDFHandler actualHandler = handler;
            if (this.parallelize) {
                actualHandler = RDFHandlers.decouple(actualHandler);
            }
            if (!this.preserveBNodes) {
                actualHandler = RDFSources.rewriteBNodes(actualHandler, //
                        Hash.murmur3(this.endpointURL).toString());
            }

            try {
                for (int i = 0; i < passes; ++i) {
                    actualHandler.startRDF();
                    this.sendQuery(actualHandler);
                    actualHandler.endRDF();
                }
            } catch (RuntimeException | Error ex) {
                throw ex;
            } catch (final Throwable ex) {
                throw new RDFSourceException("Sparql query to " + this.endpointURL + " failed",
                        ex);
            } finally {
                IO.closeQuietly(actualHandler);
            }
        }

        private void sendQuery(final RDFHandler handler) throws Throwable {

            final List<String> acceptTypes;
            acceptTypes = this.isSelect
                    ? Arrays.asList("application/sparql-results+xml", "application/xml")
                    : RDFFormat.RDFXML.getMIMETypes();

            final byte[] requestBody = ("query=" + URLEncoder.encode(this.query, "UTF-8")
                    + "&infer=true").getBytes(Charset.forName("UTF-8"));

            final URL url = new URL(this.endpointURL);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", String.join(",", acceptTypes));
            connection.setRequestProperty("Content-Length", Integer.toString(requestBody.length));
            connection.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded; charset=utf-8");

            connection.connect();

            try {
                final DataOutputStream out = new DataOutputStream(connection.getOutputStream());
                out.write(requestBody);
                out.close();

                final int httpCode = connection.getResponseCode();
                if (httpCode != HttpURLConnection.HTTP_OK) {
                    throw new IOException("Download from '" + this.endpointURL + "' failed (HTTP "
                            + httpCode + ")");
                }

                try (InputStream in = connection.getInputStream()) {
                    if (this.isSelect) {
                        this.parseTupleResult(in, handler);
                    } else {
                        this.parseTripleResult(in, handler);
                    }
                }

            } finally {
                connection.disconnect();
            }
        }

        private void parseTripleResult(final InputStream stream, final RDFHandler handler)
                throws RDFHandlerException, RDFParseException, IOException {

            final ParserConfig parserConfig = new ParserConfig();
            parserConfig.addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES);
            parserConfig.addNonFatalError(BasicParserSettings.VERIFY_LANGUAGE_TAGS);

            final RDFParser parser = Rio.createParser(RDFFormat.RDFXML, Statements.VALUE_FACTORY);
            parser.setParserConfig(parserConfig);
            parser.setParseErrorListener(new ParseErrorLogger());
            parser.setRDFHandler(new AbstractRDFHandler() {

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    handler.handleStatement(statement);
                }

            });
            parser.parse(stream, this.endpointURL);
        }

        private void parseTupleResult(final InputStream stream, final RDFHandler handler)
                throws RDFHandlerException, XMLStreamException {

            final ValueFactory vf = Statements.VALUE_FACTORY;
            final Value[] values = new Value[4];

            final XMLStreamReader in = XMLInputFactory.newInstance().createXMLStreamReader(stream);

            while (in.nextTag() != XMLStreamConstants.START_ELEMENT
                    || !in.getLocalName().equals("results")) {
            }

            while (SparqlSource.enterChild(in, "result")) {
                while (SparqlSource.enterChild(in, "binding")) {
                    final String varName = in.getAttributeValue(null, "name");
                    final char var = Character.toLowerCase(varName.charAt(0));
                    final int index = var == 's' ? 0 : var == 'p' ? 1 : var == 'o' ? 2 : 3;
                    if (!SparqlSource.enterChild(in, null)) {
                        throw new XMLStreamException("Empty <binding> element found");
                    }
                    final String tag = in.getLocalName();
                    Value value;
                    if ("bnode".equals(tag)) {
                        value = vf.createBNode(in.getElementText());
                    } else if ("uri".equals(tag)) {
                        value = vf.createIRI(in.getElementText());
                    } else if ("literal".equals(tag)) {
                        final String lang = in.getAttributeValue(null, "lang");
                        final String dt = in.getAttributeValue(null, "datatype");
                        final String label = in.getElementText();
                        value = lang != null ? vf.createLiteral(label, lang)
                                : dt != null ? vf.createLiteral(label, vf.createIRI(dt))
                                        : vf.createLiteral(label);
                    } else {
                        throw new XMLStreamException(
                                "Expected <bnode>, <uri> or <literal>, found <" + tag + ">");
                    }
                    values[index] = value;
                    SparqlSource.leaveChild(in); // leave binding
                }
                SparqlSource.leaveChild(in); // leave result

                if (values[0] instanceof Resource && values[1] instanceof IRI
                        && values[2] != null) {
                    final Resource s = (Resource) values[0];
                    final IRI p = (IRI) values[1];
                    final Value o = values[2];
                    if (values[3] instanceof Resource) {
                        final Resource c = (Resource) values[3];
                        handler.handleStatement(vf.createStatement(s, p, o, c));
                    } else {
                        handler.handleStatement(vf.createStatement(s, p, o));
                    }
                }
                Arrays.fill(values, null);
            }

            while (in.nextTag() != XMLStreamConstants.END_DOCUMENT) {
            }
        }

        private static boolean enterChild(final XMLStreamReader in, @Nullable final String name)
                throws XMLStreamException {
            if (in.nextTag() == XMLStreamConstants.END_ELEMENT) {
                return false;
            }
            if (name == null || name.equals(in.getLocalName())) {
                return true;
            }
            final String childName = in.getLocalName();
            throw new XMLStreamException("Expected <" + name + ">, found <" + childName + ">");
        }

        private static void leaveChild(final XMLStreamReader in) throws XMLStreamException {
            if (in.nextTag() != XMLStreamConstants.END_ELEMENT) {
                throw new XMLStreamException("Unexpected element <" + in.getLocalName() + ">");
            }
        }

        private static boolean isSelectQuery(final String string) {
            final int length = string.length();
            int index = 0;
            while (index < length) {
                final char ch = string.charAt(index);
                if (ch == '#') { // comment
                    while (index < length && string.charAt(index) != '\n') {
                        ++index;
                    }
                } else if (ch == 'p' || ch == 'b' || ch == 'P' || ch == 'B') { // prefix or base
                    while (index < length && string.charAt(index) != '>') {
                        ++index;
                    }
                } else if (!Character.isWhitespace(ch)) { // found begin of query
                    final int start = index;
                    while (!Character.isWhitespace(string.charAt(index))) {
                        ++index;
                    }
                    final String form = string.substring(start, index).toLowerCase();
                    if (form.equals("select")) {
                        return true;
                    } else if (form.equals("construct") || form.equals("describe")) {
                        return false;
                    } else {
                        throw new IllegalArgumentException("Invalid query form: " + form);
                    }
                }
                ++index;
            }
            throw new IllegalArgumentException("Cannot detect SPARQL query form");
        }

    }

}
