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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.openrdf.model.BNode;
import org.openrdf.model.Model;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.ParseErrorLogger;
import org.openrdf.rio.helpers.RDFJSONParserSettings;
import org.openrdf.rio.helpers.TriXParserSettings;
import org.openrdf.rio.helpers.XMLParserSettings;
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

    private static final ParserConfig DEFAULT_PARSER_CONFIG;

    static {
        final ParserConfig config = new ParserConfig();
        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
        config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
        config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
        config.set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, false);
        config.set(BasicParserSettings.NORMALIZE_LANGUAGE_TAGS, true);
        config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
        config.set(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES, false);
        config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_DATATYPES, false);
        config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_LANGUAGES, false);
        config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_TYPES, false);
        config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_VALUES, false);
        config.set(RDFJSONParserSettings.FAIL_ON_UNKNOWN_PROPERTY, false);
        config.set(RDFJSONParserSettings.SUPPORT_GRAPHS_EXTENSION, true);
        config.set(TriXParserSettings.FAIL_ON_TRIX_INVALID_STATEMENT, false);
        config.set(TriXParserSettings.FAIL_ON_TRIX_MISSING_DATATYPE, false);
        config.set(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID, false);
        config.set(XMLParserSettings.FAIL_ON_INVALID_NCNAME, false);
        config.set(XMLParserSettings.FAIL_ON_INVALID_QNAME, false);
        config.set(XMLParserSettings.FAIL_ON_MISMATCHED_TAGS, false);
        config.set(XMLParserSettings.FAIL_ON_NON_STANDARD_ATTRIBUTES, false);
        config.set(XMLParserSettings.FAIL_ON_SAX_NON_FATAL_ERRORS, false);
        DEFAULT_PARSER_CONFIG = config;
    }

    /** The null {@code RDFSource} that returns no statements, namespaces and comments. */
    public static final RDFSource NIL = new RDFSource() {

        @Override
        public void emit(final RDFHandler handler, final int passes) throws RDFSourceException,
                RDFHandlerException {
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
            return wrap(statements, ((Model) statements).getNamespaces());
        } else if (statements instanceof QuadModel) {
            return wrap(statements, ((QuadModel) statements).getNamespaces());
        } else {
            return wrap(statements, Collections.emptyList());
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
                        public void handleComment(final String comment) throws RDFHandlerException {
                            // discard
                        }

                        @Override
                        public void handleNamespace(final String prefix, final String uri)
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
                list.add(new NamespaceImpl(entry.getKey(), entry.getValue()));
            }
        }

        return wrap(statements, list);
    }

    /**
     * Returns an {@code RDFSource} that reads files from the locations specified. Each location
     * is either a file path or a full URL, possibly prefixed with an {@code .ext:} fragment that
     * overrides the file extension used to detect RDF format and compression. Files are read
     * sequentially if {@code parallelize == false}, otherwise multiple files can be parsed in
     * parallel and files in a line-oriented RDF format can be parsed using multiple threads, in
     * both cases the effect being a greater throughput but no guarantee on the statements order,
     * however. Arguments {@code preserveBNodes}, {@code baseURI} and {@code config} control the
     * parsing process.
     *
     * @param parallelize
     *            false if files should be parsed sequentially using only one thread
     * @param preserveBNodes
     *            true if BNodes in parsed files should be preserved, false if they should be
     *            rewritten on a per-file basis to avoid possible clashes
     * @param baseURI
     *            the base URI to be used for resolving relative URIs, possibly null
     * @param config
     *            the optional {@code ParserConfig} for the fine tuning of the used RDF parser; if
     *            null a default, maximally permissive configuration will be used
     * @param locations
     *            the locations of the RDF files to be read
     * @return the created {@code RDFSource}
     */
    public static RDFSource read(final boolean parallelize, final boolean preserveBNodes,
            @Nullable final String baseURI, @Nullable final ParserConfig config,
            final String... locations) {
        return new FileSource(parallelize, preserveBNodes, baseURI, config, locations);
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

                    final Resource s = (Resource) rewrite(statement.getSubject());
                    final URI p = statement.getPredicate();
                    final Value o = rewrite(statement.getObject());
                    final Resource c = (Resource) rewrite(statement.getContext());

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

    private static class FileSource implements RDFSource {

        private static final Logger LOGGER = LoggerFactory.getLogger(FileSource.class);

        private final boolean parallelize;

        private final boolean preserveBNodes;

        private final String base;

        private final ParserConfig parserConfig;

        private final String[] locations;

        public FileSource(final boolean parallelize, final boolean preserveBNodes,
                @Nullable final String baseURI, @Nullable final ParserConfig parserConfig,
                final String... locations) {

            this.parallelize = parallelize;
            this.preserveBNodes = preserveBNodes;
            this.base = baseURI != null ? baseURI : "";
            this.parserConfig = parserConfig != null ? parserConfig : DEFAULT_PARSER_CONFIG;
            this.locations = locations;
        }

        @Override
        public void emit(final RDFHandler handler, final int passes) throws RDFSourceException,
                RDFHandlerException {

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
                    parse(wrappedSink);
                    sink.endRDF();
                }
            } catch (RDFHandlerException | RuntimeException | Error ex) {
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

            final Map<String, InputStream> streams = new HashMap<String, InputStream>();

            final List<ParseJob> jobs = new ArrayList<ParseJob>();
            for (final String location : this.locations) {
                final RDFFormat format = Rio.getParserFormatForFileName("test"
                        + IO.extractExtension(location));
                final int parallelism = !this.parallelize
                        || !Statements.isRDFFormatLineBased(format) ? 1 : Environment.getCores();
                for (int i = 0; i < parallelism; ++i) {
                    jobs.add(new ParseJob(streams, location.toString(), handler));
                }
            }

            final int parallelism = !this.parallelize ? 1 : Math.min(Environment.getCores(),
                    jobs.size());

            final CountDownLatch latch = new CountDownLatch(parallelism);
            final AtomicReference<Throwable> exception = new AtomicReference<Throwable>();
            final AtomicInteger index = new AtomicInteger(0);

            final List<Runnable> runnables = new ArrayList<Runnable>();
            for (int i = 0; i < parallelism; ++i) {
                runnables.add(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            while (true) {
                                ParseJob currentJob;
                                synchronized (jobs) {
                                    final int i = index.getAndIncrement();
                                    if (i >= jobs.size()) {
                                        break;
                                    }
                                    currentJob = jobs.get(i);
                                }
                                currentJob.run();
                            }
                        } catch (final Throwable ex) {
                            synchronized (jobs) {
                                for (final ParseJob job : jobs) {
                                    job.cancel();
                                }
                            }
                            exception.set(ex);
                        } finally {
                            latch.countDown();
                        }
                    }

                });
            }

            try {
                for (int i = 1; i < parallelism; ++i) {
                    Environment.getPool().execute(runnables.get(i));
                }
                if (!runnables.isEmpty()) {
                    runnables.get(0).run();
                }
                latch.await();
                if (exception.get() != null) {
                    throw exception.get();
                }
            } finally {
                for (final InputStream stream : streams.values()) {
                    IO.closeQuietly(stream);
                }
            }
        }

        private class ParseJob {

            private final Map<String, InputStream> streams;

            private final String location;

            private final RDFHandler handler;

            private volatile boolean closed;

            private Closeable in;

            ParseJob(final Map<String, InputStream> streams, final String location,
                    final RDFHandler handler) {
                this.streams = streams;
                this.location = location;
                this.handler = handler;
                this.closed = false;
                this.in = null;
            }

            void cancel() {
                this.closed = true;
                IO.closeQuietly(this.in);
            }

            void run() throws Throwable {

                if (this.closed) {
                    return;
                }

                final RDFFormat format = Rio.getParserFormatForFileName("test"
                        + IO.extractExtension(this.location));

                final String logMsg = "Starting {} {} {} parsing for {}";
                if (!Statements.isRDFFormatTextBased(format)) {
                    LOGGER.debug(logMsg, "sequential", "binary", format.getName(), this.location);
                    this.in = IO.buffer(IO.read(this.location));

                } else if (!FileSource.this.parallelize
                        || !Statements.isRDFFormatLineBased(format)) {
                    LOGGER.debug(logMsg, "sequential", "text", format.getName(), this.location);
                    this.in = IO.buffer(new InputStreamReader(IO.read(this.location), Charset
                            .forName("UTF-8")));

                } else {
                    LOGGER.debug(logMsg, "parallel", "text", format.getName(), this.location);
                    synchronized (this.streams) {
                        InputStream stream = this.streams.get(this.location);
                        if (stream == null) {
                            if (this.streams.containsKey(this.location)) {
                                return; // read already completed for file at location
                            }
                            stream = IO.read(this.location);
                            this.streams.put(this.location, stream);
                        }
                        this.in = IO.utf8Reader(IO.parallelBuffer(stream, (byte) '\n'));
                    }
                }

                try {
                    final RDFHandler handler = FileSource.this.preserveBNodes ? this.handler //
                            : rewriteBNodes(this.handler, Hash.murmur3(this.location).toString());
                    final RDFParser parser = Rio.createParser(format);
                    parser.setParserConfig(FileSource.this.parserConfig);
                    parser.setValueFactory(Statements.VALUE_FACTORY);
                    parser.setRDFHandler(handler);
                    if (this.in instanceof InputStream) {
                        parser.parse((InputStream) this.in, FileSource.this.base);
                    } else {
                        parser.parse((Reader) this.in, FileSource.this.base);
                    }
                } catch (final Throwable ex) {
                    if (!this.closed) {
                        final String exMsg = "Parsing of " + this.location + " failed";
                        if (ex instanceof RDFHandlerException) {
                            throw new RDFHandlerException(exMsg, ex);
                        } else {
                            throw new RDFSourceException(exMsg, ex);
                        }
                    }
                } finally {
                    synchronized (this.streams) {
                        IO.closeQuietly(this.in);
                        this.in = null;
                        this.streams.put(this.location, null); // ensure stream is not read again
                    }
                }
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
            this.isSelect = isSelectQuery(query);
        }

        @Override
        public void emit(final RDFHandler handler, final int passes) throws RDFSourceException,
                RDFHandlerException {

            // different BNodes may be returned each time the query is evaluated;
            // to allow preserving their identities, we should store the query result and
            // read from it from disk the next times
            Objects.requireNonNull(handler);

            RDFHandler actualHandler = handler;
            if (this.parallelize) {
                actualHandler = RDFHandlers.decouple(actualHandler);
            }
            if (!this.preserveBNodes) {
                actualHandler = rewriteBNodes(actualHandler, //
                        Hash.murmur3(this.endpointURL).toString());
            }

            try {
                for (int i = 0; i < passes; ++i) {
                    actualHandler.startRDF();
                    sendQuery(actualHandler);
                    actualHandler.endRDF();
                }
            } catch (RDFHandlerException | RuntimeException | Error ex) {
                throw ex;
            } catch (final Throwable ex) {
                throw new RDFSourceException("Sparql query to " + this.endpointURL + " failed", ex);
            } finally {
                IO.closeQuietly(actualHandler);
            }
        }

        private void sendQuery(final RDFHandler handler) throws Throwable {

            final List<String> acceptTypes;
            acceptTypes = this.isSelect ? Arrays.asList("application/sparql-results+xml",
                    "application/xml") : RDFFormat.RDFXML.getMIMETypes();

            final byte[] requestBody = ("query=" + URLEncoder.encode(this.query, "UTF-8") + "&infer=true")
                    .getBytes(Charset.forName("UTF-8"));

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
                        parseTupleResult(in, handler);
                    } else {
                        parseTripleResult(in, handler);
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

            while (enterChild(in, "result")) {
                while (enterChild(in, "binding")) {
                    final String varName = in.getAttributeValue(null, "name");
                    final char var = Character.toLowerCase(varName.charAt(0));
                    final int index = var == 's' ? 0 : var == 'p' ? 1 : var == 'o' ? 2 : 3;
                    if (!enterChild(in, null)) {
                        throw new XMLStreamException("Empty <binding> element found");
                    }
                    final String tag = in.getLocalName();
                    Value value;
                    if ("bnode".equals(tag)) {
                        value = vf.createBNode(in.getElementText());
                    } else if ("uri".equals(tag)) {
                        value = vf.createURI(in.getElementText());
                    } else if ("literal".equals(tag)) {
                        final String lang = in.getAttributeValue(null, "lang");
                        final String dt = in.getAttributeValue(null, "datatype");
                        final String label = in.getElementText();
                        value = lang != null ? vf.createLiteral(label, lang) : dt != null ? vf
                                .createLiteral(label, vf.createURI(dt)) : vf.createLiteral(label);
                    } else {
                        throw new XMLStreamException(
                                "Expected <bnode>, <uri> or <literal>, found <" + tag + ">");
                    }
                    values[index] = value;
                    leaveChild(in); // leave binding
                }
                leaveChild(in); // leave result

                if (values[0] instanceof Resource && values[1] instanceof URI && values[2] != null) {
                    final Resource s = (Resource) values[0];
                    final URI p = (URI) values[1];
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
