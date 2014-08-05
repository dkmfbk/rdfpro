package eu.fbk.rdfpro;

import static org.openrdf.http.protocol.Protocol.ACCEPT_PARAM_NAME;

import java.io.Closeable;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.apache.commons.httpclient.HttpMethod;
import org.openrdf.http.client.HTTPClient;
import org.openrdf.http.protocol.UnauthorizedException;
import org.openrdf.http.protocol.error.ErrorInfo;
import org.openrdf.http.protocol.error.ErrorType;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryInterruptedException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.UnsupportedQueryLanguageException;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.UnsupportedRDFormatException;
import org.openrdf.rio.helpers.ParseErrorLogger;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.lang.FileFormat;

final class DownloadProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadProcessor.class);

    private final boolean rewriteBNodes;

    private final String endpointURL;

    private final String query;

    private final boolean isSelect;

    public DownloadProcessor(final boolean rewriteBNodes, final String endpointURL,
            final String query) {
        this.rewriteBNodes = rewriteBNodes;
        this.endpointURL = Util.checkNotNull(endpointURL);
        this.query = Util.checkNotNull(query);
        this.isSelect = isSelectQuery(query);
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        return new Handler(Util.checkNotNull(handler));
    }

    public static boolean isSelectQuery(final String string) {
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

    private final class Handler implements RDFHandler, Closeable {

        private final RDFHandler sink;

        private final RDFHandler logSink;

        private final RDFHandler parseSink;

        @Nullable
        private CountDownLatch latch;

        @Nullable
        private volatile Throwable exception;

        private volatile boolean halted;

        Handler(final RDFHandler handler) {
            final boolean rewrite = DownloadProcessor.this.rewriteBNodes;
            this.sink = handler;
            this.logSink = Handlers.track(this.sink, LOGGER, null, //
                    "%d triples parsed (%d tr/s avg)", "0" + toString(), //
                    "%d triples parsed (%d tr/s, %d tr/s avg)");
            // TODO: different BNodes may be returned each time the query is evaluated;
            // to allow preserving their identities, we should store the query result and
            // read from it from disk the next times
            this.parseSink = Handlers.dropStartEnd(!rewrite ? this.logSink //
                    : Handlers.rewriteBNodes(this.logSink,
                            Util.toString(Util.murmur3(DownloadProcessor.this.query))));
            this.latch = null;
            this.exception = null;
            this.halted = false;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.logSink.startRDF();
            this.latch = new CountDownLatch(1);
            Threads.getMainPool().execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        final String threadName = Thread.currentThread().getName();
                        LOGGER.debug("Begin query in thread {}", threadName);
                        evaluateQuery();
                        LOGGER.debug("Done query in thread {}", threadName);
                    } catch (final Throwable ex) {
                        if (!Handler.this.halted) {
                            Handler.this.exception = ex;
                        }
                    } finally {
                        Handler.this.latch.countDown();
                    }
                }

            });
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
        public void close() throws IOException {
            this.halted = true; // will cause query result handlers to fail at next result
            Util.closeQuietly(this.sink);
        }

        private void checkNotFailed() throws RDFHandlerException {
            if (this.exception != null) {
                Util.propagateIfPossible(this.exception, RDFHandlerException.class);
                throw new RDFHandlerException(this.exception);
            }
        }

        private void evaluateQuery() throws Throwable {

            final HTTPClient client = new Client();
            client.setQueryURL(DownloadProcessor.this.endpointURL);

            try {
                if (DownloadProcessor.this.isSelect) {
                    client.sendTupleQuery(QueryLanguage.SPARQL, DownloadProcessor.this.query,
                            null, null, true, 0, new TupleHandler());
                } else {
                    client.setPreferredTupleQueryResultFormat(TupleQueryResultFormat.SPARQL);
                    client.sendGraphQuery(QueryLanguage.SPARQL, DownloadProcessor.this.query,
                            null, null, true, 0, new GraphHandler());
                }

            } finally {
                client.shutDown();
            }
        }

        private class GraphHandler extends RDFHandlerBase {

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                if (Handler.this.halted) {
                    // Don't know other ways to interrupt a query...
                    throw new RDFHandlerException("query halted");
                }
                Handler.this.parseSink.handleStatement(statement);
            }

        }

        private class TupleHandler extends TupleQueryResultHandlerBase {

            private String subjVar;

            private String predVar;

            private String objVar;

            private String ctxVar;

            @Override
            public void startQueryResult(final List<String> names)
                    throws TupleQueryResultHandlerException {
                if (names.size() >= 3) {
                    this.subjVar = names.get(0);
                    this.predVar = names.get(1);
                    this.objVar = names.get(2);
                    this.ctxVar = names.size() >= 4 ? names.get(3) : null;
                }
            }

            @Override
            public void handleSolution(final BindingSet bindings)
                    throws TupleQueryResultHandlerException {
                if (Handler.this.halted) {
                    // Don't know other ways to interrupt a query...
                    throw new TupleQueryResultHandlerException("query halted");
                }
                if (this.subjVar != null) {
                    final Value subj = bindings.getValue(this.subjVar);
                    final Value pred = bindings.getValue(this.predVar);
                    final Value obj = bindings.getValue(this.objVar);
                    final Value ctx = this.ctxVar == null ? null : bindings.getValue(this.ctxVar);
                    if (subj instanceof Resource && pred instanceof URI && obj != null) {
                        try {
                            final RDFHandler handler = Handler.this.parseSink;
                            if (ctx == null) {
                                handler.handleStatement(Util.FACTORY.createStatement(
                                        (Resource) subj, (URI) pred, obj));
                            } else if (ctx instanceof Resource) {
                                handler.handleStatement(Util.FACTORY.createStatement(
                                        (Resource) subj, (URI) pred, obj, (Resource) ctx));
                            }
                        } catch (final RDFHandlerException ex) {
                            throw new TupleQueryResultHandlerException(ex);
                        }
                    }
                }
            }

        }

    }

    private static final class Client extends HTTPClient {

        @Override
        protected void getRDF(final HttpMethod method, final RDFHandler handler,
                final boolean requireContext) throws IOException, RDFHandlerException,
                RepositoryException, MalformedQueryException, UnauthorizedException,
                QueryInterruptedException {

            try {
                // Patched in order to accept only RDF/XML, as advertising support of multiple
                // formats seems to mislead Virtuoso, which then replies with Turtle but
                // Content-Type: text/plain (and btw, Turtle returned by Virtuoso is broken).
                final Set<RDFFormat> rdfFormats = new HashSet<RDFFormat>();
                rdfFormats.add(RDFFormat.RDFXML);
                for (final String acceptParam : RDFFormat.getAcceptParams(rdfFormats, false,
                        RDFFormat.RDFXML)) {
                    method.addRequestHeader(ACCEPT_PARAM_NAME, acceptParam);
                }

                final int httpCode = this.httpClient.executeMethod(method);

                if (httpCode != HttpURLConnection.HTTP_OK) {
                    try {
                        switch (httpCode) {
                        case HttpURLConnection.HTTP_UNAUTHORIZED: // 401
                            throw new UnauthorizedException();
                        case HttpURLConnection.HTTP_UNAVAILABLE: // 503
                            throw new QueryInterruptedException();
                        default:
                            final ErrorInfo errInfo = getErrorInfo(method);
                            if (errInfo.getErrorType() == ErrorType.MALFORMED_QUERY) {
                                throw new MalformedQueryException(errInfo.getErrorMessage());
                            } else if (errInfo.getErrorType() == ErrorType.UNSUPPORTED_QUERY_LANGUAGE) {
                                throw new UnsupportedQueryLanguageException(
                                        errInfo.getErrorMessage());
                            } else {
                                throw new RepositoryException(errInfo.getErrorMessage());
                            }
                        }
                    } finally {
                        method.abort();
                    }
                }

                final String mimeType = getResponseMIMEType(method);
                try {
                    final RDFFormat format = FileFormat.matchMIMEType(mimeType, rdfFormats);
                    final RDFParser parser = Rio.createParser(format, getValueFactory());
                    parser.setParserConfig(getParserConfig());
                    parser.setParseErrorListener(new ParseErrorLogger());
                    parser.setRDFHandler(handler);
                    parser.parse(method.getResponseBodyAsStream(), method.getURI().getURI());
                } catch (final UnsupportedRDFormatException e) {
                    throw new RepositoryException(
                            "Server responded with an unsupported file format: " + mimeType);
                } catch (final RDFParseException e) {
                    throw new RepositoryException("Malformed query result from server", e);
                }

            } finally {
                releaseConnection(method);
            }
        }
        //
        // @Override
        // protected String getResponseMIMEType(final HttpMethod method) throws IOException {
        // final Header[] headers = method.getResponseHeaders("Content-Type");
        // for (final Header header : headers) {
        // final HeaderElement[] headerElements = header.getElements();
        //
        // for (final HeaderElement headerEl : headerElements) {
        // final String mimeType = headerEl.getName();
        // if (mimeType != null) {
        // LOGGER.debug("Response type is {}", mimeType);
        // // Virtuoso sends turtle with Content-Type: text/plain, which triggers the
        // // use of NTriples parser and subsequent failure. We thus replace
        // // text/plain with text/turtle, exploiting the fact that Turtle is a
        // // superset of NTriples.
        // return mimeType.equals("text/plain") ? "text/turtle" : mimeType;
        // }
        // }
        // }
        // return null;
        // }

    }

}
