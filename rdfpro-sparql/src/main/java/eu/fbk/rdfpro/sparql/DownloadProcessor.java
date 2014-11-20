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
package eu.fbk.rdfpro.sparql;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.TupleQueryResultHandlerBase;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.QueryResultIO;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.ParseErrorLogger;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerBase;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;
import eu.fbk.rdfpro.util.Util;

public final class DownloadProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DownloadProcessor.class);

    private final boolean rewriteBNodes;

    private final String endpointURL;

    private final String query;

    private final boolean isSelect;

    static DownloadProcessor doCreate(final String... args) {
        final Options options = Options.parse("w|q!|f!|!", args);
        final boolean rewriteBNodes = options.hasOption("w");
        final String endpointURL = options.getPositionalArg(0, String.class);
        String query = options.getOptionArg("q", String.class);
        if (query == null) {
            final String source = options.getOptionArg("f", String.class);
            try {
                final File file = new File(source);
                URL url;
                if (file.exists()) {
                    url = file.toURI().toURL();
                } else {
                    url = DownloadProcessor.class.getClassLoader().getResource(source);
                }
                final BufferedReader reader = new BufferedReader(new InputStreamReader(
                        url.openStream()));
                try {
                    final StringBuilder builder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        builder.append(line);
                    }
                    query = builder.toString();
                } finally {
                    Util.closeQuietly(reader);
                }
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Cannot load SPARQL query from " + source
                        + ": " + ex.getMessage(), ex);
            }
        }
        return new DownloadProcessor(rewriteBNodes, endpointURL, query);
    }

    public DownloadProcessor(final boolean rewriteBNodes, final String endpointURL,
            final String query) {
        this.rewriteBNodes = rewriteBNodes;
        this.endpointURL = Util.checkNotNull(endpointURL);
        this.query = Util.checkNotNull(query);
        this.isSelect = isSelectQuery(query);
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        return new Handler(sink);
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

    private final class Handler extends HandlerBase {

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
            this.logSink = Handlers.track(this.sink, new Tracker(LOGGER, null, //
                    "%d triples parsed (%d tr/s avg)", //
                    "%d triples parsed (%d tr/s, %d tr/s avg)"));
            // TODO: different BNodes may be returned each time the query is evaluated;
            // to allow preserving their identities, we should store the query result and
            // read from it from disk the next times
            this.parseSink = Handlers.ignoreStartEnd(!rewrite ? this.logSink //
                    : Handlers.rewriteBNodes(this.logSink,
                            Util.murmur3str(DownloadProcessor.this.query)));
            this.latch = null;
            this.exception = null;
            this.halted = false;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.logSink.startRDF();
            this.latch = new CountDownLatch(1);
            Util.getPool().execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        final String threadName = Thread.currentThread().getName();
                        LOGGER.debug("Begin query in thread {}", threadName);
                        sendQuery();
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
        public void close() {
            this.halted = true; // will cause query result handlers to fail at next result
            Util.closeQuietly(this.sink);
        }

        private void checkNotFailed() throws RDFHandlerException {
            if (this.exception != null) {
                Util.propagateIfPossible(this.exception, RDFHandlerException.class);
                throw new RDFHandlerException(this.exception);
            }
        }

        private void sendQuery() throws Throwable {

            final List<String> acceptTypes;
            acceptTypes = DownloadProcessor.this.isSelect ? TupleQueryResultFormat.SPARQL
                    .getMIMETypes() : RDFFormat.RDFXML.getMIMETypes();

            final byte[] requestBody = ("query="
                    + URLEncoder.encode(DownloadProcessor.this.query, "UTF-8") + "&infer=true")
                    .getBytes(Charset.forName("UTF-8"));

            final URL url = new URL(DownloadProcessor.this.endpointURL);
            final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Accept", String.join(",", acceptTypes));
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
                    throw new IOException("Download from '" + DownloadProcessor.this.endpointURL
                            + "' failed (HTTP " + httpCode + ")");
                }

                if (DownloadProcessor.this.isSelect) {
                    final TupleQueryResultParser parser = QueryResultIO
                            .createParser(TupleQueryResultFormat.SPARQL);
                    parser.setQueryResultHandler(new TupleHandler());
                    parser.parseQueryResult(connection.getInputStream());

                } else {
                    final ParserConfig parserConfig = new ParserConfig();
                    parserConfig.addNonFatalError(BasicParserSettings.VERIFY_DATATYPE_VALUES);
                    parserConfig.addNonFatalError(BasicParserSettings.VERIFY_LANGUAGE_TAGS);

                    final RDFParser parser = Rio.createParser(RDFFormat.RDFXML,
                            Statements.VALUE_FACTORY);
                    parser.setParserConfig(parserConfig);
                    parser.setParseErrorListener(new ParseErrorLogger());
                    parser.setRDFHandler(new GraphHandler());
                    parser.parse(connection.getInputStream(), DownloadProcessor.this.endpointURL);
                }

            } finally {
                connection.disconnect();
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
                                handler.handleStatement(Statements.VALUE_FACTORY.createStatement(
                                        (Resource) subj, (URI) pred, obj));
                            } else if (ctx instanceof Resource) {
                                handler.handleStatement(Statements.VALUE_FACTORY.createStatement(
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

}
