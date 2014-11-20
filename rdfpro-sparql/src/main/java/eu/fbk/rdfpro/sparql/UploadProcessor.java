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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Objects;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerBase;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;

public final class UploadProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UploadProcessor.class);

    private final String endpointURL;

    private final int chunkSize;

    static UploadProcessor doCreate(final String... args) {
        final Options options = Options.parse("s!|!", args);
        final Integer chunkSize = options.getOptionArg("s", Integer.class);
        final String endpointURL = options.getPositionalArg(0, String.class);
        return new UploadProcessor(endpointURL, chunkSize);
    }

    public UploadProcessor(final String endpointURL, @Nullable final Integer chunkSize) {
        this.endpointURL = endpointURL;
        this.chunkSize = chunkSize != null ? chunkSize : 1024;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        final RDFHandler uploader = Handlers.track(new Handler(), new Tracker(LOGGER, null, //
                "%d triples uploaded (%d tr/s avg)", //
                "%d triples uploaded (%d tr/s, %d tr/s avg)"));
        return Handlers.dispatchAll(sink, Handlers.ignoreExtraPasses(uploader));
    }

    private final class Handler extends HandlerBase {

        private static final String HEAD = "INSERT DATA {\n";

        private final StringBuilder builder;

        private Resource lastCtx;

        private Resource lastSubj;

        private URI lastPred;

        private int count;

        Handler() {
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
            if (this.count == UploadProcessor.this.chunkSize) {
                flush();
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            flush();
        }

        private void emit(final Value value) {
            try {
                Statements.formatValue(value, this.builder);
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

            final URL url = new URL(UploadProcessor.this.endpointURL);
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
                    throw new IOException("Upload to '" + UploadProcessor.this.endpointURL
                            + "' failed (HTTP " + httpCode + ")");
                }

            } finally {
                connection.disconnect();
            }
        }

    }

}
