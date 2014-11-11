package eu.fbk.rdfpro;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.Objects;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class UploadProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UploadProcessor.class);

    private final String endpointURL;

    private final int chunkSize;

    public UploadProcessor(final String endpointURL, final int chunkSize) {
        this.endpointURL = endpointURL;
        this.chunkSize = chunkSize;
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler sink) {
        final RDFHandler uploader = Handlers.track(new Handler(), LOGGER, null,
                "%d triples uploaded (%d tr/s avg)", "8" + toString(),
                "%d triples uploaded (%d tr/s, %d tr/s avg)");
        return Handlers.duplicate(sink, Handlers.dropExtraPasses(uploader));
    }

    private final class Handler implements RDFHandler, Closeable {

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
        public void startRDF() throws RDFHandlerException {
            // ignored
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // ignored
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            // ignored
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

        @Override
        public void close() throws IOException {
        }

        private void emit(final Value value) {
            try {
                Values.formatValue(value, this.builder);
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
