package eu.fbk.rdfpro;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

import org.openrdf.http.client.HTTPClient;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.QueryLanguage;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.ntriples.NTriplesUtil;
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

        private final HTTPClient client;

        private final StringBuilder builder;

        private Resource lastCtx;

        private Resource lastSubj;

        private URI lastPred;

        private int count;

        Handler() {
            this.client = new HTTPClient();
            this.builder = new StringBuilder();
            this.lastCtx = null;
            this.lastSubj = null;
            this.lastPred = null;
            this.count = 0;

            this.client.setUpdateURL(UploadProcessor.this.endpointURL);
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
        public void handleStatement(final Statement statement) throws RDFHandlerException {

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
            this.client.shutDown();
        }

        private void emit(final Value value) {
            this.builder.append(NTriplesUtil.toNTriplesString(value));
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
                    this.client.sendUpdate(QueryLanguage.SPARQL, update, null, null, false);
                } catch (final Throwable ex) {
                    throw new RDFHandlerException(ex);
                }
            }
        }

    }

}
