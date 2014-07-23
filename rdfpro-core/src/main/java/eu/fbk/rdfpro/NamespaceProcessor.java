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
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

final class NamespaceProcessor extends RDFProcessor {

    private final Map<String, String> namespaces;

    public NamespaceProcessor(final Map<String, String> namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        return new Handler(handler, this.namespaces);
    }

    private static final class Handler implements RDFHandler, Closeable {

        private final RDFHandler handler;

        private Map<String, String> namespaces; // ns -> prefix map

        private Map<String, String> processed; // emitted ns, used as set

        Handler(final RDFHandler handler, final Map<String, String> availableBindings) {
            this.handler = handler;
            this.namespaces = availableBindings;
            this.processed = new ConcurrentHashMap<String, String>();
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            if (this.processed.put(uri, uri) == null) {
                String p = this.namespaces.get(uri);
                if (p == null) {
                    p = prefix;
                }
                this.handler.handleNamespace(p, uri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            emitNamespacesFor(statement.getSubject());
            emitNamespacesFor(statement.getPredicate());
            emitNamespacesFor(statement.getObject());
            emitNamespacesFor(statement.getContext());
            this.handler.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.handler.endRDF();
            this.processed.clear();
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
            this.namespaces = null;
            this.processed = null;
        }

        private void emitNamespacesFor(final Value value) throws RDFHandlerException {
            if (value instanceof URI) {
                final String namespace = ((URI) value).getNamespace();
                if (this.processed.put(namespace, namespace) == null) {
                    final String prefix = this.namespaces.get(namespace);
                    if (prefix != null) {
                        this.handler.handleNamespace(prefix, namespace);
                    }
                }
            }
        }

    }

}
