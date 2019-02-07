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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Namespaces;

final class ProcessorPrefix implements RDFProcessor {

    private final Map<String, String> sourceNsToPrefixMap; // ns -> prefix map

    ProcessorPrefix(@Nullable final Map<String, String> nsToPrefixMap) {
        this.sourceNsToPrefixMap = nsToPrefixMap != null ? nsToPrefixMap //
                : Namespaces.DEFAULT.prefixMap();
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(Objects.requireNonNull(handler));
    }

    private final class Handler extends AbstractRDFHandlerWrapper {

        private Map<String, String> collectedNs; // emitted ns, used as set

        Handler(final RDFHandler handler) {
            super(handler);
            this.collectedNs = new ConcurrentHashMap<String, String>();
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            if (this.collectedNs.put(iri, iri) == null) {
                String p = ProcessorPrefix.this.sourceNsToPrefixMap.get(iri);
                if (p == null) {
                    p = prefix;
                }
                super.handleNamespace(p, iri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            emitNamespacesFor(statement.getSubject());
            emitNamespacesFor(statement.getPredicate());
            emitNamespacesFor(statement.getObject());
            emitNamespacesFor(statement.getContext());
            super.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            super.endRDF();
            this.collectedNs.clear();
        }

        @Override
        public void close() {
            super.close();
            this.collectedNs = null;
        }

        private void emitNamespacesFor(final Value value) throws RDFHandlerException {
            if (value instanceof IRI) {
                final IRI iri = (IRI) value;
                emitNamespace(iri.getNamespace());
            } else if (value instanceof Literal) {
                final Literal literal = (Literal) value;
                final IRI iri = literal.getDatatype();
                if (iri != null) {
                    emitNamespace(iri.getNamespace());
                }
            }
        }

        private void emitNamespace(final String namespace) throws RDFHandlerException {
            if (this.collectedNs.put(namespace, namespace) == null) {
                final String prefix = ProcessorPrefix.this.sourceNsToPrefixMap.get(namespace);
                if (prefix != null) {
                    super.handleNamespace(prefix, namespace);
                }
            }
        }

    }

}
