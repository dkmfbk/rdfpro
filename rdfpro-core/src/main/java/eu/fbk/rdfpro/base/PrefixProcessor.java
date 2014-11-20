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
package eu.fbk.rdfpro.base;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;

public final class PrefixProcessor extends RDFProcessor {

    private final Map<String, String> nsToPrefixMap;

    static PrefixProcessor doCreate(final String... args) {
        final Options options = Options.parse("f!", args);
        final String source = options.getOptionArg("f", String.class);
        Map<String, String> nsToPrefixMap = null;
        if (source != null) {
            try {
                nsToPrefixMap = new HashMap<String, String>();
                URL url;
                final File file = new File(source);
                if (file.exists()) {
                    url = file.toURI().toURL();
                } else {
                    url = PrefixProcessor.class.getClassLoader().getResource(source);
                }
                Statements.parseNamespaces(url, nsToPrefixMap, null);
            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Cannot load prefix/namespace bindings from "
                        + source + ": " + ex.getMessage(), ex);
            }
        }
        return new PrefixProcessor(nsToPrefixMap);
    }

    public PrefixProcessor(@Nullable final Map<String, String> nsToPrefixMap) {
        this.nsToPrefixMap = nsToPrefixMap != null && nsToPrefixMap != Statements.NS_TO_PREFIX_MAP ? //
        new HashMap<String, String>(nsToPrefixMap)
                : Statements.NS_TO_PREFIX_MAP;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        return new Handler(sink, this.nsToPrefixMap);
    }

    private static final class Handler extends HandlerWrapper {

        private Map<String, String> sourceNsToPrefixMap; // ns -> prefix map

        private Map<String, String> collectedNsToPrefixMap; // emitted ns, used as set

        Handler(final RDFHandler handler, final Map<String, String> availableBindings) {
            super(handler);
            this.sourceNsToPrefixMap = availableBindings;
            this.collectedNsToPrefixMap = new ConcurrentHashMap<String, String>();
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            if (this.collectedNsToPrefixMap.put(uri, uri) == null) {
                String p = this.sourceNsToPrefixMap.get(uri);
                if (p == null) {
                    p = prefix;
                }
                super.handleNamespace(p, uri);
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
            this.collectedNsToPrefixMap.clear();
        }

        @Override
        public void close() {
            super.close();
            this.sourceNsToPrefixMap = null;
            this.collectedNsToPrefixMap = null;
        }

        private void emitNamespacesFor(final Value value) throws RDFHandlerException {
            if (value instanceof URI) {
                final String namespace = ((URI) value).getNamespace();
                if (this.collectedNsToPrefixMap.put(namespace, namespace) == null) {
                    final String prefix = this.sourceNsToPrefixMap.get(namespace);
                    if (prefix != null) {
                        super.handleNamespace(prefix, namespace);
                    }
                }
            }
        }

    }

}
