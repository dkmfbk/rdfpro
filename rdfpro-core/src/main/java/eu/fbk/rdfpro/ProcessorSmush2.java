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

import java.util.Comparator;
import java.util.Objects;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;

import eu.fbk.rdfpro.util.Dictionary;
import eu.fbk.rdfpro.util.StatementDeduplicator;
import eu.fbk.rdfpro.util.StatementDeduplicator.ComparisonMethod;
import eu.fbk.rdfpro.util.Statements;

final class ProcessorSmush2 implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorSmush2.class);

    private final String[] rankedNamespaces;

    private final boolean emitSameAs;

    @Nullable
    private Int2IntMap loadedMappings;

    @Nullable
    private Dictionary loadedDictionary;

    ProcessorSmush2(@Nullable final RDFSource sameAsSource, final boolean emitSameAs,
            final String... rankedNamespaces) {

        this.rankedNamespaces = rankedNamespaces.clone();
        this.emitSameAs = emitSameAs;

        if (sameAsSource == null) {
            this.loadedMappings = null;
            this.loadedDictionary = null;

        } else {
            this.loadedMappings = new Int2IntOpenHashMap();
            this.loadedDictionary = Dictionary.newMemoryDictionary();
            sameAsSource.forEach(stmt -> {
                final Resource s = stmt.getSubject();
                final IRI p = stmt.getPredicate();
                final Value o = stmt.getObject();
                if (p.equals(OWL.SAMEAS) && o instanceof Resource && !s.equals(o)) {
                    final int cs = this.loadedDictionary.encode(s);
                    final int co = this.loadedDictionary.encode(o);
                    final int csOld = this.loadedMappings.put(cs, cs);
                    final int coOld = this.loadedMappings.put(co, cs);
                    final Resource error = csOld != 0 && csOld != cs ? s
                            : coOld != 0 && coOld != cs ? (Resource) o : null;
                    if (error != null) {
                        throw new IllegalArgumentException(
                                "Incompatible owl:sameAs statements for " + s);
                    }
                }
            });
        }
    }

    @Override
    public int getExtraPasses() {
        return this.loadedMappings == null ? 1 : 0;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(Objects.requireNonNull(handler));
    }

    private static int getOrDefault(final Int2IntMap map, final int key, final int defaultValue) {
        final int value = map.get(key);
        return value != 0 ? value : defaultValue;
    }

    private static Statement createStatement(final Resource subj, final IRI pred, final Value obj,
            @Nullable final Resource ctx) {
        return ctx == null ? Statements.VALUE_FACTORY.createStatement(subj, pred, obj) //
                : Statements.VALUE_FACTORY.createStatement(subj, pred, obj, ctx);
    }

    private final class Handler extends AbstractRDFHandlerWrapper {

        private boolean indexingPass;

        private Int2IntMap mappings;

        private Dictionary dictionary;

        private StatementDeduplicator deduplicator;

        Handler(final RDFHandler handler) {
            super(handler);
            this.indexingPass = ProcessorSmush2.this.loadedMappings == null;
            this.mappings = this.indexingPass ? new Int2IntOpenHashMap()
                    : ProcessorSmush2.this.loadedMappings;
            this.dictionary = this.indexingPass ? Dictionary.newMemoryDictionary()
                    : ProcessorSmush2.this.loadedDictionary;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            if (!this.indexingPass) {
                super.startRDF();
                this.deduplicator = ProcessorSmush2.this.emitSameAs ? StatementDeduplicator
                        .newPartialDeduplicator(ComparisonMethod.EQUALS, 16 * 1024) : null;
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            if (!this.indexingPass) {
                super.handleComment(comment);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            if (!this.indexingPass) {
                super.handleNamespace(prefix, iri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            final Resource s = statement.getSubject();
            final IRI p = statement.getPredicate();
            final Value o = statement.getObject();
            final Resource c = statement.getContext();

            final boolean isSameAs = p.equals(OWL.SAMEAS) && o instanceof Resource;

            if (!this.indexingPass) {
                final Resource cn = c == null ? null : rewrite(c, true, null);
                final Resource sn = rewrite(s, false, cn);
                final IRI pn = (IRI) rewrite(p, false, cn);
                final Value on = o instanceof Literal ? o : rewrite((Resource) o, false, cn);
                if (sn == s && pn == p && on == o && cn == c) {
                    super.handleStatement(statement);
                } else if (!isSameAs || !sn.equals(on)) {
                    super.handleStatement(createStatement(sn, pn, on, cn));
                }
            } else if (isSameAs && !s.equals(o)) {
                link(s, (Resource) o);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (!this.indexingPass) {
                super.endRDF();
                this.deduplicator = null;
            } else {
                normalize();
                this.indexingPass = false;
            }
        }

        @Override
        public void close() {
            super.close();
            this.mappings = null; // eagerly release memory
            this.dictionary = null; // eagerly release memory
        }

        private void link(final Resource resource1, final Resource resource2) {
            final int code1 = this.dictionary.encode(resource1);
            final int code2 = this.dictionary.encode(resource2);
            if (code1 == 0 || code2 == 0) {
                throw new Error();
            }
            synchronized (this) {
                final int nextCode1 = getOrDefault(this.mappings, code1, code1);
                for (int c = nextCode1; c != code1; c = getOrDefault(this.mappings, c, c)) {
                    if (c == code2) {
                        return; // already linked
                    }
                }
                final int nextCode2 = getOrDefault(this.mappings, code2, code2);
                this.mappings.put(code1, nextCode2);
                this.mappings.put(code2, nextCode1);
            }
        }

        private void normalize() throws RDFHandlerException {
            final Comparator<Value> comparator = Statements.valueComparator(true,
                    ProcessorSmush2.this.rankedNamespaces);
            int numClusters = 0;
            int numResources = 0;
            outer: for (final IntIterator i = this.mappings.keySet().iterator(); i.hasNext();) {
                final int code = i.nextInt();
                int chosenCode = 0;
                Resource chosenResource = null;
                int c = code;
                do {
                    final int cn = getOrDefault(this.mappings, c, c);
                    if (cn == c) {
                        continue outer; // cluster already normalized
                    }
                    final Resource resource = (Resource) this.dictionary.decode(c);
                    if (chosenCode == 0 || comparator.compare(resource, chosenResource) < 0) {
                        chosenResource = resource;
                        chosenCode = c;
                    }
                    c = cn;
                } while (c != code);
                ++numClusters;
                c = code;
                do {
                    final int cn = getOrDefault(this.mappings, c, c);
                    this.mappings.put(c, chosenCode);
                    ++numResources;
                    c = cn;
                } while (c != code);
            }
            if (ProcessorSmush2.LOGGER.isInfoEnabled()) {
                ProcessorSmush2.LOGGER.info(String.format(
                        "owl:sameAs normalization: %d resource(s), %d cluster(s), dictionary: %s",
                        numResources, numClusters, this.dictionary));
            }
        }

        private Resource rewrite(final Resource resource, final boolean useResourceAsCtx,
                @Nullable final Resource ctx) {
            final int c = this.dictionary.encode(resource, false);
            if (c != 0) {
                final int cm = this.mappings.get(c);
                if (cm != 0 && cm != c) {
                    final Resource m = (Resource) this.dictionary.decode(cm);
                    final Resource ctxm = useResourceAsCtx ? m : ctx;
                    if (ProcessorSmush2.this.emitSameAs && !m.equals(resource)
                            && this.deduplicator.add(m, OWL.SAMEAS, resource, ctxm)) {
                        super.handleStatement(createStatement(m, OWL.SAMEAS, resource, ctxm));
                    }
                    return m;
                }
            }
            return resource;
        }

    }

}
