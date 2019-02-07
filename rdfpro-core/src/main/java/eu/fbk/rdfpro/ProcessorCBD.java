package eu.fbk.rdfpro;

import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

public class ProcessorCBD implements RDFProcessor {

    private final Set<String> namespaces;

    private final boolean symmetric;

    private final boolean includeInstances;

    private final boolean includeContexts;

    ProcessorCBD(@Nullable final Iterable<String> namespaces, final boolean symmetric,
            final boolean includeInstances, final boolean includeContexts) {
        this.namespaces = namespaces == null ? ImmutableSet.of() : ImmutableSet.copyOf(namespaces);
        this.symmetric = symmetric;
        this.includeInstances = includeInstances;
        this.includeContexts = includeContexts;
    }

    private boolean match(final Value value) {
        return value instanceof IRI && this.namespaces.contains(((IRI) value).getNamespace());
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(handler);
    }

    private final class Handler extends AbstractRDFHandlerWrapper {

        private final Multimap<Value, Value> bnodeNeighbours;

        private final Multimap<Value, Statement> bnodeStatements;

        private final Set<Value> bnodeSelection;

        public Handler(final RDFHandler sink) {
            super(sink);
            this.bnodeNeighbours = HashMultimap.create();
            this.bnodeStatements = HashMultimap.create();
            this.bnodeSelection = Sets.newHashSet();
        }

        @Override
        public void handleStatement(final Statement stmt) throws RDFHandlerException {

            final boolean symmetric = ProcessorCBD.this.symmetric;
            final boolean includeInstances = ProcessorCBD.this.includeInstances;
            final boolean includeContexts = ProcessorCBD.this.includeContexts;

            final Resource s = stmt.getSubject();
            final IRI p = stmt.getPredicate();
            final Value o = stmt.getObject();
            final Resource c = stmt.getContext();

            final boolean sb = s instanceof BNode;
            final boolean ob = o instanceof BNode;

            boolean includeInv = false;
            if (symmetric || includeInstances) {
                final boolean pt = p.equals(RDF.TYPE);
                includeInv = symmetric && !pt || includeInstances && pt;
            }

            boolean include = match(s) || includeInv && match(o) || includeContexts && match(c);

            if (!sb && !ob) {
                if (include) {
                    this.handler.handleStatement(stmt);
                }

            } else {
                synchronized (this) {
                    include |= sb && this.bnodeSelection.contains(s);
                    include |= ob && includeInv && this.bnodeSelection.contains(o);

                    if (include) {
                        this.handler.handleStatement(stmt);
                        if (sb) {
                            includeBNode(s);
                        }
                        if (ob) {
                            includeBNode(o);
                        }
                    } else {
                        if (sb) {
                            this.bnodeStatements.put(s, stmt);
                            if (ob) {
                                this.bnodeNeighbours.put(s, o);
                            }
                        }
                        if (ob && includeInv) {
                            this.bnodeStatements.put(o, stmt);
                            if (sb) {
                                this.bnodeNeighbours.put(o, s);
                            }
                        }
                    }
                }
            }
        }

        private void includeBNode(final Value bnode) {

            if (!this.bnodeSelection.add(bnode)) {
                return;
            }

            for (final Statement stmt : this.bnodeStatements.removeAll(bnode)) {
                this.handler.handleStatement(stmt);
                final Resource s = stmt.getSubject();
                final Value o = stmt.getObject();
                if (s instanceof BNode && !s.equals(bnode)) {
                    this.bnodeStatements.remove(s, stmt);
                }
                if (o instanceof BNode && !o.equals(bnode)) {
                    this.bnodeStatements.remove(o, stmt);
                }
            }

            for (final Value neighbour : this.bnodeNeighbours.removeAll(bnode)) {
                includeBNode(neighbour);
            }
        }

    }

}
