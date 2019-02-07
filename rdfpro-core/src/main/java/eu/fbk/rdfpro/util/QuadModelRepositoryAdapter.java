/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2015 by Francesco Corcoglioniti with support by Alessio Palmero Aprosio and Marco
 * Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.ModelException;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.RepositoryResult;

final class QuadModelRepositoryAdapter extends QuadModel implements AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final RepositoryConnection connection;

    private final boolean trackChanges;

    QuadModelRepositoryAdapter(final RepositoryConnection connection, final boolean trackChanges) {
        this.connection = Objects.requireNonNull(connection);
        this.trackChanges = trackChanges;
    }

    @Override
    public void close() {
        IO.closeQuietly(this.connection);
    }

    @Override
    protected Set<Namespace> doGetNamespaces() {
        try {
            final Set<Namespace> namespaces = new HashSet<>();
            RepositoryResult<? extends Namespace> iteration;
            iteration = this.connection.getNamespaces();
            try {
                while (iteration.hasNext()) {
                    namespaces.add(iteration.next());
                }
            } finally {
                iteration.close();
            }
            return namespaces;
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    @Nullable
    protected Namespace doGetNamespace(final String prefix) {
        try {
            final String name = this.connection.getNamespace(prefix);
            return name == null ? null : new SimpleNamespace(prefix, name);
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected Namespace doSetNamespace(final String prefix, @Nullable final String name) {
        try {
            final String oldName = this.connection.getNamespace(prefix);
            final Namespace oldNamespace = oldName == null ? null
                    : new SimpleNamespace(prefix, oldName);
            if (name == null) {
                this.connection.removeNamespace(prefix);
            } else {
                this.connection.setNamespace(prefix, name);
            }
            return oldNamespace;
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected int doSize(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            if (subj == null && pred == null && obj == null) {
                return (int) this.connection.size(ctxs);
            } else {
                int size = 0;
                RepositoryResult<? extends Statement> iteration;
                iteration = this.connection.getStatements(subj, pred, obj, false, ctxs);
                try {
                    while (iteration.hasNext()) {
                        iteration.next();
                        ++size;
                    }
                } finally {
                    iteration.close();
                }
                return size;
            }
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, @Nullable final Resource ctx) {
        return Integer.MAX_VALUE; // no way to efficiently estimate cardinality
    }

    @Override
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs) {
        try {
            return Iterators
                    .forIteration(this.connection.getStatements(subj, pred, obj, false, ctxs));
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doAdd(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            if (!this.trackChanges) {
                this.connection.add(subj, pred, obj, ctxs);
                return true;
            } else if (ctxs.length == 0) {
                if (this.connection.hasStatement(subj, pred, obj, false,
                        new Resource[] { null })) {
                    return false;
                }
                this.connection.add(subj, pred, obj, ctxs);
                return true;
            } else if (ctxs.length == 1) {
                if (this.connection.hasStatement(subj, pred, obj, false, ctxs)) {
                    return false;
                }
                this.connection.add(subj, pred, obj, ctxs);
                return true;
            } else {
                boolean modified = false;
                for (final Resource ctx : ctxs) {
                    final Resource[] singletonCtxs = new Resource[] { ctx };
                    if (!this.connection.hasStatement(subj, pred, obj, false, singletonCtxs)) {
                        this.connection.add(subj, pred, obj, singletonCtxs);
                        modified = true;
                    }
                }
                return modified;
            }
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            if (!this.trackChanges) {
                this.connection.remove(subj, pred, obj, ctxs);
                return true;
            } else {
                if (!this.connection.hasStatement(subj, pred, obj, false, ctxs)) {
                    return false;
                }
                this.connection.remove(subj, pred, obj, ctxs);
                return true;
            }
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected Iterator<BindingSet> doEvaluate(final TupleExpr expr, final Dataset dataset,
            final BindingSet bindings) {

        final String queryString = Algebra.renderQuery(expr, null, null, true);
        try {
            final TupleQuery query = this.connection.prepareTupleQuery(QueryLanguage.SPARQL,
                    queryString);
            query.setDataset(dataset);
            for (final Binding binding : bindings) {
                query.setBinding(binding.getName(), binding.getValue());
            }
            return Iterators.forIteration(query.evaluate());
        } catch (final QueryEvaluationException | MalformedQueryException
                | RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

}
