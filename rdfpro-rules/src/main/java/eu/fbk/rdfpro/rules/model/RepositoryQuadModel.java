package eu.fbk.rdfpro.rules.model;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.util.ModelException;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

import eu.fbk.rdfpro.rules.util.Iterators;
import eu.fbk.rdfpro.rules.util.SPARQLRenderer;

final class RepositoryQuadModel extends QuadModel implements AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final RepositoryConnection connection;

    private final boolean trackChanges;

    public RepositoryQuadModel(final RepositoryConnection connection, final boolean trackChanges) {
        this.connection = Objects.requireNonNull(connection);
        this.trackChanges = trackChanges;
    }

    @Override
    public void close() {
        Iterators.closeQuietly(this.connection);
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
            return name == null ? null : new NamespaceImpl(prefix, name);
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected Namespace doSetNamespace(final String prefix, @Nullable final String name) {
        try {
            final String oldName = this.connection.getNamespace(prefix);
            final Namespace oldNamespace = oldName == null ? null : new NamespaceImpl(prefix,
                    oldName);
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
    protected int doSize(@Nullable final Resource subj, @Nullable final URI pred,
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
    protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, @Nullable final Resource ctx) {
        return Integer.MAX_VALUE; // no way to efficiently estimate cardinality
    }

    @Override
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {
        try {
            return Iterators.forIteration(this.connection.getStatements(subj, pred, obj, false,
                    ctxs));
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doAdd(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            if (!this.trackChanges) {
                this.connection.add(subj, pred, obj, ctxs);
                return true;
            } else if (ctxs.length == 0) {
                if (this.connection.hasStatement(subj, pred, obj, false, new Resource[] { null })) {
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
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final URI pred,
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

        final String queryString = new SPARQLRenderer(null, true).render(expr, null);
        try {
            final TupleQuery query = this.connection.prepareTupleQuery(QueryLanguage.SPARQL,
                    queryString);
            query.setDataset(dataset);
            for (final Binding binding : bindings) {
                query.setBinding(binding.getName(), binding.getValue());
            }
            return Iterators.forIteration(query.evaluate());
        } catch (final QueryEvaluationException | MalformedQueryException | RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

}
