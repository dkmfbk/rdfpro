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
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class RepositoryQuadModel extends QuadModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryQuadModel.class);

    private static final long serialVersionUID = 1L;

    private final RepositoryConnection connection;

    public RepositoryQuadModel(final RepositoryConnection connection) {
        this.connection = Objects.requireNonNull(connection);
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
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {
        try {
            return new RepositoryIterator(this.connection.getStatements(subj, pred, obj, false,
                    ctxs));
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doAdd(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            final long size = this.connection.size();
            this.connection.add(subj, pred, obj, ctxs);
            return this.connection.size() != size;
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            final long size = this.connection.size();
            this.connection.remove(subj, pred, obj, ctxs);
            return this.connection.size() != size;
        } catch (final RepositoryException ex) {
            throw new ModelException(ex);
        }
    }

    private static final class RepositoryIterator implements Iterator<Statement>, AutoCloseable {

        private final RepositoryResult<? extends Statement> iteration;

        RepositoryIterator(final RepositoryResult<? extends Statement> iteration) {
            this.iteration = iteration;
        }

        @Override
        public boolean hasNext() {
            try {
                return this.iteration.hasNext();
            } catch (final RepositoryException ex) {
                close();
                throw new ModelException(ex);
            }
        }

        @Override
        public Statement next() {
            try {
                return this.iteration.next();
            } catch (final RepositoryException ex) {
                close();
                throw new ModelException(ex);
            }
        }

        @Override
        public void close() {
            try {
                this.iteration.close();
            } catch (final Throwable ex) {
                LOGGER.error("Could not close iteration", ex);
            }
        }

    }

}
