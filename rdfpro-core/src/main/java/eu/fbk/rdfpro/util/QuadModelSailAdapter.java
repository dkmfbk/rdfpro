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

import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.util.ModelException;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;

import info.aduna.iteration.CloseableIteration;

final class QuadModelSailAdapter extends QuadModel implements AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final SailConnection connection;

    private final boolean trackChanges;

    private long addCounter;

    private long removeCounter;

    QuadModelSailAdapter(final SailConnection connection, final boolean trackChanges) {
        this.connection = Objects.requireNonNull(connection);
        this.trackChanges = trackChanges;
        if (trackChanges && connection instanceof NotifyingSailConnection) {
            this.addCounter = 0;
            this.removeCounter = 0;
            ((NotifyingSailConnection) connection)
                    .addConnectionListener(new SailConnectionListener() {

                        @Override
                        public void statementAdded(final Statement stmt) {
                            ++QuadModelSailAdapter.this.addCounter;
                        }

                        @Override
                        public void statementRemoved(final Statement stmt) {
                            ++QuadModelSailAdapter.this.removeCounter;
                        }

                    });
        } else {
            this.addCounter = -1;
            this.removeCounter = -1;
        }
    }

    @Override
    public void close() {
        IO.closeQuietly(this.connection);
    }

    @Override
    protected Set<Namespace> doGetNamespaces() {
        try {
            final Set<Namespace> namespaces = new HashSet<>();
            CloseableIteration<? extends Namespace, SailException> iteration;
            iteration = this.connection.getNamespaces();
            try {
                while (iteration.hasNext()) {
                    namespaces.add(iteration.next());
                }
            } finally {
                iteration.close();
            }
            return namespaces;
        } catch (final SailException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    @Nullable
    protected Namespace doGetNamespace(final String prefix) {
        try {
            final String name = this.connection.getNamespace(prefix);
            return name == null ? null : new NamespaceImpl(prefix, name);
        } catch (final SailException ex) {
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
        } catch (final SailException ex) {
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
                CloseableIteration<? extends Statement, SailException> iteration;
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
        } catch (final SailException ex) {
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
        } catch (final SailException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doAdd(final Resource subj, final URI pred, final Value obj,
            final Resource[] ctxs) {
        try {
            if (!this.trackChanges) {
                this.connection.addStatement(subj, pred, obj, ctxs);
                return true;
            } else if (this.addCounter >= 0) {
                final long addCounterBefore = this.addCounter;
                this.connection.addStatement(subj, pred, obj, ctxs);
                return this.addCounter > addCounterBefore;
            } else {
                final Resource[] queryCtxs = ctxs.length > 0 ? ctxs : new Resource[] { null };
                int count = 0;
                CloseableIteration<? extends Statement, SailException> iteration;
                iteration = this.connection.getStatements(subj, pred, obj, false, queryCtxs);
                try {
                    while (iteration.hasNext()) {
                        iteration.next();
                        ++count;
                    }
                } finally {
                    iteration.close();
                }
                if (count >= ctxs.length) {
                    return false;
                }
                this.connection.addStatement(subj, pred, obj, ctxs);
                return true;
            }
        } catch (final SailException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        try {
            if (!this.trackChanges) {
                this.connection.removeStatements(subj, pred, obj, ctxs);
                return true;
            } else if (this.removeCounter >= 0) {
                final long removeCounterBefore = this.removeCounter;
                this.connection.removeStatements(subj, pred, obj, ctxs);
                return this.removeCounter != removeCounterBefore;
            } else {
                CloseableIteration<? extends Statement, SailException> iteration;
                iteration = this.connection.getStatements(subj, pred, obj, false, ctxs);
                try {
                    if (!iteration.hasNext()) {
                        return false;
                    }
                } finally {
                    iteration.close();
                }
                this.connection.removeStatements(subj, pred, obj, ctxs);
                return true;
            }
        } catch (final SailException ex) {
            throw new ModelException(ex);
        }
    }

    @Override
    protected Iterator<BindingSet> doEvaluate(final TupleExpr expr,
            @Nullable final Dataset dataset, @Nullable BindingSet bindings) {
        try {
            bindings = bindings != null ? bindings : EmptyBindingSet.getInstance();
            return Iterators
                    .forIteration(this.connection.evaluate(expr, dataset, bindings, false));
        } catch (final SailException ex) {
            throw new ModelException(ex);
        }
    }

}
