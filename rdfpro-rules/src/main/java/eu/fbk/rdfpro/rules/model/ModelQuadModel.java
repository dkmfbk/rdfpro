package eu.fbk.rdfpro.rules.model;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.Model;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.util.ModelException;

import eu.fbk.rdfpro.rules.util.Iterators;

final class ModelQuadModel extends QuadModel implements AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final Model model;

    ModelQuadModel(final Model model) {
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void close() throws Exception {
        Iterators.closeQuietly(this.model);
    }

    @Override
    protected Set<Namespace> doGetNamespaces() {
        return this.model.getNamespaces();
    }

    @Override
    protected Namespace doGetNamespace(final String prefix) {
        return this.model.getNamespace(prefix);
    }

    @Override
    protected Namespace doSetNamespace(final String prefix, @Nullable final String name) {
        if (name == null) {
            return this.model.removeNamespace(prefix);
        } else {
            final Namespace namespace = this.model.getNamespace(prefix);
            this.model.setNamespace(prefix, name);
            return namespace;
        }
    }

    @Override
    protected int doSize(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        return subj == null && pred == null && obj == null && ctxs.length == 0 ? this.model.size()
                : this.model.filter(subj, pred, obj, ctxs).size();
    }

    @Override
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {
        return this.model.filter(subj, pred, obj, ctxs).iterator();
    }

    @Override
    protected boolean doAdd(final Resource subj, final URI pred, final Value obj,
            final Resource[] ctxs) {
        if (ctxs.length == 0) {
            return doAddHelper(subj, pred, obj, CTX_DEFAULT);
        } else if (ctxs.length == 1) {
            return doAddHelper(subj, pred, obj, ctxs);
        } else {
            boolean modified = false;
            for (final Resource ctx : ctxs) {
                final boolean m = doAddHelper(subj, pred, obj, new Resource[] { ctx });
                modified |= m;
            }
            return modified;
        }
    }

    private boolean doAddHelper(final Resource subj, final URI pred, final Value obj,
            final Resource[] ctx) {
        final boolean added = this.model.add(subj, pred, obj, ctx);
        if (!added) {
            if (this.model.filter(subj, pred, obj, ctx).isEmpty()) {
                throw new ModelException("Model already contains statement "
                        + new ContextStatementImpl(subj, pred, obj, ctx[0]));
            }
            return false;
        }
        return true;
    }

    @Override
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        return this.model.remove(subj, pred, obj, ctxs);
    }

}
