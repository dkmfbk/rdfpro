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

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.ModelException;

final class QuadModelModelAdapter extends QuadModel implements AutoCloseable {

    private static final long serialVersionUID = 1L;

    private final Model model;

    QuadModelModelAdapter(final Model model) {
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public void close() throws Exception {
        IO.closeQuietly(this.model);
    }

    @Override
    protected Set<Namespace> doGetNamespaces() {
        return this.model.getNamespaces();
    }

    @Override
    protected Namespace doGetNamespace(final String prefix) {
        return this.model.getNamespace(prefix).orElse(null);
    }

    @Override
    protected Namespace doSetNamespace(final String prefix, @Nullable final String name) {
        if (name == null) {
            return this.model.removeNamespace(prefix).orElse(null);
        } else {
            final Namespace namespace = this.model.getNamespace(prefix).orElse(null);
            this.model.setNamespace(prefix, name);
            return namespace;
        }
    }

    @Override
    protected int doSize(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        return subj == null && pred == null && obj == null && ctxs.length == 0 ? this.model.size()
                : this.model.filter(subj, pred, obj, ctxs).size();
    }

    @Override
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs) {
        return this.model.filter(subj, pred, obj, ctxs).iterator();
    }

    @Override
    protected boolean doAdd(final Resource subj, final IRI pred, final Value obj,
            final Resource[] ctxs) {
        if (ctxs.length == 0) {
            return this.doAddHelper(subj, pred, obj, QuadModel.CTX_DEFAULT);
        } else if (ctxs.length == 1) {
            return this.doAddHelper(subj, pred, obj, ctxs);
        } else {
            boolean modified = false;
            for (final Resource ctx : ctxs) {
                final boolean m = this.doAddHelper(subj, pred, obj, new Resource[] { ctx });
                modified |= m;
            }
            return modified;
        }
    }

    private boolean doAddHelper(final Resource subj, final IRI pred, final Value obj,
            final Resource[] ctx) {
        final boolean added = this.model.add(subj, pred, obj, ctx);
        if (!added) {
            if (this.model.filter(subj, pred, obj, ctx).isEmpty()) {
                throw new ModelException("Model already contains statement "
                        + Statements.VALUE_FACTORY.createStatement(subj, pred, obj, ctx[0]));
            }
            return false;
        }
        return true;
    }

    @Override
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {
        return this.model.remove(subj, pred, obj, ctxs);
    }

}
