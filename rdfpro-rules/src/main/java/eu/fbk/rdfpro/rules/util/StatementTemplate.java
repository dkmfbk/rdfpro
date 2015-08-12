package eu.fbk.rdfpro.rules.util;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

public final class StatementTemplate implements Function<Statement, Statement> {

    private final Object subj;

    private final Object pred;

    private final Object obj;

    @Nullable
    private final Object ctx;

    public StatementTemplate(final Object subj, final Object pred, final Object obj,
            @Nullable final Object ctx) {
        this.subj = check(subj);
        this.pred = check(pred);
        this.obj = check(obj);
        this.ctx = check(ctx);
    }

    public StatementTemplate(final StatementPattern head) {
        this.subj = componentFor(head.getSubjectVar());
        this.pred = componentFor(head.getPredicateVar());
        this.obj = componentFor(head.getObjectVar());
        this.ctx = componentFor(head.getContextVar());
    }

    public StatementTemplate(final StatementPattern head, final StatementPattern body) {
        this.subj = componentFor(head.getSubjectVar(), body);
        this.pred = componentFor(head.getPredicateVar(), body);
        this.obj = componentFor(head.getObjectVar(), body);
        this.ctx = componentFor(head.getContextVar(), body);
    }

    public StatementTemplate normalize(final Function<? super Value, ?> normalizer) {
        final Object nsubj = this.subj instanceof StatementComponent ? (Object) this.subj
                : normalizer.apply((Value) this.subj);
        final Object npred = this.pred instanceof StatementComponent ? (Object) this.pred
                : normalizer.apply((Value) this.pred);
        final Object nobj = this.obj instanceof StatementComponent ? (Object) this.obj
                : normalizer.apply((Value) this.obj);
        final Object nctx = this.ctx == null //
                || this.ctx instanceof StatementComponent ? (Object) this.ctx : normalizer
                .apply((Value) this.ctx);
        return nsubj == this.subj && npred == this.pred && nobj == this.obj && nctx == this.ctx ? this
                : new StatementTemplate(nsubj, npred, nobj, nctx);
    }

    @Override
    @Nullable
    public Statement apply(final Statement stmt) {

        final Object subj = resolve(this.subj, stmt);
        if (!(subj instanceof Resource)) {
            return null;
        }

        final Object pred = resolve(this.pred, stmt);
        if (!(pred instanceof URI)) {
            return null;
        }

        final Object obj = resolve(this.obj, stmt);
        if (!(obj instanceof Value)) {
            return null;
        }

        final Object ctx = resolve(this.ctx, stmt);
        if (ctx instanceof Resource) {
            return null;
        }

        return ctx == null ? Statements.VALUE_FACTORY.createStatement((Resource) subj, (URI) pred,
                (Value) obj) : Statements.VALUE_FACTORY.createStatement((Resource) subj,
                (URI) pred, (Value) obj, (Resource) ctx);
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof StatementTemplate)) {
            return false;
        }
        final StatementTemplate other = (StatementTemplate) object;
        return this.subj.equals(other.subj) && this.pred.equals(other.pred)
                && this.obj.equals(other.obj) && Objects.equals(this.ctx, other.ctx);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.subj, this.pred, this.obj, this.ctx);
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        toStringHelper(this.subj, builder);
        builder.append(' ');
        toStringHelper(this.obj, builder);
        builder.append(' ');
        toStringHelper(this.pred, builder);
        builder.append(' ');
        toStringHelper(this.ctx, builder);
        return builder.toString();
    }

    private void toStringHelper(final Object component, final StringBuilder builder) {
        if (component instanceof StatementComponent) {
            builder.append('?').append(((StatementComponent) component).getLetter());
        } else if (component == null) {
            builder.append("sesame:nil");
        } else {
            try {
                Statements.formatValue((Value) component, Namespaces.DEFAULT, builder);
            } catch (final IOException ex) {
                throw new Error(ex);
            }
        }
    }

    private static Object resolve(final Object component, final Statement stmt) {
        return component instanceof StatementComponent ? ((StatementComponent) component)
                .apply(stmt) : component;
    }

    private static Object check(final Object component) {
        if (!(component instanceof Value) && !(component instanceof StatementComponent)) {
            throw new IllegalArgumentException("Illegal component " + component);
        }
        return component;
    }

    private static Object componentFor(final Var var) {
        if (var == null) {
            return null;
        } else if (var.hasValue()) {
            return var.getValue();
        } else {
            switch (var.getName().toLowerCase()) {
            case "s":
                return StatementComponent.SUBJECT;
            case "p":
                return StatementComponent.PREDICATE;
            case "o":
                return StatementComponent.OBJECT;
            case "c":
                return StatementComponent.CONTEXT;
            default:
                throw new IllegalArgumentException("Could not extract component from " + var);
            }
        }
    }

    private static Object componentFor(final Var var, final StatementPattern body) {
        if (var == null) {
            return null;
        } else if (var.hasValue()) {
            return var.getValue();
        } else {
            final String name = var.getName();
            final Var subjVar = body.getSubjectVar();
            final Var predVar = body.getPredicateVar();
            final Var objVar = body.getObjectVar();
            final Var ctxVar = body.getContextVar();
            if (!subjVar.hasValue() && name.equals(subjVar.getName())) {
                return StatementComponent.SUBJECT;
            } else if (!predVar.hasValue() && name.equals(predVar.getName())) {
                return StatementComponent.PREDICATE;
            } else if (!objVar.hasValue() && name.equals(objVar.getName())) {
                return StatementComponent.OBJECT;
            } else if (ctxVar != null && !ctxVar.hasValue() && name.equals(ctxVar.getName())) {
                return StatementComponent.CONTEXT;
            }
            throw new IllegalArgumentException("Could not extract component from " + var);
        }
    }

}