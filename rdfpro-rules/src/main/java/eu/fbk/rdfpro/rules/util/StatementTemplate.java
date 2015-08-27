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
import eu.fbk.rdfpro.util.StatementDeduplicator;
import eu.fbk.rdfpro.util.Statements;

public final class StatementTemplate implements Function<Statement, Statement> {

    private final Object subj;

    private final Object pred;

    private final Object obj;

    @Nullable
    private final Object ctx;

    private final byte subjIndex;

    private final byte predIndex;

    private final byte objIndex;

    private final byte ctxIndex;

    public StatementTemplate(final Object subj, final Object pred, final Object obj,
            @Nullable final Object ctx) {

        this.subj = check(subj);
        this.pred = check(pred);
        this.obj = check(obj);
        this.ctx = check(ctx);

        this.subjIndex = this.subj instanceof Resource ? 4 : ((StatementComponent) this.subj)
                .getIndex();
        this.predIndex = this.pred instanceof Resource ? 5 : ((StatementComponent) this.pred)
                .getIndex();
        this.objIndex = this.obj instanceof Resource ? 6 : ((StatementComponent) this.obj)
                .getIndex();
        this.ctxIndex = this.ctx == null || this.ctx instanceof Resource ? 7
                : ((StatementComponent) this.ctx).getIndex();
    }

    public StatementTemplate(final StatementPattern head) {
        this(componentFor(head.getSubjectVar()), componentFor(head.getPredicateVar()),
                componentFor(head.getObjectVar()), componentFor(head.getContextVar()));
    }

    public StatementTemplate(final StatementPattern head, final StatementPattern body) {
        this(componentFor(head.getSubjectVar(), body), componentFor(head.getPredicateVar(), body),
                componentFor(head.getObjectVar(), body), componentFor(head.getContextVar(), body));
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
    public Statement apply(final Statement stmt) {
        try {
            final URI p = (URI) resolve(stmt, this.predIndex);
            final Resource s = (Resource) resolve(stmt, this.subjIndex);
            final Resource c = (Resource) resolve(stmt, this.ctxIndex);
            final Value o = (Value) resolve(stmt, this.objIndex);
            return Statements.VALUE_FACTORY.createStatement(s, p, o, c);
        } catch (final Throwable ex) {
            return null;
        }
    }

    public Statement apply(final Statement stmt, final StatementDeduplicator deduplicator) {
        try {
            final URI p = (URI) resolve(stmt, this.predIndex);
            final Resource s = (Resource) resolve(stmt, this.subjIndex);
            final Resource c = (Resource) resolve(stmt, this.ctxIndex);
            final Value o = (Value) resolve(stmt, this.objIndex);
            if (deduplicator.add(s, p, o, c)) {
                return Statements.VALUE_FACTORY.createStatement(s, p, o, c);
            }
        } catch (final Throwable ex) {
        }
        return null;
    }

    private Object resolve(final Statement stmt, final byte index) {
        switch (index) {
        case 0:
            return stmt.getSubject();
        case 1:
            return stmt.getPredicate();
        case 2:
            return stmt.getObject();
        case 3:
            return stmt.getContext();
        case 4:
            return this.subj;
        case 5:
            return this.pred;
        case 6:
            return this.obj;
        case 7:
            return this.ctx;
        default:
            throw new Error();
        }
    }

    @Nullable
    public Statement apply2(final Statement stmt) {

        // NOTE: although not very elegant, the code below runs faster than the much compact code
        // commented at the end of the method (and this is a hot spot)

        final Resource ss = stmt.getSubject();
        final URI sp = stmt.getPredicate();
        final Value so = stmt.getObject();
        final Resource sc = stmt.getContext();

        URI p;
        if (this.pred instanceof URI) {
            p = (URI) this.pred;
        } else if (this.pred == StatementComponent.SUBJECT && ss instanceof URI) {
            p = (URI) ss;
        } else if (this.pred == StatementComponent.PREDICATE) {
            p = sp;
        } else if (this.pred == StatementComponent.OBJECT && so instanceof URI) {
            p = (URI) so;
        } else if (this.pred == StatementComponent.CONTEXT && sc instanceof URI) {
            p = (URI) sc;
        } else {
            return null;
        }

        Resource s;
        if (this.subj instanceof Resource) {
            s = (Resource) this.subj;
        } else if (this.subj == StatementComponent.SUBJECT) {
            s = ss;
        } else if (this.subj == StatementComponent.PREDICATE) {
            s = sp;
        } else if (this.subj == StatementComponent.OBJECT && so instanceof Resource) {
            s = (Resource) so;
        } else if (this.subj == StatementComponent.CONTEXT && sc != null) {
            s = sc;
        } else {
            return null;
        }

        Resource c;
        if (this.ctx == null || this.ctx instanceof Resource) {
            c = (Resource) this.ctx;
        } else if (this.ctx == StatementComponent.SUBJECT) {
            c = ss;
        } else if (this.ctx == StatementComponent.PREDICATE) {
            c = sp;
        } else if (this.ctx == StatementComponent.OBJECT && so instanceof Resource) {
            c = (Resource) so;
        } else if (this.ctx == StatementComponent.CONTEXT) {
            c = sc;
        } else {
            return null;
        }

        Value o;
        if (this.obj instanceof Value) {
            o = (Value) this.obj;
        } else if (this.obj == StatementComponent.SUBJECT) {
            o = ss;
        } else if (this.obj == StatementComponent.PREDICATE) {
            o = sp;
        } else if (this.obj == StatementComponent.OBJECT) {
            o = so;
        } else if (this.obj == StatementComponent.CONTEXT && sc != null) {
            o = sc;
        } else {
            return null;
        }

        return Statements.VALUE_FACTORY.createStatement(s, p, o, c);

        // final Object subj = resolve(this.subj, stmt);
        // if (!(subj instanceof Resource)) {
        // return null;
        // }
        //
        // final Object pred = resolve(this.pred, stmt);
        // if (!(pred instanceof URI)) {
        // return null;
        // }
        //
        // final Object obj = resolve(this.obj, stmt);
        // if (!(obj instanceof Value)) {
        // return null;
        // }
        //
        // final Object ctx = resolve(this.ctx, stmt);
        // if (ctx != null && !(ctx instanceof Resource)) {
        // return null;
        // }
        //
        // return Statements.VALUE_FACTORY.createStatement((Resource) subj, (URI) pred, (Value)
        // obj,
        // (Resource) ctx);
    }

    // private static Object resolve(final Object component, final Statement stmt) {
    // return component instanceof StatementComponent ? ((StatementComponent) component)
    // .apply(stmt) : component;
    // }

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
        toStringHelper(this.pred, builder);
        builder.append(' ');
        toStringHelper(this.obj, builder);
        builder.append(' ');
        toStringHelper(this.ctx, builder);
        return builder.toString();
    }

    private void toStringHelper(final Object component, final StringBuilder builder) {
        if (component instanceof StatementComponent) {
            builder.append('?').append(((StatementComponent) component).getLetter());
        } else if (component == null) {
            builder.append("null");
        } else {
            try {
                Statements.formatValue((Value) component, Namespaces.DEFAULT, builder);
            } catch (final IOException ex) {
                throw new Error(ex);
            }
        }
    }

    private static Object check(final Object component) {
        if (component != null && !(component instanceof Value)
                && !(component instanceof StatementComponent)) {
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
                throw new IllegalArgumentException("Could not extract component from "
                        + var.getName());
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
            throw new IllegalArgumentException("Could not find variable " + var.getName()
                    + " in pattern " + body);
        }
    }
}