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

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

// This looks more like a StatementMapper

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

        this.subj = StatementTemplate.check(subj);
        this.pred = StatementTemplate.check(pred);
        this.obj = StatementTemplate.check(obj);
        this.ctx = StatementTemplate.check(ctx);

        this.subjIndex = this.subj instanceof Resource ? 4
                : ((StatementComponent) this.subj).getIndex();
        this.predIndex = this.pred instanceof Resource ? 5
                : ((StatementComponent) this.pred).getIndex();
        this.objIndex = this.obj instanceof Resource ? 6
                : ((StatementComponent) this.obj).getIndex();
        this.ctxIndex = this.ctx == null || this.ctx instanceof Resource ? 7
                : ((StatementComponent) this.ctx).getIndex();
    }

    public StatementTemplate(final StatementPattern head) {
        this(StatementTemplate.componentFor(head.getSubjectVar()),
                StatementTemplate.componentFor(head.getPredicateVar()),
                StatementTemplate.componentFor(head.getObjectVar()),
                StatementTemplate.componentFor(head.getContextVar()));
    }

    public StatementTemplate(final StatementPattern head, final StatementPattern body) {
        this(StatementTemplate.componentFor(head.getSubjectVar(), body),
                StatementTemplate.componentFor(head.getPredicateVar(), body),
                StatementTemplate.componentFor(head.getObjectVar(), body),
                StatementTemplate.componentFor(head.getContextVar(), body));
    }

    public StatementTemplate normalize(final Function<? super Value, ?> normalizer) {
        final Object nsubj = this.subj instanceof StatementComponent ? (Object) this.subj
                : normalizer.apply((Value) this.subj);
        final Object npred = this.pred instanceof StatementComponent ? (Object) this.pred
                : normalizer.apply((Value) this.pred);
        final Object nobj = this.obj instanceof StatementComponent ? (Object) this.obj
                : normalizer.apply((Value) this.obj);
        final Object nctx = this.ctx == null //
                || this.ctx instanceof StatementComponent ? (Object) this.ctx
                        : normalizer.apply((Value) this.ctx);
        return nsubj == this.subj && npred == this.pred && nobj == this.obj && nctx == this.ctx
                ? this : new StatementTemplate(nsubj, npred, nobj, nctx);
    }

    @Override
    public Statement apply(final Statement stmt) {
        try {
            final IRI p = (IRI) this.resolve(stmt, this.predIndex);
            final Resource s = (Resource) this.resolve(stmt, this.subjIndex);
            final Resource c = (Resource) this.resolve(stmt, this.ctxIndex);
            final Value o = (Value) this.resolve(stmt, this.objIndex);
            return Statements.VALUE_FACTORY.createStatement(s, p, o, c);
        } catch (final Throwable ex) {
            return null;
        }
    }

    public Statement apply(final Statement stmt, final StatementDeduplicator deduplicator) {
        try {
            final IRI p = (IRI) this.resolve(stmt, this.predIndex);
            final Resource s = (Resource) this.resolve(stmt, this.subjIndex);
            final Resource c = (Resource) this.resolve(stmt, this.ctxIndex);
            final Value o = (Value) this.resolve(stmt, this.objIndex);
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
        this.toStringHelper(this.subj, builder);
        builder.append(' ');
        this.toStringHelper(this.pred, builder);
        builder.append(' ');
        this.toStringHelper(this.obj, builder);
        builder.append(' ');
        this.toStringHelper(this.ctx, builder);
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
                throw new IllegalArgumentException(
                        "Could not extract component from " + var.getName());
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
            throw new IllegalArgumentException(
                    "Could not find variable " + var.getName() + " in pattern " + body);
        }
    }
}