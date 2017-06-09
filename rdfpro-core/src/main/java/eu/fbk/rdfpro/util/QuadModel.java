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

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.ModelException;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.TripleSource;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.EvaluationStatistics;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.StrictEvaluationStrategy;

@SuppressWarnings("deprecation")
public abstract class QuadModel extends AbstractCollection<Statement>
        implements org.eclipse.rdf4j.model.Graph, Serializable {

    protected final static Resource[] CTX_ANY = new Resource[0];

    protected final static Resource[] CTX_DEFAULT = new Resource[] { null };

    private static final long serialVersionUID = 1L;

    public static QuadModel create() {
        return new QuadModelImpl();
    }

    public static QuadModel create(final Iterable<Statement> statements) {
        final QuadModel model = QuadModel.create();
        Iterables.addAll(model, statements);
        return model;
    }

    /**
     * Returns a {@code QuadModel} view of the supplied {@code SailConnection}. Given to the use
     * of internal locks in some SAIL implementations (e.g., the MemoryStore), the returned view
     * should be used only inside a thread, similarly to the SailConnection it wraps. Parameter
     * {@code trackChanges} enables or disables the checks performed each time a statement is
     * added or removed that the model was changed.
     *
     * @param connection
     *            the connection to wrap
     * @param trackChanges
     *            true, if addition/deletion operations should return true or false based on
     *            whether the model was actually changed by the operation; if false, the check is
     *            skipped and all modification operations return true
     * @return the created {@code QuadModel} view
     */
    public static QuadModel wrap(final org.eclipse.rdf4j.sail.SailConnection connection,
            final boolean trackChanges) {
        return new QuadModelSailAdapter(connection, trackChanges);
    }

    public static QuadModel wrap(
            final org.eclipse.rdf4j.repository.RepositoryConnection connection,
            final boolean trackChanges) {
        return new QuadModelRepositoryAdapter(connection, trackChanges);
    }

    public static QuadModel wrap(final Model model) {
        return new QuadModelModelAdapter(model);
    }

    protected abstract Set<Namespace> doGetNamespaces();

    @Nullable
    protected abstract Namespace doGetNamespace(String prefix);

    @Nullable
    protected abstract Namespace doSetNamespace(String prefix, String name);

    protected abstract int doSize(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
            Resource[] ctxs);

    protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, @Nullable final Resource ctx) {
        return -1;
    }

    protected abstract Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs);

    protected abstract boolean doAdd(@Nullable Resource subj, @Nullable IRI pred,
            @Nullable Value obj, Resource[] ctxs);

    protected abstract boolean doRemove(@Nullable Resource subj, @Nullable IRI pred,
            @Nullable Value obj, Resource[] ctxs);

    protected Iterator<BindingSet> doEvaluate(final TupleExpr expr,
            @Nullable final Dataset dataset, @Nullable final BindingSet bindings) {

        return Algebra.evaluateTupleExpr(expr, dataset, bindings,
                new StrictEvaluationStrategy(this.getTripleSource(), dataset,
                        Algebra.getFederatedServiceResolver()),
                this.getEvaluationStatistics(), this.getValueNormalizer());
    }

    protected Value doNormalize(@Nullable final Value value) {
        return value;
    }

    public final QuadModel unmodifiable() {
        return this instanceof UnmodifiableModel ? this : new UnmodifiableModel(this);
    }

    /**
     * Gets the namespaces associated to this quad model.
     *
     * @return the namespaces
     */
    public final Set<Namespace> getNamespaces() {
        return this.doGetNamespaces();
    }

    /**
     * Gets the namespace for the specified prefix, if any.
     *
     * @param prefix
     *            the namespace prefix
     * @return the namespace for the specified prefix, if defined, otherwise null
     */
    @Nullable
    public final Namespace getNamespace(final String prefix) {
        return this.doGetNamespace(prefix);
    }

    /**
     * Sets the prefix for a namespace.
     *
     * @param prefix
     *            the prefix
     * @param name
     *            the namespace that the prefix maps to
     * @return the {@link Namespace} object for the given namespace
     */
    public final Namespace setNamespace(final String prefix, final String name) {
        this.doSetNamespace(Objects.requireNonNull(prefix), Objects.requireNonNull(name));
        return new SimpleNamespace(prefix, name);
    }

    /**
     * Sets the prefix for a namespace.
     *
     * @param namespace
     *            a {@link Namespace} object to use in this Model.
     */
    public final void setNamespace(final Namespace namespace) {
        this.doSetNamespace(namespace.getPrefix(), namespace.getName());
    }

    /**
     * Removes a namespace declaration.
     *
     * @param prefix
     *            the prefix
     * @return the previous namespace bound to the prefix, if any, otherwise null
     */
    @Nullable
    public final Namespace removeNamespace(final String prefix) {
        return this.doSetNamespace(Objects.requireNonNull(prefix), null);
    }

    @Override
    public final boolean isEmpty() {
        final Iterator<Statement> iterator = this.doIterator(null, null, null, QuadModel.CTX_ANY);
        try {
            return !iterator.hasNext();
        } finally {
            IO.closeQuietly(iterator);
        }
    }

    @Override
    public final int size() {
        return this.doSize(null, null, null, QuadModel.CTX_ANY);
    }

    public final int size(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj) {
        return this.doSize(subj, pred, obj, QuadModel.CTX_ANY);
    }

    public final int size(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... contexts) {
        return this.doSize(subj, pred, obj, contexts);
    }

    public final int sizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... contexts) {
        if (contexts.length == 0) {
            return this.doSizeEstimate(subj, pred, obj, null);
        } else {
            int estimate = 0;
            for (final Resource ctx : contexts) {
                final int delta = this.doSizeEstimate(subj, pred, obj, ctx);
                if (ctx == null) {
                    return delta;
                }
                estimate += delta;
            }
            return estimate;
        }
    }

    @Override
    public final Iterator<Statement> iterator() {
        return this.doIterator(null, null, null, QuadModel.CTX_ANY);
    }

    public final Iterator<Statement> iterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj) {
        return this.doIterator(subj, pred, obj, QuadModel.CTX_ANY);
    }

    public final Iterator<Statement> iterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final Resource... contexts) {
        return this.doIterator(subj, pred, obj, contexts);
    }

    public final Iterator<BindingSet> evaluate(final TupleExpr expr,
            @Nullable final Dataset dataset, @Nullable final BindingSet bindings) {
        return this.doEvaluate(Objects.requireNonNull(expr), dataset, bindings);
    }

    @Override
    public final boolean contains(@Nullable final Object object) {
        if (object instanceof Statement) {
            final Statement stmt = (Statement) object;
            return this.contains(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                    new Resource[] { stmt.getContext() });
        }
        return false;
    }

    public final boolean contains(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj) {
        return this.contains(subj, pred, obj, QuadModel.CTX_ANY);
    }

    /**
     * Determines if statements with the specified subject, predicate, object and (optionally)
     * context exist in this model. The {@code subject}, {@code predicate} and {@code object}
     * parameters can be null to indicate wildcards. The {@code contexts} parameter is a wildcard
     * and accepts zero or more values. If no contexts are specified, statements will match
     * disregarding their context. If one or more contexts are specified, statements with a
     * context matching one of these will match. Note: to match statements without an associated
     * context, specify the value null and explicitly cast it to type {@code Resource}.
     *
     * @param subj
     *            the subject to match, or null to match any subject
     * @param pred
     *            the predicate to match, or null to match any predicate
     * @param obj
     *            the object to match, or null to match any object
     * @param ctxs
     *            the contexts to match; if empty, any statement context will be matched
     * @return true, if there are statements matching the specified pattern
     */
    public final boolean contains(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        final Iterator<Statement> iterator = this.doIterator(subj, pred, obj, ctxs);
        try {
            return iterator.hasNext();
        } finally {
            IO.closeQuietly(iterator);
        }
    }

    @Override
    public final boolean add(final Statement stmt) {
        return this.doAdd(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                new Resource[] { stmt.getContext() });
    }

    public final boolean add(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj) {
        return this.doAdd(subj, pred, obj, QuadModel.CTX_ANY);
    }

    /**
     * Adds one or more statements to the quad model. This method adds a statement for each
     * specified context. If no context is specified, a single statement with no associated
     * context is added. If this Model is a filtered Model then null (if context empty) values are
     * permitted and will use the corresponding filtered values.
     *
     * @param subj
     *            the subject
     * @param pred
     *            the predicate
     * @param obj
     *            the object.
     * @param ctxs
     *            the contexts to add statements to.
     * @return true if the model changed as a result of the operation
     * @throws IllegalArgumentException
     *             if this quad model cannot store the given statement, because it is filtered out
     *             of this view.
     * @throws UnsupportedOperationException
     *             if this quad model cannot accept any statements, because it is filtered to the
     *             empty set.
     */
    @Override
    public final boolean add(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return this.doAdd(subj, pred, obj, ctxs);
    }

    @Override
    public final void clear() {
        this.doRemove(null, null, null, QuadModel.CTX_ANY);
    }

    /**
     * Removes statements with the specified context existing in this quad model.
     *
     * @param contexts
     *            the contexts of the statements to remove
     * @return true, if one or more statements have been removed.
     */
    public final boolean clear(final Resource... contexts) {
        return this.doRemove(null, null, null, contexts);
    }

    @Override
    public final boolean remove(@Nullable final Object object) {
        if (object instanceof Statement) {
            final Statement stmt = (Statement) object;
            return this.doRemove(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                    new Resource[] { stmt.getContext() });
        }
        return false;
    }

    public final boolean remove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj) {
        return this.doRemove(subj, pred, obj, QuadModel.CTX_ANY);
    }

    /**
     * Removes statements with the specified subject, predicate, object and (optionally) context
     * existing in this model. The {@code subject}, {@code predicate} and {@code object}
     * parameters can be null to indicate wildcards. The {@code contexts} parameter is a wildcard
     * and accepts zero or more values. If no context is specified, statements will be removed
     * disregarding their context. If one or more contexts are specified, statements with a
     * context matching one of these will be removed. Note: to remove statements without an
     * associated context, specify the value null and explicitly cast it to type {@code Resource}.
     *
     * @param subj
     *            the subject to match, or null to match any subject
     * @param pred
     *            the predicate to match, or null to match any predicate
     * @param obj
     *            the object to match, or null to match any object
     * @param ctxs
     *            the contexts to match; if empty, any statement context will be matched
     * @return true, if one or more statements have been removed.
     */
    public final boolean remove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return this.doRemove(subj, pred, obj, ctxs);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean removeAll(final Collection<?> c) {
        Collection<?> toRemove = c;
        if ((c instanceof Set<?> || c instanceof Model || c instanceof QuadModel)
                && c.size() > this.size()) {
            toRemove = Lists.newArrayList();
            for (final Statement stmt : this) {
                if (c.contains(stmt)) {
                    ((Collection) toRemove).add(stmt);
                }
            }
        }
        boolean modified = false;
        for (final Object element : toRemove) {
            final boolean removed = this.remove(element);
            modified |= removed;
        }
        return modified;
    }

    /**
     * Returns a view of the statements with the specified subject, predicate, object and
     * (optionally) context. The {@code subject}, {@code predicate} and {@code object} parameters
     * can be null to indicate wildcards. The {@code contexts} parameter is a wildcard and accepts
     * zero or more values. If no context is specified, statements will match disregarding their
     * context. If one or more contexts are specified, statements with a context matching one of
     * these will match. Note: to match statements without an associated context, specify the
     * value null and explicitly cast it to type {@code Resource}. The returned quad model is
     * backed by this model, so changes to this model are reflected in the returned model, and
     * vice-versa. If this quad model is modified while an iteration over the returned model is in
     * progress (except through the iterator's own {@code remove} operation), the results of the
     * iteration are undefined. The returned quad model supports element removal, which removes
     * the corresponding statement from this model. Statements can be added to the returned quad
     * model only if they match the filter pattern.
     *
     * @param subj
     *            the subject to match, or null to match any subject
     * @param pred
     *            the predicate to match, or null to match any predicate
     * @param obj
     *            the object to match, or null to match any object
     * @param ctxs
     *            the contexts to match; if empty, any statement context will be matched
     * @return a quad model view with the statements matching the pattern specified
     */
    public QuadModel filter(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return new FilteredModel(this, subj, pred, obj, ctxs);
    }

    /**
     * Returns an <i>immutable</i> view of a subset of this model, containing the specified
     * statements. The supplied collection of statements <b>must</b> not contain duplicates and
     * must be a subset of the statements in this model.
     *
     * @param statements
     *            the statements of this model to include in the returned immutable view, without
     *            duplicates
     * @return an immutable view of this model including only the statements specified
     */
    public QuadModel filter(final Collection<Statement> statements) {
        return new QuadModelSubModel(this, statements);
    }

    /**
     * Returns a {@link Set} view of the subjects contained in this model. The set is backed by
     * this model, so changes to this model are reflected in the set, and vice-versa. If the model
     * is modified while an iteration over the set is in progress (except through the iterator's
     * own {@code remove} operation), the results of the iteration are undefined. The set supports
     * element removal, which removes the corresponding statement from the model. It does not
     * support element addition, unless this quad model is a filtered quad model with non-null
     * {@code pred} and {@code obj} filter parameteres.
     *
     * @return a set view of the subjects contained in this quad model
     */
    public final Set<Resource> subjects() {
        return new ValueSet<Resource>() {

            @Override
            public boolean contains(final Object object) {
                if (object instanceof Resource) {
                    return QuadModel.this.contains((Resource) object, null, null);
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof Resource) {
                    return QuadModel.this.doRemove((Resource) object, null, null, null);
                }
                return false;
            }

            @Override
            public boolean add(final Resource subj) {
                return QuadModel.this.doAdd(subj, null, null, null);
            }

            @Override
            Resource term(final Statement stmt) {
                return stmt.getSubject();
            }

        };
    }

    /**
     * Returns a {@link Set} view of the predicates contained in this model. The set is backed by
     * this model, so changes to this model are reflected in the set, and vice-versa. If the model
     * is modified while an iteration over the set is in progress (except through the iterator's
     * own {@code remove} operation), the results of the iteration are undefined. The set supports
     * element removal, which removes the corresponding statement from the model. It does not
     * support element addition, unless this is a filtered quad model with non-null {@code subj}
     * and {@code obj} filter parameters.
     *
     * @return a set view of the predicates contained in this quad model
     */
    public final Set<IRI> predicates() {
        return new ValueSet<IRI>() {

            @Override
            public boolean contains(final Object object) {
                if (object instanceof IRI) {
                    return QuadModel.this.contains(null, (IRI) object, null);
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof IRI) {
                    return QuadModel.this.doRemove(null, (IRI) object, null, null);
                }
                return false;
            }

            @Override
            public boolean add(final IRI pred) {
                return QuadModel.this.doAdd(null, pred, null, null);
            }

            @Override
            IRI term(final Statement stmt) {
                return stmt.getPredicate();
            }

        };
    }

    /**
     * Returns a {@link Set} view of the objects contained in this model. The set is backed by
     * this model, so changes to this model are reflected in the set, and vice-versa. If the model
     * is modified while an iteration over the set is in progress (except through the iterator's
     * own {@code remove} operation), the results of the iteration are undefined. The set supports
     * element removal, which removes the corresponding statement from the model. It does not
     * support element addition, unless this is a filtered quad model with non-null {@code subj}
     * and {@code pred} filter parameters.
     *
     * @return a set view of the objects contained in this quad model
     */
    public final Set<Value> objects() {
        return new ValueSet<Value>() {

            @Override
            public boolean contains(final Object object) {
                if (object instanceof Value) {
                    return QuadModel.this.contains(null, null, (Value) object);
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof Value) {
                    return QuadModel.this.doRemove(null, null, (Value) object, null);
                }
                return false;
            }

            @Override
            public boolean add(final Value obj) {
                return QuadModel.this.doAdd(null, null, obj, null);
            }

            @Override
            Value term(final Statement stmt) {
                return stmt.getObject();
            }

        };
    }

    /**
     * Returns a {@link Set} view of the contexts contained in this model. The set is backed by
     * this model, so changes to this model are reflected in the set, and vice-versa. If the model
     * is modified while an iteration over the set is in progress (except through the iterator's
     * own {@code remove} operation), the results of the iteration are undefined. The set supports
     * element removal, which removes the corresponding statement from the model. It does not
     * support element addition, unless this is a filtered quad model with non-null {@code subj},
     * {@code pred} and {@code obj} filter parameters.
     *
     * @return a set view of the contexts contained in this quad model
     */
    public final Set<Resource> contexts() {
        return new ValueSet<Resource>() {

            @Override
            public boolean contains(final Object object) {
                if (object instanceof Resource || object == null) {
                    return QuadModel.this.contains(null, null, null, (Resource) object);
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof Resource || object == null) {
                    return QuadModel.this.doRemove(null, null, null,
                            new Resource[] { (Resource) object });
                }
                return false;
            }

            @Override
            public boolean add(final Resource context) {
                return QuadModel.this.doAdd(null, null, null, new Resource[] { context });
            }

            @Override
            Resource term(final Statement stmt) {
                return stmt.getContext();
            }

        };
    }

    /**
     * Gets the object of the statement(s). If the quad model contains one or more statements, all
     * these statements should have the same object. A {@link ModelException} is thrown if this is
     * not the case.
     *
     * @return the object of the statement(s) in this model, or null if this model is empty
     * @throws ModelException
     *             if multiple objects are present
     */
    @Nullable
    public final Value objectValue() throws ModelException {
        final Iterator<Value> iter = this.objects().iterator();
        if (iter.hasNext()) {
            final Value obj = iter.next();
            if (iter.hasNext()) {
                throw new ModelException(obj, iter.next());
            }
            return obj;
        }
        return null;
    }

    /**
     * Utility method that casts the return value of {@link #objectValue()} to a Literal, or
     * throws a ModelException if that value is not a Literal.
     *
     * @return the literal object of statement(s) in this model, or null if this model is empty
     * @throws ModelException
     *             if multiple objects are present, or if the unique object is not a Literal
     */
    @Nullable
    public final Literal objectLiteral() throws ModelException {
        final Value obj = this.objectValue();
        if (obj == null) {
            return null;
        }
        if (obj instanceof Literal) {
            return (Literal) obj;
        }
        throw new ModelException(obj);
    }

    /**
     * Utility method that casts the return value of {@link #objectValue()} to a Resource, or
     * throws a ModelException if that value is not a Resource.
     *
     * @return the resource object of statement(s) in this model, or null if this model is empty
     * @throws ModelException
     *             if multiple objects are present, or if the unique object is not a resource
     */
    @Nullable
    public final Resource objectResource() throws ModelException {
        final Value obj = this.objectValue();
        if (obj == null) {
            return null;
        }
        if (obj instanceof Resource) {
            return (Resource) obj;
        }
        throw new ModelException(obj);
    }

    /**
     * Utility method that casts the return value of {@link #objectValue()} to a URI, or throws a
     * ModelException if that value is not a URI.
     *
     * @return the URI object of statement(s) in this model, or null if this model is empty
     * @throws ModelException
     *             if multiple objects are present, or if the unique object is not a URI
     */
    @Nullable
    public final IRI objectURI() throws ModelException {
        final Value obj = this.objectValue();
        if (obj == null) {
            return null;
        }
        if (obj instanceof IRI) {
            return (IRI) obj;
        }
        throw new ModelException(obj);
    }

    /**
     * Utility method that returns the string value of {@link #objectValue()}.
     *
     * @return the string value of the unique object of statement(s) in this model, or null if
     *         this model is empty
     * @throws ModelException
     *             if multiple objects are present
     */
    @Nullable
    public final String objectString() throws ModelException {
        final Value obj = this.objectValue();
        if (obj == null) {
            return null;
        }
        return obj.stringValue();
    }

    @Deprecated
    @Override
    public final Iterator<Statement> match(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource... contexts) {
        return this.filter(subj, pred, obj, contexts).iterator();
    }

    @Deprecated
    @Override
    public final ValueFactory getValueFactory() {
        return Statements.VALUE_FACTORY;
    }

    public final TripleSource getTripleSource() {
        return new TripleSource() {

            @Override
            public ValueFactory getValueFactory() {
                return Statements.VALUE_FACTORY;
            }

            @Override
            public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                    final Resource subj, final IRI pred, final Value obj,
                    final Resource... contexts) throws QueryEvaluationException {
                return Iterators.toIteration(QuadModel.this.doIterator(subj, pred, obj, contexts));
            }

        };
    }

    public final EvaluationStatistics getEvaluationStatistics() {

        return Algebra.getEvaluationStatistics((final StatementPattern pattern) -> {

            final Var sv = pattern.getSubjectVar();
            final Var pv = pattern.getPredicateVar();
            final Var ov = pattern.getObjectVar();
            final Var cv = pattern.getContextVar();

            final Resource s = sv == null || !(sv.getValue() instanceof Resource) ? null
                    : (Resource) sv.getValue();
            final IRI p = pv == null || !(pv.getValue() instanceof IRI) ? null
                    : (IRI) pv.getValue();
            final Value o = ov == null ? null : ov.getValue();
            final Resource c = cv == null || !(cv.getValue() instanceof Resource) ? null
                    : (Resource) cv.getValue();

            return this.doSizeEstimate(s, p, o, c);

        });
    }

    public final Function<Value, Value> getValueNormalizer() {
        return new Function<Value, Value>() {

            @Override
            public Value apply(final Value value) {
                return QuadModel.this.doNormalize(value);
            }

        };
    }

    @SuppressWarnings("unchecked")
    public <T extends Value> T normalize(final T value) {
        return (T) this.doNormalize(value);
    }

    private abstract class ValueSet<V extends Value> extends AbstractSet<V> {

        @Override
        public boolean isEmpty() {
            return QuadModel.this.isEmpty();
        }

        @Override
        public int size() {
            final Iterator<Statement> iter = QuadModel.this.iterator();
            final Set<V> set = new HashSet<>();
            while (iter.hasNext()) {
                set.add(this.term(iter.next()));
            }
            return set.size();
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueSetIterator(QuadModel.this.iterator());
        }

        @Override
        public void clear() {
            QuadModel.this.clear();
        }

        @Override
        public abstract boolean add(V term);

        abstract V term(Statement st);

        private final class ValueSetIterator implements Iterator<V> {

            private final Iterator<Statement> iter;

            private final Set<V> set = new LinkedHashSet<V>();

            private Statement current;

            private Statement next;

            private ValueSetIterator(final Iterator<Statement> iter) {
                this.iter = iter;
            }

            @Override
            public boolean hasNext() {
                if (this.next == null) {
                    this.next = this.findNext();
                }
                return this.next != null;
            }

            @Override
            public V next() {
                if (this.next == null) {
                    this.next = this.findNext();
                    if (this.next == null) {
                        throw new NoSuchElementException();
                    }
                }
                this.current = this.next;
                this.next = null;
                final V value = ValueSet.this.term(this.current);
                this.set.add(value);
                return value;
            }

            @Override
            public void remove() {
                if (this.current == null) {
                    throw new IllegalStateException();
                }
                ValueSet.this.remove(this.current);
                this.current = null;
            }

            private Statement findNext() {
                while (this.iter.hasNext()) {
                    final Statement st = this.iter.next();
                    if (this.accept(st)) {
                        return st;
                    }
                }
                return null;
            }

            private boolean accept(final Statement st) {
                return !this.set.contains(ValueSet.this.term(st));
            }
        }

    }

    private static final class UnmodifiableModel extends QuadModel {

        private static final long serialVersionUID = 1L;

        private final QuadModel model;

        UnmodifiableModel(final QuadModel model) {
            this.model = model;
        }

        @Override
        protected Set<Namespace> doGetNamespaces() {
            return this.model.doGetNamespaces();
        }

        @Override
        protected Namespace doGetNamespace(final String prefix) {
            return this.model.doGetNamespace(prefix);
        }

        @Override
        protected Namespace doSetNamespace(final String prefix, final String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected int doSize(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, @Nullable final Resource[] ctxs) {
            return this.model.doSize(subj, pred, obj, ctxs);
        }

        @Override
        protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, @Nullable final Resource ctx) {
            return this.model.doSizeEstimate(subj, pred, obj, ctx);
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable final Resource subj,
                @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs) {
            return Iterators.unmodifiable(this.model.doIterator(subj, pred, obj, ctxs));
        }

        @Override
        protected boolean doAdd(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean doRemove(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Iterator<BindingSet> doEvaluate(final TupleExpr expr,
                @Nullable final Dataset dataset, @Nullable final BindingSet bindings) {
            return this.model.doEvaluate(expr, dataset, bindings);
        }

    }

    private static final class EmptyModel extends QuadModel {

        private static final long serialVersionUID = 1L;

        private final QuadModel model;

        EmptyModel(final QuadModel model) {
            this.model = model;
        }

        @Override
        protected Set<Namespace> doGetNamespaces() {
            return this.model.doGetNamespaces();
        }

        @Override
        protected Namespace doGetNamespace(final String prefix) {
            return this.model.doGetNamespace(prefix);
        }

        @Override
        protected Namespace doSetNamespace(final String prefix, final String name) {
            return this.model.doSetNamespace(prefix, name);
        }

        @Override
        protected int doSize(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            return 0;
        }

        @Override
        protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, @Nullable final Resource ctx) {
            return 0;
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable final Resource subj,
                @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs) {
            return Collections.emptyIterator();
        }

        @Override
        protected boolean doAdd(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            throw new UnsupportedOperationException("All statements are filtered out of view");
        }

        @Override
        protected boolean doRemove(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            return false;
        }

        @Override
        protected Iterator<BindingSet> doEvaluate(final TupleExpr expr,
                @Nullable final Dataset dataset, @Nullable final BindingSet bindings) {
            return Collections.emptyIterator();
        }

        @Override
        public QuadModel filter(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj, final Resource... ctxs) {
            return this;
        }

    }

    private final static class FilteredModel extends QuadModel {

        private static final IRI EMPTY = Statements.VALUE_FACTORY.createIRI("sesame:empty");

        private static final long serialVersionUID = 1L;

        private final QuadModel model;

        @Nullable
        private final Resource subj;

        @Nullable
        private final IRI pred;

        @Nullable
        private final Value obj;

        private final Resource[] contexts;

        FilteredModel(final QuadModel model, @Nullable final Resource subj,
                @Nullable final IRI pred, @Nullable final Value obj, final Resource[] contexts) {
            this.model = model;
            this.subj = subj;
            this.pred = pred;
            this.obj = obj;
            this.contexts = contexts;
        }

        @Override
        protected Set<Namespace> doGetNamespaces() {
            return this.model.doGetNamespaces();
        }

        @Override
        protected Namespace doGetNamespace(final String prefix) {
            return this.model.doGetNamespace(prefix);
        }

        @Override
        protected Namespace doSetNamespace(final String prefix, final String name) {
            return this.model.doSetNamespace(prefix, name);
        }

        @Override
        protected int doSize(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
                @Nullable Resource[] ctxs) {

            subj = (Resource) FilteredModel.filter(subj, this.subj);
            pred = (IRI) FilteredModel.filter(pred, this.pred);
            obj = FilteredModel.filter(obj, this.obj);
            ctxs = FilteredModel.filter(ctxs, this.contexts);

            return subj == FilteredModel.EMPTY || pred == FilteredModel.EMPTY
                    || obj == FilteredModel.EMPTY || ctxs == null ? 0 //
                            : this.model.doSize(subj, pred, obj, ctxs);
        }

        @Override
        protected int doSizeEstimate(@Nullable Resource subj, @Nullable IRI pred,
                @Nullable Value obj, @Nullable final Resource ctx) {

            subj = (Resource) FilteredModel.filter(subj, this.subj);
            pred = (IRI) FilteredModel.filter(pred, this.pred);
            obj = FilteredModel.filter(obj, this.obj);
            final Resource[] ctxs = FilteredModel.filter(
                    ctx == null ? QuadModel.CTX_ANY : new Resource[] { ctx }, this.contexts);

            if (subj == FilteredModel.EMPTY || pred == FilteredModel.EMPTY
                    || obj == FilteredModel.EMPTY || ctxs == null) {
                return 0;
            } else if (ctxs.length == 0) {
                return this.model.doSizeEstimate(subj, pred, obj, null);
            } else if (ctxs.length == 1) {
                return this.model.doSizeEstimate(subj, pred, obj, ctxs[0]);
            } else {
                int size = 0;
                for (final Resource c : ctxs) {
                    size += this.model.doSizeEstimate(subj, pred, obj, c);
                }
                return size;
            }
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable Resource subj, @Nullable IRI pred,
                @Nullable Value obj, Resource[] ctxs) {

            subj = (Resource) FilteredModel.filter(subj, this.subj);
            pred = (IRI) FilteredModel.filter(pred, this.pred);
            obj = FilteredModel.filter(obj, this.obj);
            ctxs = FilteredModel.filter(ctxs, this.contexts);

            return subj == FilteredModel.EMPTY || pred == FilteredModel.EMPTY
                    || obj == FilteredModel.EMPTY || ctxs == null ? Collections.emptyIterator()
                            : this.model.doIterator(subj, pred, obj, ctxs);
        }

        @Override
        protected boolean doAdd(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
                Resource[] ctxs) {

            subj = (Resource) FilteredModel.filter(subj, this.subj);
            pred = (IRI) FilteredModel.filter(pred, this.pred);
            obj = FilteredModel.filter(obj, this.obj);
            ctxs = FilteredModel.filter(ctxs, this.contexts);

            if (subj == FilteredModel.EMPTY || pred == FilteredModel.EMPTY
                    || obj == FilteredModel.EMPTY || ctxs == null) {
                throw new IllegalArgumentException("Statement is filtered out of view");
            }

            return this.model.doAdd(subj, pred, obj, ctxs);
        }

        @Override
        protected boolean doRemove(@Nullable Resource subj, @Nullable IRI pred,
                @Nullable Value obj, Resource[] ctxs) {

            subj = (Resource) FilteredModel.filter(subj, this.subj);
            pred = (IRI) FilteredModel.filter(pred, this.pred);
            obj = FilteredModel.filter(obj, this.obj);
            ctxs = FilteredModel.filter(ctxs, this.contexts);

            return subj == FilteredModel.EMPTY || pred == FilteredModel.EMPTY
                    || obj == FilteredModel.EMPTY || ctxs == null ? false //
                            : this.model.doRemove(subj, pred, obj, ctxs);
        }

        @Override
        public QuadModel filter(@Nullable Resource subj, @Nullable IRI pred, @Nullable Value obj,
                Resource... ctxs) {

            subj = (Resource) FilteredModel.filter(subj, this.subj);
            pred = (IRI) FilteredModel.filter(pred, this.pred);
            obj = FilteredModel.filter(obj, this.obj);
            ctxs = FilteredModel.filter(ctxs, this.contexts);

            return subj == FilteredModel.EMPTY || pred == FilteredModel.EMPTY
                    || obj == FilteredModel.EMPTY || ctxs == null ? new EmptyModel(this.model)
                            : this.model.filter(subj, pred, obj, ctxs);
        }

        private static Value filter(@Nullable final Value inputValue,
                @Nullable final Value thisValue) {
            if (inputValue == null) {
                return thisValue;
            } else if (thisValue == null || inputValue.equals(thisValue)) {
                return inputValue;
            } else {
                return FilteredModel.EMPTY;
            }
        }

        @Nullable
        private static Resource[] filter(final Resource[] inputCtxs, final Resource[] thisCtxs) {
            if (inputCtxs.length == 0) {
                return thisCtxs;
            } else if (thisCtxs.length == 0) {
                return inputCtxs;
            } else {
                int inputIdx = 0;
                outer: for (; inputIdx < inputCtxs.length; ++inputIdx) {
                    for (final Resource thisCtx : thisCtxs) {
                        if (Objects.equals(inputCtxs[inputIdx], thisCtx)) {
                            continue outer;
                        }
                    }
                    if (inputCtxs.length == 1) {
                        return null;
                    }
                    final Resource[] ctxs = new Resource[inputCtxs.length - 1];
                    if (inputIdx > 0) {
                        System.arraycopy(inputCtxs, 0, ctxs, 0, inputIdx);
                    }
                    int index = inputIdx;
                    for (++inputIdx; inputIdx < inputCtxs.length; ++inputIdx) {
                        for (final Resource thisCtx : thisCtxs) {
                            if (Objects.equals(inputCtxs[inputIdx], thisCtx)) {
                                ctxs[index++] = inputCtxs[inputIdx];
                                break;
                            }
                        }
                    }
                    return index == 0 ? null : index == ctxs.length ? ctxs //
                            : Arrays.copyOfRange(ctxs, 0, index);
                }
                return inputCtxs;
            }
        }

    }

}
