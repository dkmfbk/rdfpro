package eu.fbk.rdfpro.rules.model;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.util.ModelException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.sail.SailConnection;

import eu.fbk.rdfpro.rules.util.Iterators;

public abstract class QuadModel extends AbstractCollection<Statement> implements Graph,
        Serializable {

    protected final static Resource[] CTX_ANY = new Resource[0];

    private static final long serialVersionUID = 1L;

    public static QuadModel create() {
        return new MemoryQuadModel();
    }

    public static QuadModel wrap(final SailConnection connection) {
        return new SailQuadModel(connection);
    }

    public static QuadModel wrap(final RepositoryConnection connection) {
        return new RepositoryQuadModel(connection);
    }

    protected abstract Set<Namespace> doGetNamespaces();

    @Nullable
    protected abstract Namespace doGetNamespace(String prefix);

    @Nullable
    protected abstract Namespace doSetNamespace(String prefix, String name);

    protected abstract int doSize(@Nullable Resource subj, @Nullable URI pred,
            @Nullable Value obj, Resource[] ctxs);

    protected abstract Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs);

    protected abstract boolean doAdd(@Nullable Resource subj, @Nullable URI pred,
            @Nullable Value obj, Resource[] ctxs);

    protected abstract boolean doRemove(@Nullable Resource subj, @Nullable URI pred,
            @Nullable Value obj, Resource[] ctxs);

    public final QuadModel unmodifiable() {
        return this instanceof UnmodifiableModel ? this : new UnmodifiableModel(this);
    }

    /**
     * Gets the namespaces associated to this quad model.
     *
     * @return the namespaces
     */
    public final Set<Namespace> getNamespaces() {
        return doGetNamespaces();
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
        return doGetNamespace(prefix);
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
        doSetNamespace(Objects.requireNonNull(prefix), Objects.requireNonNull(name));
        return new NamespaceImpl(prefix, name);
    }

    /**
     * Sets the prefix for a namespace.
     *
     * @param namespace
     *            a {@link Namespace} object to use in this Model.
     */
    public final void setNamespace(final Namespace namespace) {
        doSetNamespace(namespace.getPrefix(), namespace.getName());
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
        return doSetNamespace(Objects.requireNonNull(prefix), null);
    }

    @Override
    public final boolean isEmpty() {
        return !doIterator(null, null, null, null).hasNext();
    }

    @Override
    public final int size() {
        return doSize(null, null, null, CTX_ANY);
    }

    public final int size(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj) {
        return doSize(subj, pred, obj, CTX_ANY);
    }

    public final int size(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource... contexts) {
        return doSize(subj, pred, obj, contexts);
    }

    @Override
    public final Iterator<Statement> iterator() {
        return doIterator(null, null, null, CTX_ANY);
    }

    public final Iterator<Statement> iterator(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj) {
        return doIterator(subj, pred, obj, CTX_ANY);
    }

    public final Iterator<Statement> iterator(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource... contexts) {
        return doIterator(subj, pred, obj, contexts);
    }

    @Override
    public final boolean contains(@Nullable final Object object) {
        if (object instanceof Statement) {
            final Statement stmt = (Statement) object;
            return doIterator(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                    new Resource[] { stmt.getContext() }).hasNext();
        }
        return false;
    }

    public final boolean contains(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj) {
        return doIterator(subj, pred, obj, null).hasNext();
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
    public final boolean contains(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return doIterator(subj, pred, obj, ctxs).hasNext();
    }

    @Override
    public final boolean add(final Statement stmt) {
        return doAdd(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                new Resource[] { stmt.getContext() });
    }

    public final boolean add(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj) {
        return doAdd(subj, pred, obj, CTX_ANY);
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
    public final boolean add(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return doAdd(subj, pred, obj, ctxs);
    }

    @Override
    public final void clear() {
        doRemove(null, null, null, CTX_ANY);
    }

    /**
     * Removes statements with the specified context existing in this quad model.
     *
     * @param contexts
     *            the contexts of the statements to remove
     * @return true, if one or more statements have been removed.
     */
    public final boolean clear(final Resource... contexts) {
        return doRemove(null, null, null, contexts);
    }

    @Override
    public final boolean remove(@Nullable final Object object) {
        if (object instanceof Statement) {
            final Statement stmt = (Statement) object;
            return doRemove(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                    new Resource[] { stmt.getContext() });
        }
        return false;
    }

    public final boolean remove(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj) {
        return doRemove(subj, pred, obj, CTX_ANY);
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
    public final boolean remove(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return doRemove(subj, pred, obj, ctxs);
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
    public QuadModel filter(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, final Resource... ctxs) {
        return new FilteredModel(this, subj, pred, obj, ctxs);
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
                    return doIterator((Resource) object, null, null, null).hasNext();
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof Resource) {
                    return doRemove((Resource) object, null, null, null);
                }
                return false;
            }

            @Override
            public boolean add(final Resource subj) {
                return doAdd(subj, null, null, null);
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
    public final Set<URI> predicates() {
        return new ValueSet<URI>() {

            @Override
            public boolean contains(final Object object) {
                if (object instanceof URI) {
                    return doIterator(null, (URI) object, null, null).hasNext();
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof URI) {
                    return doRemove(null, (URI) object, null, null);
                }
                return false;
            }

            @Override
            public boolean add(final URI pred) {
                return doAdd(null, pred, null, null);
            }

            @Override
            URI term(final Statement stmt) {
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
                    return doIterator(null, null, (Value) object, null).hasNext();
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof Value) {
                    return doRemove(null, null, (Value) object, null);
                }
                return false;
            }

            @Override
            public boolean add(final Value obj) {
                return doAdd(null, null, obj, null);
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
                    return doIterator(null, null, null, new Resource[] { (Resource) object })
                            .hasNext();
                }
                return false;
            }

            @Override
            public boolean remove(final Object object) {
                if (object instanceof Resource || object == null) {
                    return doRemove(null, null, null, new Resource[] { (Resource) object });
                }
                return false;
            }

            @Override
            public boolean add(final Resource context) {
                return doAdd(null, null, null, new Resource[] { context });
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
        final Iterator<Value> iter = objects().iterator();
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
        final Value obj = objectValue();
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
        final Value obj = objectValue();
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
    public final URI objectURI() throws ModelException {
        final Value obj = objectValue();
        if (obj == null) {
            return null;
        }
        if (obj instanceof URI) {
            return (URI) obj;
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
        final Value obj = objectValue();
        if (obj == null) {
            return null;
        }
        return obj.stringValue();
    }

    @Deprecated
    @Override
    public final Iterator<Statement> match(@Nullable final Resource subj,
            @Nullable final URI pred, @Nullable final Value obj, final Resource... contexts) {
        return filter(subj, pred, obj, contexts).iterator();
    }

    @Deprecated
    @Override
    public final ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
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
                set.add(term(iter.next()));
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
                    this.next = findNext();
                }
                return this.next != null;
            }

            @Override
            public V next() {
                if (this.next == null) {
                    this.next = findNext();
                    if (this.next == null) {
                        throw new NoSuchElementException();
                    }
                }
                this.current = this.next;
                this.next = null;
                final V value = term(this.current);
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
                    if (accept(st)) {
                        return st;
                    }
                }
                return null;
            }

            private boolean accept(final Statement st) {
                return !this.set.contains(term(st));
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
        protected int doSize(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, @Nullable final Resource[] ctxs) {
            return this.model.doSize(subj, pred, obj, ctxs);
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable final Resource subj,
                @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {
            return Iterators.unmodifiable(doIterator(subj, pred, obj, ctxs));
        }

        @Override
        protected boolean doAdd(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected boolean doRemove(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            throw new UnsupportedOperationException();
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
        protected int doSize(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            return 0;
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable final Resource subj,
                @Nullable final URI pred, @Nullable final Value obj, final Resource[] ctxs) {
            return Collections.emptyIterator();
        }

        @Override
        protected boolean doAdd(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            throw new UnsupportedOperationException("All statements are filtered out of view");
        }

        @Override
        protected boolean doRemove(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, final Resource[] ctxs) {
            return false;
        }

        @Override
        public QuadModel filter(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, final Resource... ctxs) {
            return this;
        }

    }

    private final static class FilteredModel extends QuadModel {

        private static final URI EMPTY = new URIImpl("sesame:empty");

        private static final long serialVersionUID = 1L;

        private final QuadModel model;

        @Nullable
        private final Resource subj;

        @Nullable
        private final URI pred;

        @Nullable
        private final Value obj;

        @Nullable
        private final Resource[] contexts;

        FilteredModel(final QuadModel model, @Nullable final Resource subj,
                @Nullable final URI pred, @Nullable final Value obj,
                @Nullable final Resource[] contexts) {
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
        protected int doSize(@Nullable Resource subj, @Nullable URI pred, @Nullable Value obj,
                @Nullable Resource[] ctxs) {

            subj = (Resource) filter(subj, this.subj);
            pred = (URI) filter(pred, this.pred);
            obj = filter(obj, this.obj);
            ctxs = filter(ctxs, this.contexts);

            return subj == EMPTY || pred == EMPTY || obj == EMPTY || ctxs == null ? 0 //
                    : this.model.doSize(subj, pred, obj, ctxs);
        }

        @Override
        protected Iterator<Statement> doIterator(@Nullable Resource subj, @Nullable URI pred,
                @Nullable Value obj, Resource[] ctxs) {

            subj = (Resource) filter(subj, this.subj);
            pred = (URI) filter(pred, this.pred);
            obj = filter(obj, this.obj);
            ctxs = filter(ctxs, this.contexts);

            return subj == EMPTY || pred == EMPTY || obj == EMPTY || ctxs == null ? Collections
                    .emptyIterator() : this.model.doIterator(subj, pred, obj, ctxs);
        }

        @Override
        protected boolean doAdd(@Nullable Resource subj, @Nullable URI pred, @Nullable Value obj,
                Resource[] ctxs) {

            subj = (Resource) filter(subj, this.subj);
            pred = (URI) filter(pred, this.pred);
            obj = filter(obj, this.obj);
            ctxs = filter(ctxs, this.contexts);

            if (subj == EMPTY || pred == EMPTY || obj == EMPTY || ctxs == null) {
                throw new IllegalArgumentException("Statement is filtered out of view");
            }

            return this.model.doAdd(subj, pred, obj, ctxs);
        }

        @Override
        protected boolean doRemove(@Nullable Resource subj, @Nullable URI pred,
                @Nullable Value obj, Resource[] ctxs) {

            subj = (Resource) filter(subj, this.subj);
            pred = (URI) filter(pred, this.pred);
            obj = filter(obj, this.obj);
            ctxs = filter(ctxs, this.contexts);

            return subj == EMPTY || pred == EMPTY || obj == EMPTY || ctxs == null ? false //
                    : this.model.doRemove(subj, pred, obj, ctxs);
        }

        @Override
        public QuadModel filter(@Nullable Resource subj, @Nullable URI pred, @Nullable Value obj,
                Resource... ctxs) {

            subj = (Resource) filter(subj, this.subj);
            pred = (URI) filter(pred, this.pred);
            obj = filter(obj, this.obj);
            ctxs = filter(ctxs, this.contexts);

            return subj == EMPTY || pred == EMPTY || obj == EMPTY || ctxs == null ? new EmptyModel(
                    this.model) : this.model.filter(subj, pred, obj, ctxs);
        }

        private static Value filter(@Nullable final Value inputValue,
                @Nullable final Value thisValue) {
            if (inputValue == null) {
                return thisValue;
            } else if (thisValue == null || inputValue.equals(thisValue)) {
                return inputValue;
            } else {
                return EMPTY;
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
