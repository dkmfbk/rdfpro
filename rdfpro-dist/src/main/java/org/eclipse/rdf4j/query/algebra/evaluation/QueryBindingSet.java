/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.eclipse.rdf4j.query.algebra.evaluation;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Objects;

import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.SimpleBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Alternative implementation of QueryBindingSet, supposedly more efficient (actually proved to
 * improve execution times in rdfpro-rules). As QueryBindingSet is hard-coded into RDF4J query
 * algebra operators, there is no way to cleanly change implementation unless we reuse the same
 * name and force the classloader to load this class instead of the default one (which is done by
 * listing rdfpro-dist jar file first in the classpath). An INFO message is printed in case this
 * class is loaded instead of the default one.
 */
public final class QueryBindingSet implements BindingSet {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_CAPACITY = 8;

    private static final Object DELETED = new Object();

    private static final Logger LOGGER = LoggerFactory.getLogger("eu.fbk.rdfpro");

    private Object[] data;

    private int size;

    private int slots;

    public QueryBindingSet() {
        this(QueryBindingSet.DEFAULT_CAPACITY);
    }

    public QueryBindingSet(final int capacity) {
        int power = QueryBindingSet.DEFAULT_CAPACITY;
        while (power < capacity) {
            power *= 2;
        }
        this.data = new Object[power * 2];
        this.size = 0;
        this.slots = 0;
    }

    public QueryBindingSet(final BindingSet bindings) {
        this(bindings.size());
        this.addAll(bindings);
    }

    public void addAll(final BindingSet bindings) {
        if (bindings instanceof QueryBindingSet) {
            final QueryBindingSet other = (QueryBindingSet) bindings;
            for (int i = 0; i < other.data.length; i += 2) {
                final Object key = other.data[i];
                if (key instanceof String) {
                    this.addBinding((String) key, (Value) other.data[i + 1]);
                }
            }
        } else {
            for (final Binding binding : bindings) {
                this.addBinding(binding.getName(), binding.getValue());
            }
        }
    }

    public void addBinding(final Binding binding) {
        this.addBinding(binding.getName(), binding.getValue());
    }

    public void addBinding(final String name, final Value value) {
        this.setBinding(name, value);
    }

    public void setBinding(final Binding binding) {
        this.setBinding(binding.getName(), binding.getValue());
    }

    public void setBinding(final String name, final Value value) {
        for (int i = this.getSlot(name);; i = this.nextSlot(i)) {
            final Object key = this.data[i];
            if (!(key instanceof String)) {
                this.data[i] = name;
                this.data[i + 1] = value;
                ++this.size;
                if (key == null) {
                    ++this.slots;
                    if (this.slots * 4 > this.data.length) {
                        this.rehashSlots();
                    }
                }
                return;
            } else if (key instanceof String && key.hashCode() == name.hashCode()
                    && key.equals(name)) {
                this.data[i + 1] = value;
                return;
            }
        }
    }

    public void removeBinding(final String name) {
        for (int i = this.getSlot(name);; i = this.nextSlot(i)) {
            final Object key = this.data[i];
            if (key == null) {
                return;
            } else if (key instanceof String && key.hashCode() == name.hashCode()
                    && key.equals(name)) {
                this.data[i] = QueryBindingSet.DELETED;
                this.data[i + 1] = null;
                return;
            }
        }
    }

    public void removeAll(final Collection<String> names) {
        if (names instanceof Set<?>) {
            for (int i = 0; i < this.data.length; i += 2) {
                final Object key = this.data[i];
                if (key instanceof String && names.contains(key)) {
                    this.data[i] = QueryBindingSet.DELETED;
                    this.data[i + 1] = null;
                }
            }
        } else {
            for (final String name : names) {
                this.removeBinding(name);
            }
        }
    }

    public void retainAll(final Collection<String> names) {
        for (int i = 0; i < this.data.length; i += 2) {
            final Object key = this.data[i];
            if (key instanceof String && !names.contains(key)) {
                this.data[i] = QueryBindingSet.DELETED;
                this.data[i + 1] = null;
            }
        }
    }

    @Override
    public Set<String> getBindingNames() {
        return new AbstractSet<String>() {

            @Override
            public boolean contains(final Object object) {
                return object instanceof String
                        && QueryBindingSet.this.hasBinding((String) object);
            }

            @Override
            public int size() {
                return QueryBindingSet.this.size();
            }

            @Override
            public Iterator<String> iterator() {
                return new NameIterator(QueryBindingSet.this.data);
            }

        };
    }

    @Override
    public Value getValue(final String name) {
        for (int i = this.getSlot(name);; i = this.nextSlot(i)) {
            final Object key = this.data[i];
            if (key == null) {
                return null;
            } else if (key instanceof String && key.hashCode() == name.hashCode()
                    && key.equals(name)) {
                return (Value) this.data[i + 1];
            }
        }
    }

    @Override
    public Binding getBinding(final String name) {
        final Value value = this.getValue(name);
        return value == null ? null : new SimpleBinding(name, value);
    }

    @Override
    public boolean hasBinding(final String name) {
        for (int i = this.getSlot(name);; i = this.nextSlot(i)) {
            final Object key = this.data[i];
            if (key == null) {
                return false;
            } else if (key instanceof String && key.hashCode() == name.hashCode()
                    && key.equals(name)) {
                return true;
            }
        }
    }

    @Override
    public Iterator<Binding> iterator() {
        return new BindingIterator(this.data);
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean equals(final Object object) {

        if (object == this) {
            return true;

        } else if (object instanceof QueryBindingSet) {
            final QueryBindingSet other = (QueryBindingSet) object;
            if (other.data.length == this.data.length) {
                for (int i = 0; i < this.data.length; i += 2) {
                    final Object thisKey = this.data[i] instanceof String ? this.data[i] : null;
                    final Object otherKey = other.data[i] instanceof String ? other.data[i] : null;
                    if (!Objects.equal(thisKey, otherKey)
                            || !Objects.equal(this.data[i + 1], other.data[i + 1])) {
                        return false;
                    }
                }
                return true;
            }
            if (this.size() != other.size()) {
                return false;
            }
            for (int i = 0; i < other.data.length; i += 2) {
                if (other.data[i] instanceof String) {
                    final String name = (String) other.data[i];
                    final Value otherValue = (Value) other.data[i + 1];
                    final Value thisValue = this.getValue(name);
                    if (!otherValue.equals(thisValue)) {
                        return false;
                    }
                }
            }
            return true;

        } else if (object instanceof BindingSet) {
            final BindingSet other = (BindingSet) object;
            if (this.size() != other.size()) {
                return false;
            }
            for (final Binding binding : other) {
                final Value thisValue = this.getValue(binding.getName());
                if (!binding.getValue().equals(thisValue)) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int i = 0; i < this.data.length; i += 2) {
            final Object key = this.data[i];
            if (key instanceof String) {
                hashCode = hashCode ^ key.hashCode() ^ this.data[i + 1].hashCode();
            }
        }
        return hashCode;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder(8 * this.data.length);
        builder.append('[');
        String separator = "";
        for (int i = 0; i < this.data.length; i += 2) {
            final Object key = this.data[i];
            if (key instanceof String) {
                builder.append(separator);
                builder.append(key);
                builder.append('=');
                builder.append(this.data[i + 1]);
                separator = ";";
            }
        }
        builder.append(']');
        return builder.toString();
    }

    private void rehashSlots() {
        final Object[] oldData = this.data;
        this.data = new Object[oldData.length * 2];
        for (int j = 0; j < oldData.length; j += 2) {
            if (oldData[j] instanceof String) {
                final String name = (String) oldData[j];
                final Value value = (Value) oldData[j + 1];
                for (int i = this.getSlot(name);; i = this.nextSlot(i)) {
                    if (this.data[i] == null) {
                        this.data[i] = name;
                        this.data[i + 1] = value;
                        break;
                    }
                }
            }
        }
        this.slots = this.size;
    }

    private int getSlot(final String bindingName) {
        return (bindingName.hashCode() & (this.data.length >>> 1) - 1) << 1;
    }

    private int nextSlot(final int slot) {
        final int nextSlot = slot + 2;
        return nextSlot < this.data.length ? nextSlot : 0;
    }

    static {
        QueryBindingSet.LOGGER.debug("Using patched QueryBindingSet class");
    }

    private static abstract class AbstractIterator<T> implements Iterator<T> {

        private final Object[] data;

        private int nextOffset;

        private int removeOffset;

        AbstractIterator(final Object[] data) {
            this.data = data;
            this.nextOffset = 0;
            this.removeOffset = -1;
        }

        abstract T elementAt(Object[] data, int offset);

        @Override
        public boolean hasNext() {
            while (this.nextOffset < this.data.length
                    && !(this.data[this.nextOffset] instanceof String)) {
                this.nextOffset += 2;
            }
            return this.nextOffset < this.data.length;
        }

        @Override
        public T next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            this.removeOffset = this.nextOffset;
            this.nextOffset += 2;
            return this.elementAt(this.data, this.removeOffset);
        }

        @Override
        public void remove() {
            if (this.removeOffset < 0) {
                throw new NoSuchElementException();
            }
            this.data[this.removeOffset] = QueryBindingSet.DELETED;
            this.data[this.removeOffset + 1] = null;
            this.removeOffset = -1;
        }

    }

    private static final class NameIterator extends AbstractIterator<String> {

        NameIterator(final Object[] data) {
            super(data);
        }

        @Override
        String elementAt(final Object[] data, final int offset) {
            return (String) data[offset];
        }

    }

    private static final class BindingIterator extends AbstractIterator<Binding> {

        BindingIterator(final Object[] data) {
            super(data);
        }

        @Override
        Binding elementAt(final Object[] data, final int offset) {
            return new SimpleBinding((String) data[offset], (Value) data[offset + 1]);
        }

    }

}
