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

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.Iterators;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF;

final class QuadModelSubModel extends QuadModel {

    private static final long serialVersionUID = 1;

    private static final int PROPERTY_MASK_SIZE = 8 * 8 * 1024 - 1;

    private static final int TYPE_MASK_SIZE = 8 * 8 * 1024 - 1;

    private static final int TYPE_HASH = hash(RDF.TYPE);

    private static final Sorting.ArrayComparator<Value> VALUE_ARRAY_COMPARATOR //
            = new Sorting.ArrayComparator<Value>() {

                @Override
                public int size() {
                    return 4;
                }

                @Override
                public int compare(final Value[] leftArray, final int leftIndex,
                        final Value[] rightArray, final int rightIndex) {
                    // POSC order
                    int result = hash(leftArray[leftIndex + 1]) - hash(rightArray[rightIndex + 1]);
                    if (result == 0) {
                        result = hash(leftArray[leftIndex + 2]) - hash(rightArray[rightIndex + 2]);
                        if (result == 0) {
                            result = hash(leftArray[leftIndex]) - hash(rightArray[rightIndex]);
                            if (result == 0) {
                                result = hash(leftArray[leftIndex + 3])
                                        - hash(rightArray[rightIndex + 3]);
                            }
                        }
                    }
                    return result;
                }

            };

    private final QuadModel model;

    private final BitSet propertyMask;

    private final BitSet typeMask;

    private final Value[] data;

    QuadModelSubModel(final QuadModel model, final Collection<Statement> stmts) {

        // stmts must not contain duplicates!

        // Store model and size parameters
        this.model = model;

        // Initialize bitmasks for properties and types
        this.propertyMask = new BitSet(PROPERTY_MASK_SIZE);
        this.typeMask = new BitSet(TYPE_MASK_SIZE);

        // Initialize storage for delta statements
        this.data = new Value[4 * stmts.size()];

        int index = 0;
        for (final Statement stmt : stmts) {
            // Store statement
            this.data[index] = stmt.getSubject();
            this.data[index + 1] = stmt.getPredicate();
            this.data[index + 2] = stmt.getObject();
            this.data[index + 3] = stmt.getContext();
            index += 4;

            // Update masks
            final int predHash = hash(stmt.getPredicate());
            this.propertyMask.set(predHash % PROPERTY_MASK_SIZE);
            if (predHash == TYPE_HASH) {
                final int objHash = hash(stmt.getObject());
                this.typeMask.set(objHash % TYPE_MASK_SIZE);
            }
        }
        Sorting.sort(VALUE_ARRAY_COMPARATOR, this.data);
    }

    @Override
    protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, @Nullable final Resource ctx) {

        // Return 0 if view is empty
        if (this.data.length == 0) {
            return 0;
        }

        // Check masks, if possible
        if (pred != null) {
            final int predHash = hash(pred);
            if (!this.propertyMask.get(predHash % PROPERTY_MASK_SIZE)) {
                return 0;
            }
            if (obj != null && predHash == TYPE_HASH) {
                final int objHash = hash(obj);
                if (!this.typeMask.get(objHash % TYPE_MASK_SIZE)) {
                    return 0;
                }
            }
        }

        // Otherwise, simply delegate to the wrapped model, limiting the result to this size
        return Math.min(this.data.length / 4, this.model.sizeEstimate(subj, pred, obj,
                ctx == null ? CTX_ANY : new Resource[] { ctx }));
    }

    @Override
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs) {

        // In case of a wildcard <?s ?p ?o ?c> returns all the statements in the delta
        if (subj == null && pred == null && obj == null && ctxs.length == 0) {
            return new Iterator<Statement>() {

                private int offset = 0;

                @Override
                public boolean hasNext() {
                    return this.offset < QuadModelSubModel.this.data.length;
                }

                @Override
                public Statement next() {
                    final Statement stmt = statementAt(this.offset);
                    this.offset += 4;
                    return stmt;
                }

            };
        }

        // Compute prefix
        final Value[] prefix = prefixFor(subj, pred, obj);

        // Delegate to model without filtering (thus going towards a naive approach) in case
        // there is no way to exploit the order of quads and their number in the model is less
        // than the delta size
        if (prefix.length == 0) {
            final int estimate = this.model.sizeEstimate(subj, pred, obj, ctxs);
            if (estimate < this.data.length / 4) {
                return this.model.iterator(subj, pred, obj, ctxs);
            }
        }

        // Compute start index in the value array
        final int startIndex = indexOf(prefix, 0, this.data.length);
        if (startIndex < 0) {
            return Collections.emptyIterator();
        }

        // Otherwise, iterate starting at computed index, stopping when prefix does not match
        return new Iterator<Statement>() {

            private int offset = startIndex;

            private Statement next = null;

            @Override
            public boolean hasNext() {
                if (this.next != null) {
                    return true;
                }
                final Value[] data = QuadModelSubModel.this.data;
                while (this.offset < data.length) {
                    if (prefixCompare(prefix, this.offset) != 0) {
                        this.offset = data.length;
                        return false;
                    }
                    if ((subj == null || subj.equals(data[this.offset]))
                            && (pred == null || pred.equals(data[this.offset + 1]))
                            && (obj == null || obj.equals(data[this.offset + 2]))) {
                        if (ctxs.length == 0) {
                            this.next = statementAt(this.offset);
                            this.offset += 4;
                            return true;
                        }
                        final Resource c = (Resource) data[this.offset + 3];
                        for (final Resource ctx : ctxs) {
                            if (Objects.equals(c, ctx)) {
                                this.next = statementAt(this.offset);
                                this.offset += 4;
                                return true;
                            }
                        }
                    }
                    this.offset += 4;
                }
                return false;
            }

            @Override
            public Statement next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final Statement stmt = this.next;
                this.next = null;
                return stmt;
            }

        };
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
    protected Namespace doSetNamespace(final String prefix, final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int doSize(final Resource subj, final IRI pred, final Value obj,
            final Resource[] ctxs) {
        if (subj == null && pred == null && obj == null && ctxs.length == 0) {
            return this.data.length / 4;
        } else if (sizeEstimate(subj, pred, obj, ctxs) == 0) {
            return 0;
        } else {
            return Iterators.size(iterator(subj, pred, obj, ctxs));
        }
    }

    @Override
    protected boolean doAdd(final Resource subj, final IRI pred, final Value obj,
            final Resource[] ctxs) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doRemove(final Resource subj, final IRI pred, final Value obj,
            final Resource[] ctxs) {
        throw new UnsupportedOperationException(); // not invoked
    }

    @Override
    protected Value doNormalize(final Value value) {
        return this.model.normalize(value);
    }

    private Value[] prefixFor(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj) {

        // Compute prefix length
        int prefixLength = 0;
        if (pred != null) {
            ++prefixLength;
            if (obj != null) {
                ++prefixLength;
                if (subj != null) {
                    ++prefixLength;
                }
            }
        }

        // Compute prefix
        final Value[] prefix = new Value[prefixLength];
        if (prefixLength >= 1) {
            prefix[0] = pred;
        }
        if (prefixLength >= 2) {
            prefix[1] = obj;
        }
        if (prefixLength >= 3) {
            prefix[2] = subj;
        }
        return prefix;
    }

    private int prefixCompare(final Value[] prefix, final int offset) {
        int result = 0;
        if (prefix.length >= 1) {
            result = hash(this.data[offset + 1]) - hash(prefix[0]);
            if (result == 0 && prefix.length >= 2) {
                result = hash(this.data[offset + 2]) - hash(prefix[1]);
                if (result == 0 && prefix.length >= 3) {
                    result = hash(this.data[offset]) - hash(prefix[2]);
                }
            }
        }
        return result;
    }

    private Statement statementAt(final int offset) {
        final Resource subj = (Resource) QuadModelSubModel.this.data[offset];
        final IRI pred = (IRI) QuadModelSubModel.this.data[offset + 1];
        final Value obj = QuadModelSubModel.this.data[offset + 2];
        final Resource ctx = (Resource) QuadModelSubModel.this.data[offset + 3];
        return ctx == null ? Statements.VALUE_FACTORY.createStatement(subj, pred, obj)
                : Statements.VALUE_FACTORY.createStatement(subj, pred, obj, ctx);
    }

    private int indexOf(final Value[] prefix, final int lo, final int hi) {
        if (lo >= hi) {
            return -1;
        }
        if (prefix.length == 0) {
            return lo;
        }
        final int mid = (lo >>> 2) + (hi >>> 2) >>> 1 << 2;
        final int c = prefixCompare(prefix, mid);
        if (c < 0) {
            return indexOf(prefix, mid + 4, hi);
        } else if (c > 0) {
            return indexOf(prefix, lo, mid);
        } else if (mid > lo) {
            final int index = indexOf(prefix, lo, mid);
            return index >= 0 ? index : mid;
        }
        return mid;
    }

    private static int hash(@Nullable final Value value) {
        return value == null ? 0 : value.hashCode() & 0x7FFFFFFF;
    }

}