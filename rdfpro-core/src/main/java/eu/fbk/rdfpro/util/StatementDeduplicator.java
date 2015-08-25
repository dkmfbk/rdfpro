package eu.fbk.rdfpro.util;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;

public abstract class StatementDeduplicator {

    private static final Hash NULL_HASH = Hash.murmur3("\u0001");

    // initial capacity of hash tables (num statements): 64-128 seems a good range when doing RDFS
    // inference - lower values cause a lot of rehashing; higher values impact negatively on cache
    // locality
    private static final int INITIAL_TABLE_SIZE = 64;

    private static final int LOCK_NUM = 64;

    private static final int LOCK_MASK = 0x3F;

    public static StatementDeduplicator newTotalDeduplicator(final ComparisonMethod method) {

        Preconditions.checkNotNull(method);

        if (method == ComparisonMethod.HASH) {
            return new TotalHashDeduplicator();
        } else {
            return new TotalValueDeduplicator(method == ComparisonMethod.IDENTITY);
        }
    }

    public static StatementDeduplicator newPartialDeduplicator(final ComparisonMethod method,
            final int numCachedStatements) {

        Preconditions.checkNotNull(method);
        Preconditions.checkArgument(numCachedStatements > 0);

        if (method == ComparisonMethod.HASH) {
            return new PartialHashDeduplicator(numCachedStatements);
        } else {
            return new PartialValueDeduplicator(numCachedStatements,
                    method == ComparisonMethod.IDENTITY);
        }
    }

    public static StatementDeduplicator newChainedDeduplicator(
            final StatementDeduplicator... deduplicators) {

        for (final StatementDeduplicator deduplicator : deduplicators) {
            Preconditions.checkNotNull(deduplicator);
        }

        return deduplicators.length == 1 ? deduplicators[0] : new ChainedDeduplicator(
                deduplicators.clone());
    }

    public final boolean isTotal() {
        return total();
    }

    public final boolean test(final Statement stmt) {
        return process(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                stmt.getContext(), false);
    }

    public final boolean test(final Resource subj, final URI pred, final Value obj,
            @Nullable final Resource ctx) {
        return process(subj, pred, obj, ctx, false);
    }

    public final boolean add(final Statement stmt) {
        return process(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                stmt.getContext(), true);
    }

    public final boolean add(final Resource subj, final URI pred, final Value obj,
            @Nullable final Resource ctx) {
        return process(subj, pred, obj, ctx, true);
    }

    public final RDFHandler deduplicate(final RDFHandler handler, final boolean add) {
        return new AbstractRDFHandlerWrapper(handler) {

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                if (process(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                        stmt.getContext(), add)) {
                    super.handleStatement(stmt);
                }
            }

        };
    }

    abstract boolean total();

    abstract boolean process(Resource subj, URI pred, Value obj, @Nullable Resource ctx,
            boolean mark);

    static boolean equals(final boolean identityComparison, final Value first, final Value second) {
        return identityComparison ? first == second : first.equals(second);
    }

    static int hashCode(final boolean identityComparison, final Value value) {
        return identityComparison ? System.identityHashCode(value) : value.hashCode();
    }

    static Hash hash(final Resource subj, final URI pred, final Value obj, final Resource ctx) {
        return Hash.combine(hash(subj), hash(pred), hash(obj), hash(ctx));
    }

    static Hash hash(final Value value) {
        if (value instanceof URI) {
            return Hash.murmur3("\u0001", value.stringValue());
        } else if (value instanceof BNode) {
            return Hash.murmur3("\u0002", ((BNode) value).getID());
        } else if (value instanceof Literal) {
            final Literal l = (Literal) value;
            if (l.getLanguage() != null) {
                return Hash.murmur3("\u0003", l.getLanguage(), "\u0006", l.getLabel());
            } else if (l.getDatatype() != null) {
                return Hash.murmur3("\u0004", l.getDatatype().stringValue(), "\u0006",
                        l.getLabel());
            } else {
                return Hash.murmur3("\u0005", l.getLabel());
            }
        } else if (value == null) {
            return NULL_HASH;
        } else {
            throw new Error("Unexpected value: " + value);
        }
    }

    public static enum ComparisonMethod {

        IDENTITY,

        EQUALS,

        HASH

    }

    private static final class TotalHashDeduplicator extends StatementDeduplicator {

        private long[] hashes;

        private int size;

        TotalHashDeduplicator() {
            this.hashes = new long[2 * INITIAL_TABLE_SIZE]; // 16KB initial size
            this.size = 0;
        }

        @Override
        boolean total() {
            return true;
        }

        @Override
        boolean process(final Resource subj, final URI pred, final Value obj, final Resource ctx,
                final boolean add) {

            final Hash hash = hash(subj, pred, obj, ctx);

            final long hi = hash.getHigh() != 0 ? hash.getHigh() : 1L;
            final long lo = hash.getLow();

            synchronized (this) {
                int slot = ((int) lo & 0x7FFFFFFF) % (this.hashes.length >>> 1) << 1;
                while (true) {
                    final long storedHi = this.hashes[slot];
                    final long storedLo = this.hashes[slot + 1];
                    if (storedHi == 0L) {
                        if (add) {
                            this.hashes[slot] = hi;
                            this.hashes[slot + 1] = lo;
                            ++this.size;
                            if (this.size >= this.hashes.length / 3) { // fill factor 0.66
                                rehash();
                            }
                        }
                        return true;

                    } else if (storedHi == hi && storedLo == lo) {
                        return false;

                    } else {
                        slot += 2;
                        if (slot >= this.hashes.length) {
                            slot = 0;
                        }
                    }
                }
            }
        }

        private void rehash() {
            final long[] newTable = new long[this.hashes.length * 2];
            for (int slot = 0; slot < this.hashes.length; slot += 2) {
                final long hi = this.hashes[slot];
                final long lo = this.hashes[slot + 1];
                int newSlot = ((int) lo & 0x7FFFFFFF) % (newTable.length >>> 1) << 1;
                while (newTable[newSlot] != 0L) {
                    newSlot += 2;
                    if (newSlot >= newTable.length) {
                        newSlot = 0;
                    }
                }
                newTable[newSlot] = hi;
                newTable[newSlot + 1] = lo;
            }
            this.hashes = newTable;
        }

    }

    private static final class TotalValueDeduplicator extends StatementDeduplicator {

        private final boolean identity;

        private int[] hashes; // SPOC hashes

        private Value[] values; // 4 values for each statement

        private int size; // # statements

        TotalValueDeduplicator(final boolean identityComparison) {
            this.identity = identityComparison;
            this.hashes = new int[INITIAL_TABLE_SIZE];
            this.values = new Value[INITIAL_TABLE_SIZE * 4];
        }

        @Override
        boolean total() {
            return true;
        }

        @Override
        boolean process(final Resource subj, final URI pred, final Value obj, final Resource ctx,
                final boolean add) {

            int hash = 6661 * hashCode(this.identity, subj) + 961 * hashCode(this.identity, pred)
                    + 31 * hashCode(this.identity, obj)
                    + (ctx == null ? 0 : hashCode(this.identity, ctx));
            hash = hash != 0 ? hash : 1;

            synchronized (this) {
                int hashIndex = (hash & 0x7FFFFFFF) % this.hashes.length;
                while (true) {
                    if (this.hashes[hashIndex] == 0) {
                        if (add) {
                            final int valueIndex = hashIndex << 2;
                            this.hashes[hashIndex] = hash;
                            this.values[valueIndex] = subj;
                            this.values[valueIndex + 1] = pred;
                            this.values[valueIndex + 2] = obj;
                            this.values[valueIndex + 3] = ctx;
                            ++this.size;
                            if (this.size >= (this.hashes.length << 1) / 3) { // fill factor 0.66
                                rehash();
                            }
                        }
                        return true;

                    } else if (this.hashes[hashIndex] == hash) {
                        final int valueIndex = hashIndex << 2;
                        if (equals(this.identity, subj, this.values[valueIndex])
                                && equals(this.identity, pred, this.values[valueIndex + 1])
                                && equals(this.identity, obj, this.values[valueIndex + 2])
                                && (ctx == null && this.values[valueIndex + 3] == null || ctx != null
                                        && equals(this.identity, ctx, this.values[valueIndex + 3]))) {
                            return false;
                        }

                    }

                    ++hashIndex;
                    if (hashIndex == this.hashes.length) {
                        hashIndex = 0;
                    }
                }
            }
        }

        private void rehash() {
            final int[] newHashes = new int[this.hashes.length * 2];
            final Value[] newValues = new Value[this.values.length * 2];
            for (int hashIndex = 0; hashIndex < this.hashes.length; ++hashIndex) {
                final int valueIndex = hashIndex << 2;
                final int hash = this.hashes[hashIndex];
                int newHashIndex = (hash & 0x7FFFFFFF) % newHashes.length;
                while (newHashes[newHashIndex] != 0) {
                    newHashIndex++;
                    if (newHashIndex >= newHashes.length) {
                        newHashIndex = 0;
                    }
                }
                newHashes[newHashIndex] = hash;
                final int newValueIndex = newHashIndex << 2;
                System.arraycopy(this.values, valueIndex, newValues, newValueIndex, 4);
            }
            this.hashes = newHashes;
            this.values = newValues;
        }

    }

    private static final class PartialHashDeduplicator extends StatementDeduplicator {

        private final long[] hashes;

        private final Object[] locks;

        PartialHashDeduplicator(final int numCachedStatements) {
            this.hashes = new long[numCachedStatements * 2];
            this.locks = new Object[LOCK_NUM];
            for (int i = 0; i < LOCK_NUM; ++i) {
                this.locks[i] = new Object();
            }
        }

        @Override
        boolean total() {
            return false;
        }

        @Override
        boolean process(final Resource subj, final URI pred, final Value obj,
                @Nullable final Resource ctx, final boolean add) {

            final Hash hash = hash(subj, pred, obj, ctx);

            final long hi = hash.getHigh() != 0 ? hash.getHigh() : 1;
            final long lo = hash.getLow();

            final int index = ((int) lo & 0x7FFFFFFF) % (this.hashes.length >>> 1) << 1;

            synchronized (this.locks[index & LOCK_MASK]) {
                if (this.hashes[index] == hi && this.hashes[index + 1] == lo) {
                    return false;
                }
                if (add) {
                    this.hashes[index] = hi;
                    this.hashes[index + 1] = lo;
                }
            }

            return true;
        }

    }

    private static final class PartialValueDeduplicator extends StatementDeduplicator {

        private final boolean identity;

        private final int[] hashes;

        private final Value[] values;

        @Nullable
        private final Object[] locks;

        PartialValueDeduplicator(final int numCachedStatements, final boolean identityComparison) {
            this.identity = identityComparison;
            this.hashes = new int[numCachedStatements];
            this.values = new Value[numCachedStatements * 4];
            this.locks = new Object[LOCK_NUM];
            for (int i = 0; i < LOCK_NUM; ++i) {
                this.locks[i] = new Object();
            }
        }

        @Override
        boolean total() {
            return false;
        }

        @Override
        boolean process(final Resource subj, final URI pred, final Value obj,
                @Nullable final Resource ctx, final boolean add) {

            int hash = 6661 * hashCode(this.identity, subj) + 961 * hashCode(this.identity, pred)
                    + 31 * hashCode(this.identity, obj)
                    + (ctx == null ? 0 : hashCode(this.identity, ctx));
            hash = hash != 0 ? hash : 1;

            final int hashIndex = (hash & 0x7FFFFFFF) % this.hashes.length;
            final int valueIndex = hashIndex << 2;

            synchronized (this.locks[hash & LOCK_MASK]) {
                if (this.hashes[hashIndex] == hash
                        && equals(this.identity, subj, this.values[valueIndex])
                        && equals(this.identity, pred, this.values[valueIndex + 1])
                        && equals(this.identity, obj, this.values[valueIndex + 2])
                        && (ctx == null && this.values[valueIndex + 3] == null || ctx != null
                                && equals(this.identity, ctx, this.values[valueIndex + 3]))) {
                    return false;
                }
                if (add) {
                    this.hashes[hashIndex] = hash;
                    this.values[valueIndex] = subj;
                    this.values[valueIndex + 1] = pred;
                    this.values[valueIndex + 2] = obj;
                    this.values[valueIndex + 3] = ctx;
                }
            }

            return true;
        }

    }

    private static class ChainedDeduplicator extends StatementDeduplicator {

        private final StatementDeduplicator[] deduplicators;

        private final boolean total;

        ChainedDeduplicator(final StatementDeduplicator[] deduplicators) {

            boolean total = deduplicators.length > 0;
            for (final StatementDeduplicator deduplicator : deduplicators) {
                if (!deduplicator.isTotal()) {
                    total = false;
                    break;
                }
            }

            this.deduplicators = deduplicators;
            this.total = total;
        }

        @Override
        boolean total() {
            return this.total;
        }

        @Override
        boolean process(final Resource subj, final URI pred, final Value obj, final Resource ctx,
                final boolean add) {

            for (final StatementDeduplicator deduplicator : this.deduplicators) {
                if (!deduplicator.process(subj, pred, obj, ctx, true)) {
                    return false;
                }
            }
            return true;
        }

    }

}
