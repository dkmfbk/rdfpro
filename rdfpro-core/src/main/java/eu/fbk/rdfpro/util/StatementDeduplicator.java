package eu.fbk.rdfpro.util;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;

public abstract class StatementDeduplicator {

    // initial capacity of hash tables (num statements): 64-128 seems a good range when doing RDFS
    // inference - lower values cause a lot of rehashing; higher values impact negatively on cache
    // locality
    private static final int INITIAL_TABLE_SIZE = 64;

    private static final int LOCK_NUM = 64;

    private static final int LOCK_MASK = 0x3F;

    public static StatementDeduplicator newTotalDeduplicator(final ComparisonMethod method) {

        Preconditions.checkNotNull(method);

        if (method == ComparisonMethod.EQUALS) {
            return new TotalEqualsDeduplicator();
        } else if (method == ComparisonMethod.HASH) {
            return new TotalHashDeduplicator2();
        } else if (method == ComparisonMethod.IDENTITY) {
            return new TotalIdentityDeduplicator();
        } else {
            throw new Error("Unexpected method " + method);
        }
    }

    public static StatementDeduplicator newPartialDeduplicator(final ComparisonMethod method,
            final int numCachedStatements) {

        Preconditions.checkNotNull(method);
        Preconditions.checkArgument(numCachedStatements > 0);

        if (method == ComparisonMethod.EQUALS) {
            return new PartialEqualsDeduplicator(numCachedStatements);
        } else if (method == ComparisonMethod.HASH) {
            return new PartialHashDeduplicator(numCachedStatements);
        } else if (method == ComparisonMethod.IDENTITY) {
            return new PartialIdentityDeduplicator(numCachedStatements);
        } else {
            throw new Error("Unexpected method " + method);
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

    static Hash hash(final Resource subj, final URI pred, final Value obj, final Resource ctx) {
        Hash hash = Hash.combine(Statements.getHash(subj), Statements.getHash(pred),
                Statements.getHash(obj), Statements.getHash(ctx));
        if (hash.getLow() == 0) {
            hash = Hash.fromLongs(hash.getHigh(), 1L);
        }
        return hash;
    }

    private static Object[] rehash(final int[] hashes, final Value[] values) {
        final int[] newHashes = new int[hashes.length * 2];
        final Value[] newValues = new Value[values.length * 2];
        for (int hashIndex = 0; hashIndex < hashes.length; ++hashIndex) {
            final int valueIndex = hashIndex << 2;
            final int hash = hashes[hashIndex];
            int newHashIndex = (hash & 0x7FFFFFFF) % newHashes.length;
            while (newHashes[newHashIndex] != 0) {
                newHashIndex++;
                if (newHashIndex >= newHashes.length) {
                    newHashIndex = 0;
                }
            }
            newHashes[newHashIndex] = hash;
            final int newValueIndex = newHashIndex << 2;
            System.arraycopy(values, valueIndex, newValues, newValueIndex, 4);
        }
        return new Object[] { newHashes, newValues };
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
            this.hashes = new long[2 * INITIAL_TABLE_SIZE];
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

            final long lo = hash.getLow();
            final long hi = hash.getHigh();

            synchronized (this) {
                int slot = ((int) lo & 0x7FFFFFFF) % (this.hashes.length >>> 1) << 1;
                while (true) {
                    final long storedLo = this.hashes[slot];
                    final long storedHi = this.hashes[slot + 1];
                    if (storedLo == 0L) {
                        if (add) {
                            this.hashes[slot] = lo;
                            this.hashes[slot + 1] = hi;
                            ++this.size;
                            if (this.size >= this.hashes.length / 3) { // fill factor 0.66
                                rehash();
                            }
                        }
                        return true;

                    } else if (storedLo == lo && storedHi == hi) {
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
                final long lo = this.hashes[slot];
                final long hi = this.hashes[slot + 1];
                int newSlot = ((int) lo & 0x7FFFFFFF) % (newTable.length >>> 1) << 1;
                while (newTable[newSlot] != 0L) {
                    newSlot += 2;
                    if (newSlot >= newTable.length) {
                        newSlot = 0;
                    }
                }
                newTable[newSlot] = lo;
                newTable[newSlot + 1] = hi;
            }
            this.hashes = newTable;
        }

    }

    private static final class TotalHashDeduplicator2 extends StatementDeduplicator {

        private final Table[] tables;

        TotalHashDeduplicator2() {
            this.tables = new Table[64];
            for (int i = 0; i < this.tables.length; ++i) {
                this.tables[i] = new Table();
            }
        }

        @Override
        boolean total() {
            return true;
        }

        @Override
        boolean process(final Resource subj, final URI pred, final Value obj, final Resource ctx,
                final boolean add) {

            final Hash hash = hash(subj, pred, obj, ctx);

            final long lo = hash.getLow();
            final long hi = hash.getHigh();

            return this.tables[(int) hi & 0x3F].process(lo, hi, add);
        }

        private static class Table {

            private long[] hashes;

            private int size;

            Table() {
                this.hashes = new long[2 * INITIAL_TABLE_SIZE];
                this.size = 0;
            }

            synchronized boolean process(final long lo, final long hi, final boolean add) {

                int slot = ((int) lo & 0x7FFFFFFF) % (this.hashes.length >>> 1) << 1;
                while (true) {
                    final long storedLo = this.hashes[slot];
                    final long storedHi = this.hashes[slot + 1];
                    if (storedLo == 0L) {
                        if (add) {
                            this.hashes[slot] = lo;
                            this.hashes[slot + 1] = hi;
                            ++this.size;
                            if (this.size >= this.hashes.length / 3) { // fill factor 0.66
                                rehash();
                            }
                        }
                        return true;

                    } else if (storedLo == lo && storedHi == hi) {
                        return false;

                    } else {
                        slot += 2;
                        if (slot >= this.hashes.length) {
                            slot = 0;
                        }
                    }
                }
            }

            private void rehash() {
                final long[] newTable = new long[this.hashes.length * 2];
                for (int slot = 0; slot < this.hashes.length; slot += 2) {
                    final long lo = this.hashes[slot];
                    final long hi = this.hashes[slot + 1];
                    int newSlot = ((int) lo & 0x7FFFFFFF) % (newTable.length >>> 1) << 1;
                    while (newTable[newSlot] != 0L) {
                        newSlot += 2;
                        if (newSlot >= newTable.length) {
                            newSlot = 0;
                        }
                    }
                    newTable[newSlot] = lo;
                    newTable[newSlot + 1] = hi;
                }
                this.hashes = newTable;
            }

        }

    }

    private static final class TotalEqualsDeduplicator extends StatementDeduplicator {

        private int[] hashes; // SPOC hashes

        private Value[] values; // 4 values for each statement

        private int size; // # statements

        TotalEqualsDeduplicator() {
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

            int hash = 6661 * subj.hashCode() + 961 * pred.hashCode() + 31 * obj.hashCode()
                    + (ctx == null ? 0 : ctx.hashCode());
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
                                final Object[] pair = rehash(this.hashes, this.values);
                                this.hashes = (int[]) pair[0];
                                this.values = (Value[]) pair[1];
                            }
                        }
                        return true;

                    } else if (this.hashes[hashIndex] == hash) {
                        final int valueIndex = hashIndex << 2;
                        if (subj.equals(this.values[valueIndex])
                                && pred.equals(this.values[valueIndex + 1])
                                && obj.equals(this.values[valueIndex + 2])
                                && (ctx == null && this.values[valueIndex + 3] == null || ctx != null
                                        && ctx.equals(this.values[valueIndex + 3]))) {
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

    }

    private static final class TotalIdentityDeduplicator extends StatementDeduplicator {

        private int[] hashes; // SPOC hashes

        private Value[] values; // 4 values for each statement

        private int size; // # statements

        TotalIdentityDeduplicator() {
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

            int hash = 6661 * System.identityHashCode(subj) + 961 * System.identityHashCode(pred)
                    + 31 * System.identityHashCode(obj)
                    + (ctx == null ? 0 : System.identityHashCode(ctx));
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
                                final Object[] pair = rehash(this.hashes, this.values);
                                this.hashes = (int[]) pair[0];
                                this.values = (Value[]) pair[1];
                            }
                        }
                        return true;

                    } else if (this.hashes[hashIndex] == hash) {
                        final int valueIndex = hashIndex << 2;
                        if (subj == this.values[valueIndex] //
                                && pred == this.values[valueIndex + 1]
                                && obj == this.values[valueIndex + 2]
                                && ctx == this.values[valueIndex + 3]) {
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

            final long hi = hash.getHigh();
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

    private static final class PartialIdentityDeduplicator extends StatementDeduplicator {

        private final int[] hashes;

        private final Value[] values;

        @Nullable
        private final Object[] locks;

        PartialIdentityDeduplicator(final int numCachedStatements) {
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

            int hash = 6661 * System.identityHashCode(subj) + 961 * System.identityHashCode(pred)
                    + 31 * System.identityHashCode(obj)
                    + (ctx == null ? 0 : System.identityHashCode(ctx));
            hash = hash != 0 ? hash : 1;

            final int hashIndex = (hash & 0x7FFFFFFF) % this.hashes.length;
            final int valueIndex = hashIndex << 2;

            synchronized (this.locks[hash & LOCK_MASK]) {
                if (this.hashes[hashIndex] == hash //
                        && subj == this.values[valueIndex]
                        && pred == this.values[valueIndex + 1]
                        && obj == this.values[valueIndex + 2]
                        && ctx == this.values[valueIndex + 3]) {
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

    private static final class PartialEqualsDeduplicator extends StatementDeduplicator {

        private final int[] hashes;

        private final Value[] values;

        @Nullable
        private final Object[] locks;

        PartialEqualsDeduplicator(final int numCachedStatements) {
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

            int hash = 6661 * subj.hashCode() + 961 * pred.hashCode() + 31 * obj.hashCode()
                    + (ctx == null ? 0 : ctx.hashCode());
            hash = hash != 0 ? hash : 1;

            final int hashIndex = (hash & 0x7FFFFFFF) % this.hashes.length;
            final int valueIndex = hashIndex << 2;

            synchronized (this.locks[hash & LOCK_MASK]) {
                if (this.hashes[hashIndex] == hash
                        && subj.equals(this.values[valueIndex])
                        && pred.equals(this.values[valueIndex + 1])
                        && obj.equals(this.values[valueIndex + 2])
                        && (ctx == null && this.values[valueIndex + 3] == null || ctx != null
                                && ctx.equals(this.values[valueIndex + 3]))) {
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
