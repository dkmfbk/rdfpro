package eu.fbk.rdfpro.rules.util;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;

public abstract class StatementDeduplicator {

    public static StatementDeduplicator newPartialDeduplicator(final int numCachedStatements,
            final boolean identityComparison, final boolean threadSafe) {
        return new PartialDeduplicator(numCachedStatements, identityComparison, threadSafe);
    }

    // TODO: exact deduplicator

    public abstract boolean isNew(final Resource subj, final URI pred, final Value obj,
            @Nullable final Resource ctx);

    public final boolean isNew(final Statement stmt) {
        return isNew(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
    }

    public final RDFHandler deduplicate(final RDFHandler handler) {
        return new AbstractRDFHandlerWrapper(handler) {

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                if (isNew(stmt)) {
                    super.handleStatement(stmt);
                }
            }

        };
    }

    private static final class PartialDeduplicator extends StatementDeduplicator {

        private static final int LOCK_NUM = 64;

        private static final int LOCK_MASK = 0x3F;

        private final int[] hashes;

        private final Value[] values;

        private final boolean identityComparison;

        @Nullable
        private Object[] locks;

        PartialDeduplicator(final int numCachedStatements, final boolean identityComparison,
                final boolean threadSafe) {

            this.hashes = new int[numCachedStatements];
            this.values = new Value[numCachedStatements * 4];
            this.identityComparison = identityComparison;
            if (!threadSafe) {
                this.locks = null;
            } else {
                this.locks = new Object[LOCK_NUM];
                for (int i = 0; i < LOCK_NUM; ++i) {
                    this.locks[i] = new Object();
                }
            }
        }

        @Override
        public boolean isNew(final Resource subj, final URI pred, final Value obj,
                final Resource ctx) {

            final int hash = 6661 * hash(subj) + 961 * hash(pred) + 31 * hash(obj)
                    + (ctx == null ? 0 : hash(ctx));

            if (this.locks == null) {
                return isNew(subj, pred, obj, ctx, hash);
            } else {
                synchronized (this.locks[hash & LOCK_MASK]) {
                    return isNew(subj, pred, obj, ctx, hash);
                }
            }
        }

        private boolean isNew(final Resource subj, final URI pred, final Value obj,
                final Resource ctx, final int hash) {

            final int hashIndex = (hash & 0x7FFFFFFF) % this.hashes.length;
            final int valueIndex = hashIndex << 2;

            if (this.hashes[hashIndex] == hash) {
                if (equals(subj, this.values[valueIndex])
                        && equals(pred, this.values[valueIndex + 1])
                        && equals(obj, this.values[valueIndex + 2])
                        && (ctx == null && this.values[valueIndex + 3] == null || ctx != null
                                && equals(ctx, this.values[valueIndex + 3]))) {
                    return false;
                }
            }

            this.hashes[hashIndex] = hash;
            this.values[valueIndex] = subj;
            this.values[valueIndex + 1] = pred;
            this.values[valueIndex + 2] = obj;
            this.values[valueIndex + 3] = ctx;
            return true;
        }

        private boolean equals(final Value first, final Value second) {
            return this.identityComparison ? first == second : first.equals(second);
        }

        private int hash(final Value value) {
            return this.identityComparison ? System.identityHashCode(value) : value.hashCode();
        }

    }

}
