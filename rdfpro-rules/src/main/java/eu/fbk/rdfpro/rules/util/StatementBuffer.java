package eu.fbk.rdfpro.rules.util;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.collect.Lists;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.rules.model.QuadModel;

/**
 * A statement buffer behaving as a list with duplicates where statements can only be added and
 * retrieved, but never removed.
 */
public final class StatementBuffer extends AbstractCollection<Statement> implements
        Supplier<RDFHandler> {

    private static final int BLOCK_SIZE = 4 * 1024; // 1K quads, 4K values, 16K bytes

    private final List<Value[]> blocks;

    private int offset;

    @Nullable
    private transient int[] buckets;

    public StatementBuffer() {
        this.blocks = Lists.newArrayList();
        this.offset = BLOCK_SIZE;
    }

    @Override
    public boolean isEmpty() {
        return this.blocks.isEmpty();
    }

    @Override
    public int size() {
        return this.blocks.isEmpty() ? 0
                : ((this.blocks.size() - 1) * BLOCK_SIZE + this.offset) / 4;
    }

    public boolean contains(final Resource subj, final URI pred, final Value obj,
            @Nullable final Resource ctx) {

        // Retrieve (build if necessary) the hash index
        final int[] buckets = getBuckets();

        // Lookup the statement using the index
        final int hash = hash(subj, pred, obj, ctx);
        int slot = (hash & 0x7FFFFFFF) % buckets.length;
        while (true) {
            if (buckets[slot] == 0) {
                return false;
            } else {
                final int pointer = buckets[slot] - 4;
                final int thisIndex = pointer / BLOCK_SIZE;
                final int offset = pointer % BLOCK_SIZE;
                final Value[] block = this.blocks.get(thisIndex);
                if (block[offset].equals(subj) && block[offset + 1].equals(pred)
                        && block[offset + 2].equals(obj) && Objects.equals(block[offset + 3], ctx)) {
                    return true;
                }
            }
            slot = (slot + 1) % buckets.length;
        }
    }

    @Override
    public boolean contains(final Object object) {

        if (!(object instanceof Statement)) {
            return false;
        }

        final Statement stmt = (Statement) object;
        return contains(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                stmt.getContext());
    }

    @Override
    public Iterator<Statement> iterator() {
        return new Iterator<Statement>() {

            private Value[] block = StatementBuffer.this.blocks.isEmpty() ? null
                    : StatementBuffer.this.blocks.get(0);

            private int index = 0;

            private int offset = 0;

            private int maxOffset = StatementBuffer.this.blocks.size() > 1 ? BLOCK_SIZE
                    : StatementBuffer.this.offset;

            @Override
            public boolean hasNext() {
                return this.block != null;
            }

            @Override
            public Statement next() {

                // Fail if there are no more elements to retrieve
                if (this.block == null) {
                    throw new NoSuchElementException();
                }

                // Otherwise, retrieve the SPOC components of the next statement to return
                final Resource subj = (Resource) this.block[this.offset++];
                final URI pred = (URI) this.block[this.offset++];
                final Value obj = this.block[this.offset++];
                final Resource ctx = (Resource) this.block[this.offset++];

                // Update index / offset / block variables; block set to null if iterator exhaust
                if (this.offset >= this.maxOffset) {
                    ++this.index;
                    if (this.index < StatementBuffer.this.blocks.size()) {
                        this.block = StatementBuffer.this.blocks.get(this.index);
                        this.offset = 0;
                        this.maxOffset = this.index < StatementBuffer.this.blocks.size() - 1 ? BLOCK_SIZE
                                : StatementBuffer.this.offset;
                    } else {
                        this.block = null;
                    }
                }

                // Return the statement
                return new ContextStatementImpl(subj, pred, obj, ctx);
            }

        };
    }

    public int toModel(final QuadModel model, final boolean add,
            @Nullable final RDFHandler callback) throws RDFHandlerException {

        // Notify the callback handler, if any
        if (callback != null) {
            callback.startRDF();
        }

        // Iterate over the statements in the buffer
        int numChanges = 0;
        for (int index = 0; index < this.blocks.size(); ++index) {
            final Value[] block = this.blocks.get(index);
            final int maxOffset = index < this.blocks.size() - 1 ? BLOCK_SIZE : this.offset;
            for (int offset = 0; offset < maxOffset; offset += 4) {

                // Retrieve SPOC components of current statement
                Resource subj = (Resource) block[offset];
                URI pred = (URI) block[offset + 1];
                Value obj = block[offset + 2];
                Resource ctx = (Resource) block[offset + 3];

                // Either add or remove the statement to/from the model
                boolean modified;
                if (add) {
                    subj = model.normalize(subj);
                    pred = model.normalize(pred);
                    obj = model.normalize(obj);
                    ctx = model.normalize(ctx);
                    modified = model.add(subj, pred, obj, ctx);
                } else {
                    modified = model.remove(subj, pred, obj, ctx);
                }

                // If the model was modified as a result of the operation, increment changes
                // counter and notify the callback, if any
                if (modified) {
                    ++numChanges;
                    if (callback != null) {
                        callback.handleStatement(new ContextStatementImpl(subj, pred, obj, ctx));
                    }
                }
            }
        }

        // Notify the callback handler, if any
        if (callback != null) {
            callback.endRDF();
        }

        // Return the number of statements actually added to or deleted from the model
        return numChanges;
    }

    public void toHandler(final RDFHandler handler) throws RDFHandlerException {

        // Forward statements to supplied handler, calling also startRDF and endRDF
        handler.startRDF();
        for (int index = 0; index < this.blocks.size(); ++index) {
            final Value[] block = this.blocks.get(index);
            final int maxOffset = index < this.blocks.size() - 1 ? BLOCK_SIZE : this.offset;
            for (int offset = 0; offset < maxOffset; offset += 4) {
                handler.handleStatement(new ContextStatementImpl((Resource) block[offset],
                        (URI) block[offset + 1], block[offset + 2], (Resource) block[offset + 3]));
            }
        }
        handler.endRDF();
    }

    public synchronized boolean add(final Resource subj, final URI pred, final Value obj,
            @Nullable final Resource ctx) {

        // Invalidate hash index
        this.buckets = null;

        // Retrieve the block where to add the statement; add a new block if necessary
        Value[] block;
        if (this.offset < BLOCK_SIZE) {
            block = this.blocks.get(this.blocks.size() - 1);
        } else {
            block = new Value[BLOCK_SIZE];
            this.blocks.add(block);
            this.offset = 0;
        }

        // Store the statement in the block and increment the offset in the block
        block[this.offset] = subj;
        block[this.offset + 1] = pred;
        block[this.offset + 2] = obj;
        block[this.offset + 3] = ctx;
        this.offset += 4;

        // Always return true (buffer always modified)
        return true;
    }

    @Override
    public boolean add(final Statement stmt) {
        return add(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
    }

    @Override
    public RDFHandler get() {
        return new Appender();
    }

    private int[] getBuckets() {

        // Create hash index at first access. Elements of the hash table are pointers to values in
        // the combined block arrays (pointer incremeted by 4 to avoid pointer = 0)
        if (this.buckets == null) {
            final int size = size();
            final int[] buckets = new int[Math.max(4, Integer.highestOneBit(size) * 4) - 1];
            int pointer = 4; // never use 0
            for (int index = 0; index < this.blocks.size(); ++index) {
                final Value[] block = this.blocks.get(index);
                final int maxOffset = index < this.blocks.size() - 1 ? BLOCK_SIZE : this.offset;
                for (int offset = 0; offset < maxOffset; offset += 4) {
                    final int hash = hash(block[offset], block[offset + 1], block[offset + 2],
                            block[offset + 3]);
                    int slot = (hash & 0x7FFFFFFF) % buckets.length;
                    while (buckets[slot] != 0) {
                        slot = (slot + 1) % buckets.length;
                    }
                    buckets[slot] = pointer;
                    pointer += 4;
                }
            }
            this.buckets = buckets;
        }
        return this.buckets;
    }

    private synchronized void append(final Value[] block, final int blockLength) {

        // Invalidate hash index
        this.buckets = null;

        // Handle two cases
        if (blockLength == block.length) {

            // (1) A full block is being added. Don't copy, just insert the block in the list
            if (this.offset >= BLOCK_SIZE) {
                this.blocks.add(block);
            } else {
                final Value[] last = this.blocks.remove(this.blocks.size() - 1);
                this.blocks.add(block);
                this.blocks.add(last);
            }

        } else {

            // (2) A partial block is being added. Copy the content of the block specified
            // into buffer blocks, possibly allocating new blocks if necessary.
            int offset = 0;
            while (offset < blockLength) {
                Value[] thisBlock;
                if (this.offset < BLOCK_SIZE) {
                    thisBlock = this.blocks.get(this.blocks.size() - 1);
                } else {
                    thisBlock = new Value[BLOCK_SIZE];
                    this.blocks.add(thisBlock);
                    this.offset = 0;
                }
                final int length = Math.min(blockLength - offset, BLOCK_SIZE - this.offset);
                System.arraycopy(block, offset, thisBlock, this.offset, length);
                offset += length;
                this.offset += length;
            }
        }
    }

    private static int hash(final Value subj, final Value pred, final Value obj, final Value ctx) {
        // Return an hash code depending on all four SPOC components
        return 6661 * subj.hashCode() + 961 * pred.hashCode() + 31 * obj.hashCode()
                + (ctx == null ? 0 : ctx.hashCode());
    }

    private final class Appender extends AbstractRDFHandler {

        private Value[] block;

        private int offset;

        private Appender() {
            this.block = null;
            this.offset = 0;
        }

        @Override
        public void startRDF() {

            // Allocate a local block
            this.block = new Value[BLOCK_SIZE];
            this.offset = 0;
        }

        @Override
        public void handleStatement(final Statement stmt) {

            // Extract components
            final Resource subj = stmt.getSubject();
            final URI pred = stmt.getPredicate();
            final Value obj = stmt.getObject();
            final Resource ctx = stmt.getContext();

            // Append the SPOC components to the local block
            this.block[this.offset++] = subj;
            this.block[this.offset++] = pred;
            this.block[this.offset++] = obj;
            this.block[this.offset++] = ctx;

            // If the local block is full, copy its content to the buffer (this requires
            // synchronization)
            if (this.offset == this.block.length) {
                append(this.block, this.block.length);
                this.block = new Value[BLOCK_SIZE];
                this.offset = 0;
            }

        }

        @Override
        public void endRDF() {

            // Flush the content of the local block to the buffer, if necessary, and release
            // the block to free memory
            if (this.offset > 0) {
                append(this.block, this.offset);
                this.offset = 0;
            }
            this.block = null;
        }

    }

}
