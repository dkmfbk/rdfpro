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
import java.io.ObjectOutputStream;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

final class QuadModelImpl extends QuadModel {

    private static final int INITIAL_VALUE_TABLE_SIZE = 256 - 1;

    private static final int INITIAL_STATEMENT_TABLE_SIZE = 256 - 1;

    private static final ModelIRI NULL_VALUE = new ModelIRI(null, "sesame:null");

    private static final ModelStatement NULL_STATEMENT = new ModelStatement(
            QuadModelImpl.NULL_VALUE, QuadModelImpl.NULL_VALUE, QuadModelImpl.NULL_VALUE,
            QuadModelImpl.NULL_VALUE);

    private static final int SUBJ = 0;

    private static final int PRED = 1;

    private static final int OBJ = 2;

    private static final int CTX = 3;

    private static final long serialVersionUID = 1L;

    private final Map<String, Namespace> namespaces;

    private final StringIndex stringIndex;

    private ModelValue[] valueTable;

    private int valueCount;

    private int valueSlots;

    private final ModelIRI valueNil;

    private final ModelIRI valueLang;

    private ModelStatement[] statementTable;

    private int statementSlots;

    private int statementCount;

    private int statementZombies;

    public QuadModelImpl() {
        this.namespaces = new HashMap<>();
        this.stringIndex = new StringIndex();
        this.valueTable = new ModelValue[QuadModelImpl.INITIAL_VALUE_TABLE_SIZE];
        this.valueCount = 0;
        this.valueSlots = 0;
        this.statementTable = new ModelStatement[QuadModelImpl.INITIAL_STATEMENT_TABLE_SIZE];
        this.statementCount = 0;
        this.statementSlots = 0;
        this.statementZombies = 0;
        this.valueNil = (ModelIRI) lookupValue(SESAME.NIL, true);
        this.valueLang = (ModelIRI) lookupValue(RDF.LANGSTRING, true);
    }

    // NAMESPACE HANDLING

    @Override
    protected Set<Namespace> doGetNamespaces() {
        return new HashSet<>(this.namespaces.values());
    }

    @Override
    protected Namespace doGetNamespace(final String prefix) {
        return this.namespaces.get(prefix);

    }

    @Override
    protected Namespace doSetNamespace(final String prefix, @Nullable final String name) {
        if (name == null) {
            return this.namespaces.remove(prefix);
        } else {
            return this.namespaces.put(prefix, new SimpleNamespace(prefix, name));
        }
    }

    // STATEMENT HANDLING - CONTEXT ARRAYS
    //
    // the following methods performs three things:
    // (1) they translate input values to model values, possibly creating them
    // (2) they translate doXXX calls supplying multiple contexts to corresponding doXXX calls
    // operating on a single context (possibly a wildcard)

    @Override
    protected int doSize(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {

        // Null context arrays are forbidden
        Objects.requireNonNull(ctxs);

        // Lookup SPO in the values hash table
        final ModelResource msubj = (ModelResource) lookupValue(subj, false);
        final ModelIRI mpred = (ModelIRI) lookupValue(pred, false);
        final ModelValue mobj = lookupValue(obj, false);

        // If one of SPO is missing in the table, then no statements can exist for that component
        if (msubj == QuadModelImpl.NULL_VALUE || mpred == QuadModelImpl.NULL_VALUE
                || mobj == QuadModelImpl.NULL_VALUE) {
            return 0;
        }

        // Otherwise, handle two cases based on the contents of the context array
        if (ctxs.length == 0) {
            // (1) Match any context
            return this.doSize(msubj, mpred, mobj, (ModelResource) null);

        } else {
            // (2) Match multiple contexts, summing the # of statements for each of them
            int size = 0;
            for (final Resource ctx : ctxs) {
                final ModelResource mctx = ctx == null ? this.valueNil
                        : (ModelResource) lookupValue(ctx, false);
                size += mctx == QuadModelImpl.NULL_VALUE ? 0
                        : this.doSize(msubj, mpred, mobj, mctx);
            }
            return size;
        }
    }

    @Override
    protected int doSizeEstimate(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, @Nullable final Resource ctx) {

        // Lookup SPO in the values hash table
        final ModelResource msubj = (ModelResource) lookupValue(subj, false);
        final ModelIRI mpred = (ModelIRI) lookupValue(pred, false);
        final ModelValue mobj = lookupValue(obj, false);
        final ModelResource mctx = (ModelResource) lookupValue(ctx, false);

        // If one of SPOC is missing in the table, then no statements can exist for that component
        if (msubj == QuadModelImpl.NULL_VALUE || mpred == QuadModelImpl.NULL_VALUE
                || mobj == QuadModelImpl.NULL_VALUE || mctx == QuadModelImpl.NULL_VALUE) {
            return 0;
        }

        // Otherwise, delegate
        return this.doSizeEstimate(msubj, mpred, mobj, mctx);
    }

    @Override
    protected Iterator<Statement> doIterator(@Nullable final Resource subj,
            @Nullable final IRI pred, @Nullable final Value obj, final Resource[] ctxs) {

        // Null context arrays are forbidden
        Objects.requireNonNull(ctxs);

        // Lookup SPO in the values hash table
        final ModelResource msubj = (ModelResource) lookupValue(subj, false);
        final ModelIRI mpred = (ModelIRI) lookupValue(pred, false);
        final ModelValue mobj = lookupValue(obj, false);

        // If any of SPO is missing in the table, then no statements can exist for that component
        if (msubj == QuadModelImpl.NULL_VALUE || mpred == QuadModelImpl.NULL_VALUE
                || mobj == QuadModelImpl.NULL_VALUE) {
            return Collections.emptyIterator();
        }

        // Otherwise handle three cases based on the contexts array
        if (ctxs.length == 0) {
            // (1) Match any context
            return this.doIterator(msubj, mpred, mobj, (ModelResource) null);

        } else if (ctxs.length == 1) {
            // (2) Match exactly one context. If not defined, return an empty iterator
            final ModelResource mctx = ctxs[0] == null ? this.valueNil
                    : (ModelResource) lookupValue(ctxs[0], false);
            return mctx == QuadModelImpl.NULL_VALUE ? Collections.emptyIterator() //
                    : this.doIterator(msubj, mpred, mobj, mctx);

        } else {
            // (3) Match multiple contexts, concatenating the iterators for each context
            final Iterator<Resource> ctxIterator = Arrays.asList(ctxs).iterator();
            return Iterators.concat(Iterators.transform(ctxIterator, (final Resource ctx) -> {
                final ModelResource mctx = ctx == null ? this.valueNil
                        : (ModelResource) lookupValue(ctx, false);
                return ctx == QuadModelImpl.NULL_VALUE ? Collections.emptyIterator() //
                        : this.doIterator(msubj, mpred, mobj, mctx);
            }));
        }
    }

    @Override
    protected boolean doAdd(final Resource subj, final IRI pred, final Value obj,
            final Resource[] ctxs) {

        // All SPOC components must be specified
        Objects.requireNonNull(subj);
        Objects.requireNonNull(pred);
        Objects.requireNonNull(obj);
        Objects.requireNonNull(ctxs);

        // Lookup SPO model values in the values hash table, creating them if necessary
        final ModelResource msubj = (ModelResource) lookupValue(subj, true);
        final ModelIRI mpred = (ModelIRI) lookupValue(pred, true);
        final ModelValue mobj = lookupValue(obj, true);

        // Handle two cases based on the context array
        if (ctxs.length == 0) {
            // (1) Add a single statement in the default context (sesame:nil)
            return this.doAdd(msubj, mpred, mobj, this.valueNil);

        } else {
            // (2) Add multiple statements in different contexts
            boolean modified = false;
            for (final Resource ctx : ctxs) {
                final ModelResource mctx = ctx == null ? this.valueNil
                        : (ModelResource) lookupValue(ctx, true);
                final boolean isNew = this.doAdd(msubj, mpred, mobj, mctx);
                modified |= isNew;
            }
            return modified;
        }
    }

    @Override
    protected boolean doRemove(@Nullable final Resource subj, @Nullable final IRI pred,
            @Nullable final Value obj, final Resource[] ctxs) {

        // Null context arrays are forbidden
        Objects.requireNonNull(ctxs);

        // Lookup SPO in the values hash table
        final ModelResource msubj = (ModelResource) lookupValue(subj, false);
        final ModelIRI mpred = (ModelIRI) lookupValue(pred, false);
        final ModelValue mobj = lookupValue(obj, false);

        // If any of SPO is missing in the table, then no statements can exist for that component
        if (msubj == QuadModelImpl.NULL_VALUE || mpred == QuadModelImpl.NULL_VALUE
                || mobj == QuadModelImpl.NULL_VALUE) {
            return false;
        }

        // Otherwise handle two cases based on the contents of the contexts array
        if (ctxs.length == 0) {
            // (1) Wildcard context: remove statements matching SPO in any context
            return this.doRemove(msubj, mpred, mobj, (ModelResource) null);

        } else {
            // (2) Specific contexts: remove statements matching SPO in the given contexts
            boolean modified = false;
            for (final Resource ctx : ctxs) {
                final ModelResource mctx = ctx == null ? this.valueNil
                        : (ModelResource) lookupValue(ctx, false);
                if (mctx != QuadModelImpl.NULL_VALUE) {
                    final boolean m = this.doRemove(msubj, mpred, mobj, mctx);
                    modified |= m;
                }
            }
            return modified;
        }
    }

    @Override
    protected synchronized Value doNormalize(final Value value) {
        return lookupValue(value, true);
    }

    // STATEMENT HANDLING - SINGLE CONTEXTS

    private int doSize(@Nullable final ModelResource subj, @Nullable final ModelIRI pred,
            @Nullable final ModelValue obj, @Nullable final ModelResource ctx) {

        // Select the SPOC component associated to the minimum number of statements
        final int comp = QuadModelImpl.selectComponent(subj, pred, obj, ctx);

        // If no component has been specified, return the total model size
        if (comp < 0) {
            return this.statementCount;
        }

        // Otherwise, iterate over the statements associated to the component, applying the filter
        // and returning the number of statements that matches it
        int size = 0;
        final ModelValue value = comp == 0 ? subj : comp == 1 ? pred : comp == 2 ? obj : ctx;
        for (ModelStatement stmt = value.next(comp); stmt != null; stmt = stmt.next(comp)) {
            if (stmt.match(subj, pred, obj, ctx)) {
                ++size;
            }
        }
        return size;
    }

    private int doSizeEstimate(@Nullable final ModelResource subj, @Nullable final ModelIRI pred,
            @Nullable final ModelValue obj, @Nullable final ModelResource ctx) {

        int size = this.statementCount;
        if (subj != null && subj.numSubj < size) {
            size = subj.numSubj;
        }
        if (pred != null && pred.numPred < size) {
            size = pred.numPred;
        }
        if (obj != null && obj.numObj < size) {
            size = obj.numObj;
        }
        if (ctx != null && ctx.numCtx < size) {
            size = ctx.numCtx;
        }
        return size;
    }

    private Iterator<Statement> doIterator(@Nullable final ModelResource subj,
            @Nullable final ModelIRI pred, @Nullable final ModelValue obj,
            @Nullable final ModelResource ctx) {

        // Select the SPOC component associated to the min number of statements
        final int comp = QuadModelImpl.selectComponent(subj, pred, obj, ctx);

        // If no component has been specified, return an iterator over all the statements
        // The returned iterator supports element removal (delegating to removeStatement)
        if (comp < 0) {
            return new Iterator<Statement>() {

                private int index = 0;

                private ModelStatement next = null;

                private ModelStatement last = null;

                @Override
                public boolean hasNext() {
                    if (this.next != null) {
                        return true;
                    }
                    while (this.index < QuadModelImpl.this.statementTable.length) {
                        final ModelStatement stmt = QuadModelImpl.this.statementTable[this.index++];
                        if (stmt != null && stmt != QuadModelImpl.NULL_STATEMENT) {
                            this.next = stmt;
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public Statement next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    this.last = this.next;
                    this.next = null;
                    return this.last;
                }

                @Override
                public void remove() {
                    if (this.last == null) {
                        throw new NoSuchElementException();
                    }
                    QuadModelImpl.this.removeStatement(this.last.subj, this.last.pred,
                            this.last.obj, this.last.ctx);
                    this.last = null;
                }

            };
        }

        // Otherwise, build an iterator over all the statements associated to the component, and
        // then filter it so to return only statements matching the supplied filter. The returned
        // iterator supports statement removal (by delegating to removeStatement)
        final ModelValue value = comp == 0 ? subj : comp == 1 ? pred : comp == 2 ? obj : ctx;
        ModelStatement stmt = value.next(comp);
        while (true) {
            if (stmt == null) {
                return Collections.emptyIterator();
            } else if (!stmt.isZombie() && stmt.match(subj, pred, obj, ctx)) {
                break;
            }
            stmt = stmt.next(comp);
        }
        final ModelStatement firstStmt = stmt;
        return new Iterator<Statement>() {

            private ModelStatement next = firstStmt;

            private ModelStatement last = null;

            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public ModelStatement next() {
                this.last = this.next;
                while (true) {
                    this.next = this.next.next(comp);
                    if (this.next == null
                            || !this.next.isZombie() && this.next.match(subj, pred, obj, ctx)) {
                        break;
                    }
                }
                return this.last;
            }

            @Override
            public void remove() {
                if (this.last == null) {
                    throw new NoSuchElementException();
                }
                QuadModelImpl.this.removeStatement(this.last.subj, this.last.pred, this.last.obj,
                        this.last.ctx);
                this.last = null;
            }

        };
    }

    private boolean doAdd(final ModelResource subj, final ModelIRI pred, final ModelValue obj,
            final ModelResource ctx) {

        // Identify the first slot where the statement could be stored in the hash table
        final int hash = ModelStatement.hash(subj, pred, obj, ctx);
        int slot = (hash & 0x7FFFFFFF) % this.statementTable.length;

        // Scan the hash table (linear probing), doing nothing if a matching statement is found or
        // adding the statement otherwise
        while (true) {

            // Retrieve the statement for the current slot (if any) and handle three cases
            ModelStatement stmt = this.statementTable[slot];
            final boolean isNull = stmt == null;
            if (isNull || stmt == QuadModelImpl.NULL_STATEMENT) {

                // (1) Empty/deleted slot: add the statement
                // First add the statement to the hash table and rehash if necessary
                stmt = new ModelStatement(subj, pred, obj, ctx);
                this.statementTable[slot] = stmt;
                ++this.statementCount;
                if (isNull) {
                    ++this.statementSlots;
                    if (this.statementSlots * 2 >= this.statementTable.length) {
                        rehashStatements();
                    }
                }

                // Then connect the statement to the various linked lists
                stmt.nextBySubj = subj.nextBySubj;
                stmt.nextByPred = pred.nextByPred;
                stmt.nextByObj = obj.nextByObj;
                stmt.nextByCtx = ctx.nextByCtx;
                subj.nextBySubj = stmt;
                pred.nextByPred = stmt;
                obj.nextByObj = stmt;
                ctx.nextByCtx = stmt;

                // Increment statement counters
                ++subj.numSubj;
                ++pred.numPred;
                ++obj.numObj;
                ++ctx.numCtx;

                // Signal a statement was added
                return true;

            } else if (subj == stmt.subj && pred == stmt.pred && obj == stmt.obj
                    && ctx == stmt.ctx) {

                // (2) Statement already in the model: abort signalling nothing happened
                return false;

            } else {

                // (3) Another statement in the slot: move to next slot
                slot = QuadModelImpl.incrementSlot(slot, this.statementTable.length);

            }
        }
    }

    private boolean doRemove(@Nullable final ModelResource subj, @Nullable final ModelIRI pred,
            @Nullable final ModelValue obj, @Nullable final ModelResource ctx) {

        // Do nothing if model is empty
        if (this.statementCount == 0) {
            return false;
        }

        // Remove exactly one statement (at most) if all the components were supplied
        if (subj != null && pred != null && obj != null && ctx != null) {
            return removeStatement(subj, pred, obj, ctx);
        }

        // Select the SPOC component associated to the min number of statements
        final int comp = QuadModelImpl.selectComponent(subj, pred, obj, ctx);

        // Clear the whole model (preserving generated values) if no component was specified
        if (comp < 0) {
            this.statementTable = new ModelStatement[QuadModelImpl.INITIAL_STATEMENT_TABLE_SIZE];
            this.statementCount = 0;
            this.statementSlots = 0;
            for (final ModelValue value : this.valueTable) {
                if (value != null) {
                    value.nextByObj = null;
                    value.numObj = 0;
                    if (value instanceof ModelResource) {
                        final ModelResource resource = (ModelResource) value;
                        resource.nextBySubj = null;
                        resource.numSubj = 0;
                        resource.nextByCtx = null;
                        resource.numCtx = 0;
                        if (value instanceof ModelIRI) {
                            final ModelIRI iri = (ModelIRI) value;
                            iri.nextByPred = null;
                            iri.numPred = 0;
                        }
                    }
                }
            }
            return true;
        }

        // Remove all the statements associated to the component that satisfy the filter
        boolean modified = false;
        final ModelValue value = comp == 0 ? subj : comp == 1 ? pred : comp == 2 ? obj : ctx;
        ModelStatement stmt = value.next(comp);
        while (stmt != null) {
            final ModelStatement next = stmt.next(comp);
            if (stmt.match(subj, pred, obj, ctx)) {
                final boolean m = removeStatement(stmt.subj, stmt.pred, stmt.obj, stmt.ctx);
                modified |= m;
            }
            stmt = next;
        }
        return modified;
    }

    // STATEMENT HANDLING - MISC METHODS

    private boolean removeStatement(final ModelResource subj, final ModelIRI pred,
            final ModelValue obj, final ModelResource ctx) {

        // Delete a matching statement from the hash table, aborting if it does not exist
        final int hash = ModelStatement.hash(subj, pred, obj, ctx);
        int slot = (hash & 0x7FFFFFFF) % this.statementTable.length;
        ModelStatement mstmt;
        while (true) {
            mstmt = this.statementTable[slot];
            if (mstmt == null) {
                return false;
            } else if (mstmt != QuadModelImpl.NULL_STATEMENT && subj == mstmt.subj
                    && pred == mstmt.pred && obj == mstmt.obj && ctx == mstmt.ctx) {
                this.statementTable[slot] = QuadModelImpl.NULL_STATEMENT;
                break;
            } else {
                slot = QuadModelImpl.incrementSlot(slot, this.statementTable.length);
            }
        }

        // Mark the statement as deleted
        mstmt.markZombie();

        // Update counters
        --subj.numSubj;
        --pred.numPred;
        --obj.numObj;
        --ctx.numCtx;
        --this.statementCount;
        ++this.statementZombies;

        // Remove zombie statements if too many
        if (this.statementZombies >= this.statementCount) {
            cleanZombies();
        }

        // Signal that a statement was removed
        return true;
    }

    private ModelValue lookupValue(@Nullable final Value value, final boolean canCreate) {

        // Handle null case
        if (value == null) {
            return null;
        }

        // Handle the case the value is already a model value belonging to this model
        if (value instanceof ModelValue) {
            final ModelValue mv = (ModelValue) value;
            if (mv.model == this) {
                return mv;
            }
        }

        // Lookup the model value in the hash table
        final int hash = value.hashCode();
        int slot = (hash & 0x7FFFFFFF) % this.valueTable.length;
        while (true) {
            ModelValue mv = this.valueTable[slot];
            final boolean isNull = mv == null;
            if (isNull || mv == QuadModelImpl.NULL_VALUE) {

                // Return null if missing and cannot create
                if (!canCreate) {
                    return QuadModelImpl.NULL_VALUE;
                }

                // Otherwise create the model value
                if (value instanceof IRI) {
                    mv = new ModelIRI(this, value.stringValue());
                } else if (value instanceof BNode) {
                    mv = new ModelBNode(this, ((BNode) value).getID());
                } else if (value instanceof Literal) {
                    final Literal lit = (Literal) value;
                    final String language = lit.getLanguage().orElse(null);
                    final IRI datatype = lit.getLanguage().isPresent() ? RDF.LANGSTRING //
                            : lit.getDatatype() != null ? lit.getDatatype() : XMLSchema.STRING;
                    mv = new ModelLiteral(this, lit.getLabel(),
                            language == null ? null : language.intern(),
                            (ModelIRI) lookupValue(datatype, true));
                } else {
                    throw new Error(value.getClass().getName());
                }

                // Add the model value to the hash table and rehash if necessary
                this.valueTable[slot] = mv;
                ++this.valueCount;
                if (isNull) {
                    ++this.valueSlots;
                    if (this.valueSlots * 2 >= this.valueTable.length) {
                        rehashValues();
                    }
                }

                // Return inserted model value
                return mv;

            } else if (mv.equals(value)) {

                // Value already in the table: return it
                return mv;

            } else {

                // Move to next slot
                slot = QuadModelImpl.incrementSlot(slot, this.valueTable.length);

            }
        }
    }

    private void rehashValues() {

        // Abort if there is no need for rehashing
        if (this.valueSlots < this.valueTable.length / 2) {
            return;
        }

        // Compute the new table size based on the number of stored values and NOT on the
        // number of filled slots (which may contain the NULL_VALUE marker)
        final int newLength = this.valueTable.length
                * (this.valueCount * 2 >= this.valueTable.length ? 2 : 1);

        // Replace the statement hash table with a possibly larger table
        final ModelValue[] oldTable = this.valueTable;
        this.valueTable = new ModelValue[newLength];
        for (final ModelValue mv : oldTable) {
            if (mv != null && mv != QuadModelImpl.NULL_VALUE) {
                final int hash = mv.hashCode();
                int slot = (hash & 0x7FFFFFFF) % this.valueTable.length;
                while (this.valueTable[slot] != null) {
                    slot = QuadModelImpl.incrementSlot(slot, this.valueTable.length);
                }
                this.valueTable[slot] = mv;
            }
        }

        // The number of used slots is now equal to the number of stored values
        this.valueSlots = this.valueCount;
    }

    private void rehashStatements() {

        // Abort if there is no need for re-hashing
        if (this.statementSlots * 2 < this.statementTable.length) {
            return;
        }

        // Compute the new table size based on the number of stored statements and NOT on the
        // number of filled slots (which may contain the NULL_STATEMENT marker)
        final int newLength = this.statementTable.length
                * (this.statementCount * 2 >= this.statementTable.length ? 2 : 1);

        // Replace the statement hash table with a possibly larger table
        final ModelStatement[] oldTable = this.statementTable;
        this.statementTable = new ModelStatement[newLength];
        for (final ModelStatement mstmt : oldTable) {
            if (mstmt != null && mstmt != QuadModelImpl.NULL_STATEMENT) {
                final int hash = mstmt.hash();
                int slot = (hash & 0x7FFFFFFF) % this.statementTable.length;
                while (this.statementTable[slot] != null) {
                    slot = QuadModelImpl.incrementSlot(slot, this.statementTable.length);
                }
                this.statementTable[slot] = mstmt;
            }
        }

        // The number of used slots is now equal to the number of stored statements
        this.statementSlots = this.statementCount;
    }

    private void cleanZombies() {

        // Iterate over the values in this model, removing zombie statements from SPOC lists
        ModelStatement stmt;
        ModelStatement prev;
        for (final ModelValue mv : this.valueTable) {

            // Skip empty slots
            if (mv == null || mv == QuadModelImpl.NULL_VALUE) {
                continue;
            }

            // Remove zombie statements from object list
            for (prev = null, stmt = mv.nextByObj; stmt != null; stmt = stmt.nextByObj) {
                if (!stmt.isZombie()) {
                    prev = stmt;
                } else if (prev == null) {
                    mv.nextByObj = stmt.nextByObj;
                } else {
                    prev.nextByObj = stmt.nextByObj;
                }
            }

            // Proceed only if the value is a Resource with subject and context lists
            if (!(mv instanceof ModelResource)) {
                continue;
            }
            final ModelResource mr = (ModelResource) mv;

            // Remove zombie statements from subject list
            for (prev = null, stmt = mr.nextBySubj; stmt != null; stmt = stmt.nextBySubj) {
                if (!stmt.isZombie()) {
                    prev = stmt;
                } else if (prev == null) {
                    mr.nextBySubj = stmt.nextBySubj;
                } else {
                    prev.nextBySubj = stmt.nextBySubj;
                }
            }

            // Remove zombie statements from context list
            for (prev = null, stmt = mr.nextByCtx; stmt != null; stmt = stmt.nextByCtx) {
                if (!stmt.isZombie()) {
                    prev = stmt;
                } else if (prev == null) {
                    mr.nextByCtx = stmt.nextByCtx;
                } else {
                    prev.nextByCtx = stmt.nextByCtx;
                }
            }

            // Proceed only if the value is a IRI with predicate list
            if (!(mv instanceof ModelIRI)) {
                continue;
            }
            final ModelIRI mu = (ModelIRI) mv;

            // Remove zombie statements from predicate list
            for (prev = null, stmt = mu.nextByPred; stmt != null; stmt = stmt.nextByPred) {
                if (!stmt.isZombie()) {
                    prev = stmt;
                } else if (prev == null) {
                    mu.nextByPred = stmt.nextByPred;
                } else {
                    prev.nextByPred = stmt.nextByPred;
                }
            }
        }

        // Reset zombie counter
        this.statementZombies = 0;
    }

    private static int selectComponent(@Nullable final ModelResource subj,
            @Nullable final ModelIRI pred, @Nullable final ModelValue obj,
            @Nullable final ModelResource ctx) {

        // Start with no component selected
        int result = -1;
        int num = Integer.MAX_VALUE;

        // Then, choose the component with the minimum number of associated statements
        if (subj != null && subj.numSubj < num) {
            result = QuadModelImpl.SUBJ;
            num = subj.numSubj;
        }
        if (pred != null && pred.numPred < num) {
            result = QuadModelImpl.PRED;
            num = pred.numPred;
        }
        if (obj != null && obj.numObj < num) {
            result = QuadModelImpl.OBJ;
            num = obj.numObj;
        }
        if (ctx != null && ctx.numCtx < num) {
            result = QuadModelImpl.CTX;
            num = ctx.numCtx;
        }

        // Return either a component ID or a negative value if no component was specified
        return result;
    }

    private static int incrementSlot(final int num, final int max) {
        final int result = num + 1;
        return result >= max ? 0 : result;
    }

    private static abstract class ModelValue implements Value, Hashable {

        private static final long serialVersionUID = 1L;

        @Nullable
        final transient QuadModelImpl model;

        @Nullable
        transient ModelStatement nextByObj;

        transient int numObj;

        transient long hashLo;

        transient long hashHi;

        ModelValue(final QuadModelImpl model) {
            this.model = model;
        }

        ModelStatement next(final int component) {
            switch (component) {
            case OBJ:
                return this.nextByObj;
            default:
                return null;
            }
        }

        @Override
        public Hash getHash() {
            Hash hash;
            if (this.hashLo != 0L) {
                hash = Hash.fromLongs(this.hashHi, this.hashLo);
            } else {
                hash = Statements.computeHash(this);
                this.hashHi = hash.getHigh();
                this.hashLo = hash.getLow();
            }
            return hash;
        }

    }

    private static abstract class ModelResource extends ModelValue implements Resource {

        private static final long serialVersionUID = 1L;

        @Nullable
        transient ModelStatement nextBySubj;

        @Nullable
        transient ModelStatement nextByCtx;

        transient int numSubj;

        transient int numCtx;

        ModelResource(final QuadModelImpl model) {
            super(model);
        }

        @Override
        ModelStatement next(final int component) {
            switch (component) {
            case SUBJ:
                return this.nextBySubj;
            case OBJ:
                return this.nextByObj;
            case CTX:
                return this.nextByCtx;
            default:
                return null;
            }
        }

    }

    private static final class ModelIRI extends ModelResource implements IRI {

        private static final long serialVersionUID = 1L;

        private transient final int namespace;

        private transient final int localName;

        private final int hash;

        private Object cachedString;

        @Nullable
        transient ModelStatement nextByPred;

        transient int numPred;

        ModelIRI(@Nullable final QuadModelImpl model, final String string) {
            super(model);
            final int index = URIUtil.getLocalNameIndex(string);
            if (model != null) {
                this.namespace = model.stringIndex.put(string.substring(0, index));
                this.localName = model.stringIndex.put(string.substring(index));
                this.cachedString = null;
            } else {
                this.namespace = 0;
                this.localName = 0;
                this.cachedString = string;
            }
            this.hash = string.hashCode();
        }

        @Override
        ModelStatement next(final int component) {
            switch (component) {
            case SUBJ:
                return this.nextBySubj;
            case PRED:
                return this.nextByPred;
            case OBJ:
                return this.nextByObj;
            case CTX:
                return this.nextByCtx;
            default:
                throw new Error();
            }
        }

        @Nullable
        private String getCachedString(final boolean compute) {
            if (this.cachedString instanceof Reference<?>) {
                @SuppressWarnings("unchecked")
                final String s = ((Reference<String>) this.cachedString).get();
                if (s == null) {
                    this.cachedString = null;
                } else {
                    return s;
                }
            } else if (this.cachedString instanceof String) {
                return (String) this.cachedString;
            }
            if (compute) {
                final StringBuilder builder = new StringBuilder();
                this.model.stringIndex.get(this.namespace, builder);
                this.model.stringIndex.get(this.localName, builder);
                final String s = builder.toString();
                this.cachedString = new SoftReference<String>(s);
                return s;
            }
            return null;
        }

        private void writeObject(final ObjectOutputStream out) throws IOException {
            final String string = getCachedString(true);
            final Object oldCachedString = this.cachedString;
            this.cachedString = string;
            out.defaultWriteObject();
            this.cachedString = oldCachedString;
        }

        @Override
        public String getNamespace() {
            if (this.model != null) {
                return this.model.stringIndex.get(this.namespace);
            } else {
                final String s = (String) this.cachedString;
                final int index = URIUtil.getLocalNameIndex(s);
                return s.substring(0, index);
            }
        }

        @Override
        public String getLocalName() {
            if (this.model != null) {
                return this.model.stringIndex.get(this.localName);
            } else {
                final String s = (String) this.cachedString;
                final int index = URIUtil.getLocalNameIndex(s);
                return s.substring(index);
            }
        }

        @Override
        public String stringValue() {
            return getCachedString(true);
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (this.model != null && object instanceof ModelIRI
                    && this.model == ((ModelIRI) object).model) {
                return false;
            }
            if (object instanceof IRI) {
                final String string = ((IRI) object).stringValue();
                final String s = getCachedString(false);
                if (s != null) {
                    return s.equals(string);
                } else {
                    final StringIndex index = this.model.stringIndex;
                    final int namespaceLength = index.length(this.namespace);
                    final int localNameLength = index.length(this.localName);
                    return namespaceLength + localNameLength == string.length()
                            && index.equals(this.namespace, string, 0, namespaceLength)
                            && index.equals(this.localName, string, namespaceLength,
                                    string.length());
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public String toString() {
            return stringValue();
        }

    }

    private static final class ModelBNode extends ModelResource implements BNode {

        private static final long serialVersionUID = 1L;

        private final int id;

        private final int hash;

        ModelBNode(final QuadModelImpl model, final String id) {
            super(model);
            this.id = model.stringIndex.put(id);
            this.hash = id.hashCode();
        }

        @Override
        public String getID() {
            return this.model.stringIndex.get(this.id);
        }

        @Override
        public String stringValue() {
            return getID();
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (this.model != null && object instanceof ModelBNode
                    && this.model == ((ModelBNode) object).model) {
                return false;
            }
            if (object instanceof BNode) {
                return this.model.stringIndex.equals(this.id, ((BNode) object).getID());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder("_:");
            this.model.stringIndex.get(this.id, builder);
            return builder.toString();
        }

    }

    private static final class ModelLiteral extends ModelValue implements Literal {

        private static final long serialVersionUID = 1L;

        private final int label;

        private final Object langOrDatatype;

        private final int hash;

        @Nullable
        private Object cachedLabel;

        ModelLiteral(@Nullable final QuadModelImpl model, final String label,
                @Nullable final String language, final ModelIRI datatype) {

            super(model);

            final int hashCode = label.hashCode();
            // if (language != null) {
            // hashCode = 31 * hashCode + language.hashCode();
            // }
            // hashCode = 31 * hashCode + datatype.hashCode();

            this.langOrDatatype = language == null ? datatype : language.intern();
            this.hash = hashCode;

            if (model != null) {
                this.label = model.stringIndex.put(label);
                this.cachedLabel = null;
            } else {
                this.label = 0;
                this.cachedLabel = label;
            }
        }

        @Nullable
        private String getCachedLabel(final boolean compute) {
            if (this.cachedLabel instanceof Reference<?>) {
                @SuppressWarnings("unchecked")
                final String s = ((Reference<String>) this.cachedLabel).get();
                if (s == null) {
                    this.cachedLabel = null;
                } else {
                    return s;
                }
            } else if (this.cachedLabel instanceof String) {
                return (String) this.cachedLabel;
            }
            if (compute) {
                final String s = this.model.stringIndex.get(this.label);
                this.cachedLabel = new SoftReference<String>(s);
                return s;
            }
            return null;
        }

        private void writeObject(final ObjectOutputStream out) throws IOException {
            final String label = getCachedLabel(true);
            final Object oldCachedLabel = this.cachedLabel;
            this.cachedLabel = label;
            out.defaultWriteObject();
            this.cachedLabel = oldCachedLabel;
        }

        @Override
        public String getLabel() {
            return getCachedLabel(true);
        }

        @Override
        public Optional<String> getLanguage() {
            return this.langOrDatatype instanceof String
                    ? Optional.of((String) this.langOrDatatype) : Optional.empty();
        }

        @Override
        public IRI getDatatype() {
            return this.langOrDatatype instanceof String
                    ? this.model != null ? this.model.valueLang : RDF.LANGSTRING
                    : (IRI) this.langOrDatatype;
        }

        @Override
        public String stringValue() {
            return getLabel();
        }

        @Override
        public boolean booleanValue() {
            return XMLDatatypeUtil.parseBoolean(getLabel());
        }

        @Override
        public byte byteValue() {
            return XMLDatatypeUtil.parseByte(getLabel());
        }

        @Override
        public short shortValue() {
            return XMLDatatypeUtil.parseShort(getLabel());
        }

        @Override
        public int intValue() {
            return XMLDatatypeUtil.parseInt(getLabel());
        }

        @Override
        public long longValue() {
            return XMLDatatypeUtil.parseLong(getLabel());
        }

        @Override
        public float floatValue() {
            return XMLDatatypeUtil.parseFloat(getLabel());
        }

        @Override
        public double doubleValue() {
            return XMLDatatypeUtil.parseDouble(getLabel());
        }

        @Override
        public BigInteger integerValue() {
            return XMLDatatypeUtil.parseInteger(getLabel());
        }

        @Override
        public BigDecimal decimalValue() {
            return XMLDatatypeUtil.parseDecimal(getLabel());
        }

        @Override
        public XMLGregorianCalendar calendarValue() {
            return XMLDatatypeUtil.parseCalendar(getLabel());
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (this.model != null && object instanceof ModelLiteral
                    && this.model == ((ModelLiteral) object).model) {
                return false;
            }
            if (object instanceof Literal) {
                final Literal l = (Literal) object;
                if (this.langOrDatatype instanceof String
                        && this.langOrDatatype.equals(l.getLanguage().orElse(null)) //
                        || this.langOrDatatype instanceof IRI
                                && this.langOrDatatype.equals(l.getDatatype())) {
                    final String s = getCachedLabel(false);
                    return s != null ? s.equals(l.getLabel())
                            : this.model.stringIndex.equals(this.label, l.getLabel());
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.hash;
        }

        @Override
        public String toString() {
            final String s = getCachedLabel(false);
            final StringBuilder builder = new StringBuilder(256);
            builder.append('"');
            if (s != null) {
                builder.append(s);
            } else {
                this.model.stringIndex.get(this.label, builder);
            }
            builder.append('"');
            if (this.langOrDatatype instanceof String) {
                builder.append('@').append(this.langOrDatatype);
            } else {
                builder.append("^^<").append(this.langOrDatatype).append(">");
            }
            return builder.toString();
        }

    }

    private static final class ModelStatement implements Statement {

        private static final long serialVersionUID = 1L;

        private static final int HASH_ZOMBIE = 0;

        private static final int HASH_UNCACHED = 1;

        int hash;

        final ModelResource subj;

        final ModelIRI pred;

        final ModelValue obj;

        final ModelResource ctx;

        @Nullable
        transient ModelStatement nextBySubj;

        @Nullable
        transient ModelStatement nextByPred;

        @Nullable
        transient ModelStatement nextByObj;

        @Nullable
        transient ModelStatement nextByCtx;

        ModelStatement(final ModelResource subj, final ModelIRI pred, final ModelValue obj,
                final ModelResource ctx) {

            // final int cachedHash = 961 * subj.hashCode() + 31 * pred.hashCode() +
            // obj.hashCode();
            final int cachedHash = Objects.hash(subj, pred, obj, ctx);

            this.hash = cachedHash != ModelStatement.HASH_ZOMBIE ? cachedHash
                    : ModelStatement.HASH_UNCACHED;
            this.subj = subj;
            this.pred = pred;
            this.obj = obj;
            this.ctx = ctx;
        }

        @Nullable
        ModelStatement next(final int component) {

            switch (component) {
            case SUBJ:
                return this.nextBySubj;
            case PRED:
                return this.nextByPred;
            case OBJ:
                return this.nextByObj;
            case CTX:
                return this.nextByCtx;
            default:
                throw new Error();
            }
        }

        boolean match(@Nullable final ModelResource subj, @Nullable final ModelIRI pred,
                @Nullable final ModelValue obj, @Nullable final ModelResource ctx) {
            return (subj == null || subj == this.subj) && (pred == null || pred == this.pred)
                    && (obj == null || obj == this.obj) && (ctx == null || ctx == this.ctx);
        }

        int hash() {
            return ModelStatement.hash(this.subj, this.pred, this.obj, this.ctx);
        }

        static int hash(final ModelResource subj, final ModelIRI pred, final ModelValue obj,
                final ModelResource ctx) {
            return 6661 * System.identityHashCode(subj) + 961 * System.identityHashCode(pred)
                    + 31 * System.identityHashCode(obj) + System.identityHashCode(ctx);
        }

        boolean isZombie() {
            return this.hash == ModelStatement.HASH_ZOMBIE;
        }

        void markZombie() {
            this.hash = ModelStatement.HASH_ZOMBIE;
        }

        @Override
        public Resource getSubject() {
            return this.subj;
        }

        @Override
        public IRI getPredicate() {
            return this.pred;
        }

        @Override
        public Value getObject() {
            return this.obj;
        }

        @Override
        @Nullable
        public Resource getContext() {
            if (this.ctx != null) {
                if (this.ctx.model != null) {
                    if (this.ctx == this.ctx.model.valueNil) {
                        return null;
                    }
                } else {
                    if (this.ctx.equals(SESAME.NIL)) {
                        return null;
                    }
                }
            }
            return this.ctx;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof Statement)) {
                return false;
            }
            final Statement other = (Statement) object;
            return this.obj.equals(other.getObject()) && this.subj.equals(other.getSubject())
                    && this.pred.equals(other.getPredicate());
        }

        @Override
        public int hashCode() {
            if (this.hash != ModelStatement.HASH_ZOMBIE
                    && this.hash != ModelStatement.HASH_UNCACHED) {
                return this.hash;
            } else {
                return Objects.hash(this.subj, this.pred, this.obj, this.ctx);
                // return 961 * this.subj.hashCode() + 31 * this.pred.hashCode()
                // + this.obj.hashCode();
            }
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder(256);
            builder.append("(").append(this.subj).append(", ").append(this.pred).append(", ")
                    .append(this.obj).append(")");
            if (!SESAME.NIL.equals(this.ctx)) {
                builder.append(" [").append(this.ctx).append("]");
            }
            return builder.toString();
        }

    }

}
