package eu.fbk.rdfpro.rules.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Table;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.And;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.ListBindingSet;

public final class StatementMatcher {

    private static final Object NORMALIZED_MARKER = new Object();

    private static final Object UNNORMALIZED_MARKER = new Object();

    private static final List<String> VAR_NAMES = ImmutableList.of("s", "p", "o", "c");

    @Nullable
    private final Function<Value, Value> normalizer;

    private final byte[] masks;

    private final int[][] tables;

    private final Object[] values;

    private final Object[] normalizedValues; // modified during use

    private final URI nil;

    private final int numPatterns;

    private final int numValues;

    private final boolean matchAll;

    private StatementMatcher(@Nullable final Function<Value, Value> normalizer,
            final byte[] masks, final int[][] tables, @Nullable final Object[] values,
            final int numPatterns, final int numValues, final boolean matchAll) {

        // Initialize object state
        this.normalizer = normalizer;
        this.masks = masks;
        this.tables = tables;
        this.values = values;
        this.normalizedValues = normalizer == null ? values : values.clone();
        this.nil = normalizer == null ? SESAME.NIL : (URI) normalizer.apply(SESAME.NIL);
        this.numPatterns = numPatterns;
        this.numValues = numValues;
        this.matchAll = matchAll;
    }

    public StatementMatcher normalize(@Nullable final Function<Value, Value> normalizer) {
        return normalizer == null ? this : new StatementMatcher(normalizer, this.masks,
                this.tables, this.values, this.numPatterns, this.numValues, this.matchAll);
    }

    public boolean matchAll() {
        return this.matchAll;
    }

    public boolean match(final Statement stmt) {
        return match(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
    }

    public boolean match(final Resource subj, final URI pred, final Value obj,
            @Nullable Resource ctx) {

        if (this.matchAll) {
            return true;
        }

        if (ctx == null) {
            ctx = this.nil;
        }

        List<Filter> filters = null;

        for (int i = 0; i < this.masks.length; ++i) {
            final byte mask = this.masks[i];
            final int[] table = this.tables[i];
            final int hash = hash(subj, pred, obj, ctx, mask);
            for (int slot = (hash & 0x7FFFFFFF) % table.length; table[slot] != 0; slot = next(
                    slot, table.length)) {
                final int token = table[slot];
                int offset = match(subj, pred, obj, ctx, mask, hash, token);
                if (offset != 0) {
                    if (tokenToUnfiltered(token)) {
                        return true;
                    }
                    while (true) {
                        final Object value = this.normalizedValues[offset++];
                        if (value == NORMALIZED_MARKER || value == UNNORMALIZED_MARKER) {
                            break;
                        } else if (value instanceof Filter) {
                            if (filters == null) {
                                filters = new ArrayList<>();
                            }
                            filters.add((Filter) value);
                        }
                    }
                    break;
                }
            }
        }

        if (filters != null) {
            for (final Filter filter : filters) {
                if (filter.eval(subj, pred, obj, ctx)) {
                    return true;
                }
            }
        }

        return false;
    }

    public <T> List<T> map(final Resource subj, final URI pred, final Value obj,
            @Nullable final Resource ctx, final Class<T> clazz) {
        return map(subj, pred, obj, ctx, clazz, null);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> map(final Resource subj, final URI pred, final Value obj,
            @Nullable Resource ctx, final Class<T> clazz, @Nullable final List<T> list) {

        if (ctx == null) {
            ctx = this.nil;
        }

        List<T> result = list;

        outer: for (int i = 0; i < this.masks.length; ++i) {

            final byte mask = this.masks[i];

            int offset = 0;
            if (mask == 0) {
                normalizeIfNecessary(1);
                offset = 1;
            } else {
                final int[] table = this.tables[i];
                final int hash = hash(subj, pred, obj, ctx, mask);
                for (int slot = (hash & 0x7FFFFFFF) % table.length;; slot = next(slot,
                        table.length)) {
                    final int token = table[slot];
                    if (token == 0) {
                        continue outer;
                    }
                    offset = match(subj, pred, obj, ctx, mask, hash, token);
                    if (offset != 0) {
                        break;
                    }
                }
            }

            boolean add = true;
            while (true) {
                final Object value = this.normalizedValues[offset++];
                if (value == NORMALIZED_MARKER || value == UNNORMALIZED_MARKER) {
                    break;
                } else if (value instanceof Filter) {
                    add = ((Filter) value).eval(subj, pred, obj, ctx);
                } else if (add && clazz.isInstance(value)) {
                    result = result != null ? result : new ArrayList<>(16);
                    result.add((T) value);
                }
            }
        }

        return result != null ? result : Collections.emptyList();
    }

    @Override
    public String toString() {
        return this.numPatterns + " patterns, " + this.numValues + " values";
    }

    private int match(final Resource subj, final URI pred, final Value obj, final Resource ctx,
            final byte mask, final int hash, final int token) {

        // Check that lower 12 bits of the hash match with lower 12 bits of cell
        if (!tokenMatchHash(hash, token)) {
            return 0;
        }

        // Use the higher 20 bits of the cell as an index in the value array
        final int offset = tokenToOffset(token);

        // Normalize if necessary (the lack of synchronization is deliberate)
        normalizeIfNecessary(offset);

        // Check that the quad matches the constants in the value array
        int index = offset;
        if ((mask & 0x01) != 0 && !this.normalizedValues[index++].equals(subj)) {
            return 0;
        }
        if ((mask & 0x02) != 0 && !this.normalizedValues[index++].equals(pred)) {
            return 0;
        }
        if ((mask & 0x04) != 0 && !this.normalizedValues[index++].equals(obj)) {
            return 0;
        }
        if ((mask & 0x08) != 0 && !this.normalizedValues[index].equals(ctx)) {
            return 0;
        }
        return index;
    }

    private void normalizeIfNecessary(final int offset) {
        if (this.normalizer != null && this.normalizedValues[offset - 1] == UNNORMALIZED_MARKER) {
            for (int i = offset;; ++i) {
                Object value = this.normalizedValues[i];
                if (value == UNNORMALIZED_MARKER || value == NORMALIZED_MARKER) {
                    break;
                } else if (value instanceof Filter) {
                    value = ((Filter) value).normalize(this.normalizer);
                } else if (value instanceof Value) {
                    value = this.normalizer.apply((Value) value);
                } else if (value instanceof StatementMatcher) {
                    value = ((StatementMatcher) value).normalize(this.normalizer);
                } else if (value instanceof StatementTemplate) {
                    value = ((StatementTemplate) value).normalize(this.normalizer);
                }
                this.normalizedValues[i] = value;
            }
            this.normalizedValues[offset - 1] = NORMALIZED_MARKER;
        }
    }

    private static int next(final int slot, final int numSlots) {
        final int result = slot + 1;
        return result < numSlots ? result : 0;
    }

    private static int hash(final Resource subj, final URI pred, final Value obj,
            final Resource ctx, final byte mask) {

        int hash = 1;
        if ((mask & 0x01) != 0) {
            hash += 6661 * subj.hashCode();
        }
        if ((mask & 0x02) != 0) {
            hash += 961 * pred.hashCode();
        }
        if ((mask & 0x04) != 0) {
            hash += 31 * obj.hashCode();
        }
        if ((mask & 0x08) != 0) {
            hash += ctx.hashCode();
        }
        return hash;
    }

    private static byte mask(@Nullable final Resource subj, @Nullable final URI pred,
            @Nullable final Value obj, @Nullable final Resource ctx) {

        byte mask = 0;
        if (subj != null) {
            mask |= 0x01;
        }
        if (pred != null) {
            mask |= 0x02;
        }
        if (obj != null) {
            mask |= 0x04;
        }
        if (ctx != null) {
            mask |= 0x08;
        }
        return mask;
    }

    @Nullable
    private static Value variableValue(final Var var) {
        return var == null ? null : var.getValue();
    }

    private static void variableReplacement(final Var from, final String to,
            final Map<String, Var> map) {
        if (from != null && from.getValue() == null) {
            map.put(from.getName(), new Var(to));
        }
    }

    private static int tokenEncode(final int hash, final int offset, final boolean unfiltered) {
        assert offset < 0x1000000;
        return hash & 0x7FF00000 | offset & 0xFFFFF | (unfiltered ? 0x80000000 : 0);
    }

    private static boolean tokenToUnfiltered(final int token) {
        return (token & 0x80000000) != 0;
    }

    private static int tokenToOffset(final int token) {
        return token & 0xFFFFF;
    }

    private static boolean tokenMatchHash(final int hash, final int token) {
        return ((hash ^ token) & 0x7FF00000) == 0;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private static final Object EMPTY_FILTER = new Object();

        private final Table<List<Value>, Object, Set<Object>>[] maskData;

        private int numValues;

        private int numMasks;

        private int numFilters;

        @SuppressWarnings("unchecked")
        Builder() {
            // There are at most 16 masks to consider
            this.maskData = new Table[16];
            this.numValues = 0;
            this.numMasks = 0;
        }

        public Builder addExpr(final TupleExpr expr, final Object... mappedValues) {

            // Identify the statement pattern in the expression
            final List<StatementPattern> patterns = Algebra.extractNodes(expr,
                    StatementPattern.class, null, null);
            Preconditions.checkArgument(patterns.size() == 1);
            final StatementPattern pattern = patterns.get(0);

            // Identify the filter conditions in the expression
            ValueExpr filter = null;
            for (QueryModelNode node = pattern.getParentNode(); node != null; node = node
                    .getParentNode()) {
                if (node instanceof org.openrdf.query.algebra.Filter) {
                    final ValueExpr f = ((org.openrdf.query.algebra.Filter) node).getCondition();
                    if (filter == null) {
                        filter = f;
                    } else {
                        filter = new And(filter, f);
                    }
                } else {
                    throw new IllegalArgumentException("Invalid expression: " + expr);
                }
            }

            // Delegate
            return addPattern(pattern, filter, mappedValues);
        }

        public Builder addPattern(final StatementPattern pattern, @Nullable ValueExpr filter,
                final Object... mappedValues) {

            // Extract components
            final Resource subj = (Resource) variableValue(pattern.getSubjectVar());
            final URI pred = (URI) variableValue(pattern.getPredicateVar());
            final Value obj = variableValue(pattern.getObjectVar());
            final Resource ctx = (Resource) variableValue(pattern.getContextVar());

            // Rewrite filter if necessary
            if (filter != null) {
                final Map<String, Var> replacements = new HashMap<>();
                variableReplacement(pattern.getSubjectVar(), "s", replacements);
                variableReplacement(pattern.getPredicateVar(), "p", replacements);
                variableReplacement(pattern.getObjectVar(), "o", replacements);
                variableReplacement(pattern.getContextVar(), "c", replacements);
                filter = Algebra.rewrite(filter, replacements);
            }

            // Delegate
            return addValues(subj, pred, obj, ctx, filter, mappedValues);
        }

        public Builder addValues(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, @Nullable final Resource ctx,
                @Nullable final ValueExpr filter, final Object... mappedValues) {

            // Map the filter to a non-null key
            final Object filterKey = filter == null ? EMPTY_FILTER : filter;

            // Compute mask and pattern list
            final byte mask = mask(subj, pred, obj, ctx);
            final List<Value> pattern = Arrays.asList(subj, pred, obj, ctx);

            // Retrieve the table for the mask. Create it if necessary
            Table<List<Value>, Object, Set<Object>> sets = this.maskData[mask];
            if (sets == null) {
                sets = HashBasedTable.create();
                this.maskData[mask] = sets;
                this.numMasks++;
            }

            // Retrieve the values set for the (pattern, filter) pair. Create it if necessary
            Set<Object> set = sets.get(pattern, filterKey);
            if (set == null) {
                set = new HashSet<>();
                sets.put(pattern, filterKey, set);
                if (filterKey != EMPTY_FILTER) {
                    this.numFilters++;
                }
            }

            // Add the mapped values to the set
            this.numValues -= set.size();
            set.addAll(Arrays.asList(mappedValues));
            this.numValues += set.size();

            // Return this builder object for call chaining
            return this;
        }

        public StatementMatcher build(@Nullable final Function<Value, Value> normalizer) {

            // Compute the total size of the values array
            int valuesSize = this.numFilters + this.numValues + 1;
            for (int mask = 0; mask < this.maskData.length; ++mask) {
                final Table<List<Value>, Object, Set<Object>> table = this.maskData[mask];
                if (table != null) {
                    valuesSize += table.rowKeySet().size() * (Integer.bitCount(mask) + 1);
                }
            }

            // Allocate data structures
            final byte[] masks = new byte[this.numMasks];
            final int[][] tables = new int[this.numMasks][];
            final Object[] values = new Object[valuesSize * 4];

            // Initialize counters
            int numPatterns = 0;

            // Initialize mask and value indexes
            int maskIndex = 0;
            int valueIndex = 0;

            // Emit an initial marker
            values[valueIndex++] = UNNORMALIZED_MARKER;

            // Consider the patterns for each mask, starting from least selective mask
            boolean matchAll = false;
            for (final byte mask : new byte[] { 0, 8, 2, 4, 1, 10, 12, 9, 6, 3, 5, 14, 11, 13, 7,
                    15 }) {

                // Retrieve the table for the current mask. Abort if undefined
                final Table<List<Value>, Object, Set<Object>> sets = this.maskData[mask];
                if (sets == null) {
                    continue;
                }

                // Allocate the hash table for the mask (load factor 0.66)
                final int maskPatterns = sets.rowKeySet().size();
                final int[] table = new int[maskPatterns + Math.max(1, maskPatterns >> 1)];

                // Populate the hash table and the values array
                numPatterns += maskPatterns;
                for (final List<Value> pattern : sets.rowKeySet()) {

                    // Compute hash
                    final int hash = hash((Resource) pattern.get(0), (URI) pattern.get(1),
                            pattern.get(2), (Resource) pattern.get(3), mask);

                    // Identify whether the pattern is used unfiltered
                    final Map<Object, Set<Object>> map = sets.rowMap().get(pattern);
                    final boolean unfiltered = map.containsKey(EMPTY_FILTER);
                    if (unfiltered && mask == 0) {
                        matchAll = true;
                    }

                    // Update hash map
                    int slot = (hash & 0x7FFFFFFF) % table.length;
                    while (table[slot] != 0) {
                        slot = next(slot, table.length);
                    }
                    table[slot] = tokenEncode(hash, valueIndex, unfiltered);

                    // Append the constants used in the pattern
                    for (final Value component : pattern) {
                        if (component != null) {
                            values[valueIndex++] = component;
                        }
                    }

                    // Append filters and mapped values
                    for (final Map.Entry<Object, Set<Object>> entry : map.entrySet()) {
                        if (entry.getKey() != EMPTY_FILTER) {
                            values[valueIndex++] = Filter.create((ValueExpr) entry.getKey());
                        }
                        for (final Object value : entry.getValue()) {
                            values[valueIndex++] = value;
                        }
                    }

                    // Append marker
                    values[valueIndex++] = UNNORMALIZED_MARKER;
                }

                // Update masks and tables structures
                masks[maskIndex] = mask;
                tables[maskIndex] = table;
                ++maskIndex;
            }

            // Build a pattern matcher using the created data structures
            return new StatementMatcher(normalizer, masks, tables, values, numPatterns,
                    this.numValues, matchAll);
        }

    }

    private static abstract class Filter {

        static Filter create(final ValueExpr expr) {
            if (expr instanceof And) {
                final And and = (And) expr;
                return new AndFilter(create(and.getLeftArg()), create(and.getRightArg()));
            } else if (expr instanceof Compare) {
                final Compare cmp = (Compare) expr;
                if (cmp.getOperator() == CompareOp.EQ || cmp.getOperator() == CompareOp.NE) {
                    ValueExpr left = cmp.getLeftArg();
                    ValueExpr right = cmp.getRightArg();
                    if (left instanceof ValueConstant) {
                        left = new Var("l", ((ValueConstant) left).getValue());
                    }
                    if (right instanceof ValueConstant) {
                        right = new Var("r", ((ValueConstant) right).getValue());
                    }
                    if (left instanceof Var && right instanceof Var) {
                        return new CompareFilter((Var) left, (Var) right,
                                cmp.getOperator() == CompareOp.EQ);
                    }
                }
            }
            return new ValueExprFilter(expr);
        }

        Filter normalize(final Function<Value, Value> normalizer) {
            return this;
        }

        abstract boolean eval(final Resource subj, final URI pred, final Value obj,
                final Resource ctx);

        private static final class ValueExprFilter extends Filter {

            private final ValueExpr expr;

            ValueExprFilter(final ValueExpr expr) {
                this.expr = expr;
            }

            @Override
            Filter normalize(final Function<Value, Value> normalizer) {
                return new ValueExprFilter(Algebra.normalize(this.expr, normalizer));
            }

            @Override
            boolean eval(final Resource subj, final URI pred, final Value obj, final Resource ctx) {
                final BindingSet bindings = new ListBindingSet(VAR_NAMES, subj, pred, obj, ctx);
                return ((Literal) Algebra.evaluateValueExpr(this.expr, bindings)).booleanValue();
            }

        }

        private static final class CompareFilter extends Filter {

            private final Value leftValue;

            private final Value rightValue;

            private final char left;

            private final char right;

            private final boolean negate;

            CompareFilter(final Var left, final Var right, final boolean equal) {
                this.leftValue = left.getValue();
                this.rightValue = right.getValue();
                this.left = left.hasValue() ? 'l' //
                        : Character.toLowerCase(left.getName().charAt(0));
                this.right = right.hasValue() ? 'r' //
                        : Character.toLowerCase(right.getName().charAt(0));
                this.negate = !equal;
            }

            @Override
            boolean eval(final Resource subj, final URI pred, final Value obj, final Resource ctx) {
                final Value left = select(subj, pred, obj, ctx, this.left);
                final Value right = select(subj, pred, obj, ctx, this.right);
                final boolean equals = Objects.equals(left, right);
                return equals ^ this.negate;
            }

            private Value select(final Resource subj, final URI pred, final Value obj,
                    final Resource ctx, final char c) {
                switch (c) {
                case 's':
                    return subj;
                case 'p':
                    return pred;
                case 'o':
                    return obj;
                case 'c':
                    return ctx;
                case 'l':
                    return this.leftValue;
                case 'r':
                    return this.rightValue;
                default:
                    throw new Error();
                }
            }

        }

        private static final class AndFilter extends Filter {

            private final Filter left;

            private final Filter right;

            AndFilter(final Filter left, final Filter right) {
                this.left = left;
                this.right = right;
            }

            @Override
            Filter normalize(final Function<Value, Value> normalizer) {
                final Filter left = this.left.normalize(normalizer);
                final Filter right = this.right.normalize(normalizer);
                return left == this.left && right == this.right ? this
                        : new AndFilter(left, right);
            }

            @Override
            boolean eval(final Resource subj, final URI pred, final Value obj, final Resource ctx) {
                return this.left.eval(subj, pred, obj, ctx)
                        && this.right.eval(subj, pred, obj, ctx);
            }

        }

    }

}
