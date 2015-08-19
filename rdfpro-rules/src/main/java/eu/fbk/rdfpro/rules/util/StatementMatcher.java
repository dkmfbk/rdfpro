package eu.fbk.rdfpro.rules.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

public final class StatementMatcher {

    @Nullable
    private final Function<Object, Object> normalizer;

    private final byte[] masks;

    private final int[][] tables;

    private final Object[] values;

    private final Object[] normalizedValues; // modified during use

    private final URI nil;

    private StatementMatcher(@Nullable final Function<Object, Object> normalizer,
            final byte[] masks, final int[][] tables, final Object[] values) {
        this.normalizer = normalizer;
        this.masks = masks;
        this.tables = tables;
        this.values = values;
        this.normalizedValues = normalizer == null ? values : values.clone();
        this.nil = normalizer == null ? SESAME.NIL : (URI) normalizer.apply(SESAME.NIL);
    }

    public StatementMatcher normalize(@Nullable final Function<Object, Object> normalizer) {
        return normalizer == null ? this : new StatementMatcher(normalizer, this.masks,
                this.tables, this.values);
    }

    public boolean matchAll() {
        for (final byte mask : this.masks) {
            if (mask == 0) {
                return true;
            }
        }
        return false;
    }

    public boolean match(final Statement stmt) {
        return match(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), stmt.getContext());
    }

    public boolean match(final Resource subj, final URI pred, final Value obj,
            @Nullable Resource ctx) {

        ctx = replaceNull(ctx);
        for (int i = 0; i < this.masks.length; ++i) {
            final byte mask = this.masks[i];
            final int[] table = this.tables[i];
            final int hash = hash(subj, pred, obj, ctx, mask);
            for (int slot = (hash & 0x7FFFFFFF) % table.length; table[slot] != 0; slot = next(
                    slot, table.length)) {
                final int offset = match(subj, pred, obj, ctx, mask, hash, table[slot]);
                if (offset >= 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public <T> List<T> map(final Resource subj, final URI pred, final Value obj,
            @Nullable Resource ctx, final Class<T> clazz) {

        ctx = replaceNull(ctx);
        List<T> result = Collections.emptyList();
        for (int i = 0; i < this.masks.length; ++i) {
            final byte mask = this.masks[i];
            final int[] table = this.tables[i];
            final int hash = hash(subj, pred, obj, ctx, mask);
            for (int slot = (hash & 0x7FFFFFFF) % table.length; table[slot] != 0; slot = next(
                    slot, table.length)) {
                int offset = match(subj, pred, obj, ctx, mask, hash, table[slot]);
                if (offset >= 0) {
                    for (; this.normalizedValues[offset] != null; ++offset) {
                        if (clazz.isInstance(this.normalizedValues[offset])) {
                            if (result.isEmpty()) {
                                result = new ArrayList<>();
                            }
                            result.add(clazz.cast(this.normalizedValues[offset]));
                        }
                    }
                }
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private <T extends Value> T replaceNull(@Nullable final T value) {
        return value != null ? value : (T) this.nil;
    }

    private int match(final Resource subj, final URI pred, final Value obj, final Resource ctx,
            final byte mask, final int hash, final int cell) {

        // Check that lower 12 bits of the hash matches with lower 12 bits of cell
        if (((hash ^ cell) & 0x00000FFF) != 0) {
            return -1;
        }

        // Use the higher 20 bits of the cell as an index in the value array
        final int offset = cell >>> 12;

        // Normalize if necessary (the lack of synchronization is deliberate)
        if (this.normalizer != null
                && !Objects.equal(this.normalizedValues[offset - 1], this.normalizer)) {
            for (int i = offset; this.normalizedValues[i] != null; ++i) {
                this.normalizedValues[i] = this.normalizer.apply(i);
            }
            this.normalizedValues[offset - 1] = this.normalizer;
        }

        // Check that the quad matches the constants in the value array
        int index = offset;
        if ((mask & 0x01) != 0 && !this.normalizedValues[index++].equals(subj)) {
            return -1;
        }
        if ((mask & 0x02) != 0 && !this.normalizedValues[index++].equals(pred)) {
            return -1;
        }
        if ((mask & 0x04) != 0 && !this.normalizedValues[index++].equals(obj)) {
            return -1;
        }
        if ((mask & 0x08) != 0 && !this.normalizedValues[index].equals(ctx)) {
            return -1;
        }
        return offset + Integer.bitCount(mask);
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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private final Map<List<Value>, Collection<Object>>[] patternMaps;

        private int numValues;

        private int numMasks;

        @SuppressWarnings("unchecked")
        Builder() {
            // There are at most 16 masks to consider
            this.patternMaps = new Map[16];
            this.numValues = 0;
            this.numMasks = 0;
        }

        public Builder addPattern(final StatementPattern pattern, final Object... mappedValues) {
            return addValues((Resource) variableValue(pattern.getSubjectVar()),
                    (URI) variableValue(pattern.getPredicateVar()),
                    variableValue(pattern.getObjectVar()), //
                    (Resource) variableValue(pattern.getContextVar()), mappedValues);
        }

        public Builder addValues(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj, @Nullable final Resource ctx,
                final Object... mappedValues) {

            // Compute mask and pattern list starting from the four components specified
            final byte mask = mask(subj, pred, obj, ctx);
            final List<Value> pattern = Arrays.asList(subj, pred, obj, ctx);

            // Retrieve the pattern map for the mask. Create it if necessary
            Map<List<Value>, Collection<Object>> patternMap = this.patternMaps[mask];
            if (patternMap == null) {
                patternMap = new HashMap<>();
                this.patternMaps[mask] = patternMap;
                this.numMasks++;
            }

            // Retrieved the previous list of values associated to the pattern, if any
            final Collection<Object> oldMappedValues = patternMap.get(pattern);
            final int oldSize = oldMappedValues == null ? 0 : oldMappedValues.size();

            // Update the patterns map handling two cases
            if (mappedValues.length > 0) {
                // (1) There are values to add, in which case we perform merging and deduplication
                final Set<Object> set = new HashSet<>(Arrays.asList(mappedValues));
                if (oldSize != 0) {
                    set.addAll(oldMappedValues);
                }
                if (set.size() > oldSize) {
                    final List<Object> list = Arrays.asList(set.toArray(new Object[set.size()]));
                    patternMap.put(pattern, list);
                    this.numValues += list.size() - oldSize;
                }

            } else if (oldMappedValues == null) {
                // (2) It is enough to add a pattern entry mapping it to an empty value list
                patternMap.put(pattern, Collections.emptyList());
            }

            return this;
        }

        public StatementMatcher build(@Nullable final Function<Object, Object> normalizer) {

            // Compute the total size of the values array
            int valuesSize = this.numValues + 1;
            for (int mask = 0; mask < this.patternMaps.length; ++mask) {
                final Map<List<Value>, Collection<Object>> patternMap = this.patternMaps[mask];
                if (patternMap != null) {
                    valuesSize += patternMap.size() * (Integer.bitCount(mask) + 1);
                }
            }

            // Allocate data structures
            final byte[] masks = new byte[this.numMasks];
            final int[][] tables = new int[this.numMasks][];
            final Object[] values = new Object[valuesSize];

            // Initialize mask and value indexes
            int maskIndex = 0;
            int valueIndex = 1; // leave a null marker at beginning

            // Consider the patterns for each mask, starting from least selective mask
            for (final byte mask : new byte[] { 0, 8, 2, 4, 1, 10, 12, 9, 6, 3, 5, 14, 11, 13, 7,
                    15 }) {

                // Retrieve the patterns for the current mask. Abort if no patterns are defined
                final Map<List<Value>, Collection<Object>> patternMap = this.patternMaps[mask];
                if (patternMap == null) {
                    continue;
                }

                // Allocate the hash table for the mask (load factor 0.66)
                final int[] table = new int[patternMap.size()
                        + Math.max(1, patternMap.size() >> 1)];

                // Populate the hash table, appending pattern and value data to the value array
                for (final Map.Entry<List<Value>, Collection<Object>> entry : patternMap
                        .entrySet()) {
                    final int hash = hash((Resource) entry.getKey().get(0), (URI) entry.getKey()
                            .get(1), entry.getKey().get(2), (Resource) entry.getKey().get(3), mask);
                    int slot = (hash & 0x7FFFFFFF) % table.length;
                    while (table[slot] != 0) {
                        slot = next(slot, table.length);
                    }
                    table[slot] = hash & 0x00000FFF | valueIndex << 12;
                    for (final Value component : entry.getKey()) {
                        if (component != null) {
                            values[valueIndex++] = component;
                        }
                    }
                    for (final Object mappedValue : entry.getValue()) {
                        values[valueIndex++] = mappedValue;
                    }
                    values[valueIndex++] = null; // mark end of entry
                }

                // Update masks and tables structures
                masks[maskIndex] = mask;
                tables[maskIndex] = table;
                ++maskIndex;
            }

            // Build a pattern matcher using the created data structures
            return new StatementMatcher(normalizer, masks, tables, values);
        }

    }

}
