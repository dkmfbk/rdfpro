package eu.fbk.rdfpro.rules.drools;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

public final class Dictionary {

    static final int SIZE = 4 * 1024 * 1024 - 1;

    private final Value[] table;

    public Dictionary() {
        this.table = new Value[SIZE];
    }

    public Dictionary(final Dictionary source) {
        this.table = source.table.clone();
    }

    public Value[] decode(final int... ids) {
        final Value[] values = new Value[ids.length];
        for (int i = 0; i < ids.length; ++i) {
            values[i] = decode(ids[i]);
        }
        return values;
    }

    @Nullable
    public Value decode(final int id) {
        return this.table[id & 0x1FFFFFFF];
    }

    public int[] encode(final Value... values) {
        final int[] ids = new int[values.length];
        for (int i = 0; i < values.length; ++i) {
            ids[i] = encode(values[i]);
        }
        return ids;
    }

    public int encode(@Nullable final Value value) {
        if (value == null) {
            return 0;
        }
        int id = (value.hashCode() & 0x7FFFFFFF) % SIZE;
        if (id == 0) {
            id = 1; // 0 used for null context ID
        }
        final int initialID = id;
        while (true) {
            final Value storedValue = this.table[id];
            if (storedValue == null) {
                this.table[id] = value;
                break;
            }
            if (storedValue.equals(value)) {
                break;
            }
            ++id;
            if (id == SIZE) {
                id = 1;
            }
            if (id == initialID) {
                throw new Error("Dictionary full (capacity " + SIZE + ")");
            }
        }
        if (value instanceof URI) {
            return id;
        } else if (value instanceof BNode) {
            return id | 0x20000000;
        } else {
            return id | 0x40000000;
        }
    }

    public static boolean isResource(final int id) {
        return (id & 0x40000000) == 0;
    }

    public static boolean isURI(final int id) {
        return (id & 0x60000000) == 0;
    }

    public static boolean isBNode(final int id) {
        return (id & 0x60000000) == 0x20000000;
    }

    public static boolean isLiteral(final int id) {
        return (id & 0x60000000) == 0x40000000;
    }

}