package eu.fbk.rdfpro.rules.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO
// (1) move hash values out of hash table or drop them, revising rehashing code

final class StringIndex {

    private static final int SMALL_BUFFER_SIZE = 64 * 1024;

    private static final int LARGE_BUFFER_SIZE = SMALL_BUFFER_SIZE * 8;

    private final List<byte[]> smallBuffers;

    private final List<byte[]> largeBuffers;

    private int smallNextID;

    private int largeNextID;

    private int[] table;

    private int size;

    public StringIndex() {

        this.smallBuffers = new ArrayList<>();
        this.largeBuffers = new ArrayList<>();
        this.smallNextID = getID(true, 0, 0);
        this.largeNextID = getID(false, 0, 0);
        this.table = new int[1022]; // 511 entries, ~4K memory page
        this.size = 0;

        this.smallBuffers.add(new byte[SMALL_BUFFER_SIZE]);
        this.largeBuffers.add(new byte[LARGE_BUFFER_SIZE]);
    }

    public int size() {
        return this.size;
    }

    public boolean contains(final String string) {
        return lookup(string, false) != 0;
    }

    public int put(final String string) {
        return lookup(string, true);
    }

    public String get(final int id) {
        return get(id, new StringBuilder()).toString();
    }

    public StringBuilder get(final int id, final StringBuilder out) {
        try {
            get(id, (Appendable) out);
            return out;
        } catch (final IOException ex) {
            throw new Error(ex);
        }
    }

    public <T extends Appendable> T get(final int id, final T out) throws IOException {

        final boolean small = isSmall(id);
        final int page = getPage(id);
        int offset = getOffset(id);

        final byte[] buffer;
        int length;
        if (small) {
            buffer = this.smallBuffers.get(page);
            length = buffer[offset] & 0xFF;
            offset++;
        } else {
            buffer = this.largeBuffers.get(page);
            length = getInt(buffer, offset);
            offset += 4;
        }

        for (int i = 0; i < length; ++i) {
            final byte b = buffer[offset++];
            if (b != 0) {
                out.append((char) b);
            } else {
                out.append(getChar(buffer, offset));
                offset += 2;
            }
        }

        return out;
    }

    public int length(final int id) {
        final boolean small = isSmall(id);
        final int page = getPage(id);
        final int offset = getOffset(id);
        return small ? this.smallBuffers.get(page)[offset] & 0xFF //
                : getInt(this.largeBuffers.get(page), offset);
    }

    public boolean equals(final int id, final String string) {
        return equals(id, string, 0, string.length());
    }

    public boolean equals(final int id, final String string, final int startIndex,
            final int endIndex) {

        final boolean idSmall = isSmall(id);
        final boolean stringSmall = isSmall(string);
        if (idSmall != stringSmall) {
            return false;
        }

        final int page = getPage(id);
        int offset = getOffset(id);

        final byte[] buffer;
        int length;
        if (idSmall) {
            buffer = this.smallBuffers.get(page);
            length = buffer[offset] & 0xFF;
            offset++;
        } else {
            buffer = this.largeBuffers.get(page);
            length = getInt(buffer, offset);
            offset += 4;
        }

        if (length != endIndex - startIndex) {
            return false;
        }

        for (int i = startIndex; i < endIndex; ++i) {
            char c;
            final byte b = buffer[offset++];
            if (b != 0) {
                c = (char) b;
            } else {
                c = getChar(buffer, offset);
                offset += 2;
            }
            if (c != string.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private int lookup(final String string, final boolean canAppend) {
        final int hash = string.hashCode();
        int slot = (hash & 0x7FFFFFFF) % (this.table.length / 2) * 2;
        while (true) {
            int id = this.table[slot];
            if (id == 0) {
                if (!canAppend) {
                    return 0;
                } else if (this.size > this.table.length * 2 / 5) { // enforce load factor < .8
                    rehash();
                    return lookup(string, canAppend); // repeat after rehashing
                }
                id = append(string);
                this.table[slot] = id;
                this.table[slot + 1] = hash;
                ++this.size;
                return id;
            }
            if (this.table[slot + 1] == hash && equals(id, string)) {
                return id;
            }
            slot += 2;
            if (slot >= this.table.length) {
                slot = 0;
            }
        }
    }

    private void rehash() {
        final int newSize = this.table.length + 1; // new number of elements
        final int[] newTable = new int[2 * newSize];
        for (int oldOffset = 0; oldOffset < this.table.length; oldOffset += 2) {
            final int id = this.table[oldOffset];
            if (id != 0) {
                final int hash = this.table[oldOffset + 1];
                int newOffset = (hash & 0x7FFFFFFF) % newSize * 2;
                while (newTable[newOffset] != 0) {
                    newOffset += 2;
                    if (newOffset >= newTable.length) {
                        newOffset = 0;
                    }
                }
                newTable[newOffset] = id;
                newTable[newOffset + 1] = hash;
            }
        }
        this.table = newTable;
    }

    private int append(final String string) {

        final boolean small = isSmall(string);
        final int length = string.length();

        byte[] buffer;
        int offset;
        int id;
        if (small) {
            offset = getOffset(this.smallNextID);
            if (offset + 1 + length * 3 > SMALL_BUFFER_SIZE) {
                this.smallBuffers.add(new byte[SMALL_BUFFER_SIZE]);
                this.smallNextID = getID(true, this.smallBuffers.size() - 1, 0);
                offset = 0;
            }
            id = this.smallNextID;
            buffer = this.smallBuffers.get(this.smallBuffers.size() - 1);
            buffer[offset++] = (byte) length;
        } else {
            offset = getOffset(this.largeNextID);
            if (offset + 4 + length * 3 > LARGE_BUFFER_SIZE) {
                this.largeBuffers.add(new byte[LARGE_BUFFER_SIZE]);
                this.largeNextID = getID(false, this.largeBuffers.size() - 1, 0);
                offset = 0;
            }
            id = this.largeNextID;
            buffer = this.largeBuffers.get(this.largeBuffers.size() - 1);
            putInt(buffer, offset, length);
            offset += 4;
        }

        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            if (ch > 0 && ch <= 127) {
                buffer[offset++] = (byte) ch;
            } else {
                buffer[offset++] = (byte) 0;
                putChar(buffer, offset, ch);
                offset += 2;
            }
        }

        if (small) {
            this.smallNextID = getID(small, this.smallBuffers.size() - 1, offset);
        } else {
            this.largeNextID = getID(small, this.largeBuffers.size() - 1, offset);
        }

        return id;
    }

    private int getID(final boolean small, final int page, final int offset) {
        return small ? (page + 1 & 0x7FFF) << 16 | offset & 0xFFFF : 0x80000000
                | (page + 1 & 0x7FFF) << 16 | offset + 7 >> 3 & 0xFFFF;
    }

    private int getPage(final int id) {
        return (id >> 16 & 0x7FFF) - 1;
    }

    private int getOffset(final int id) {
        return (id & 0x80000000) == 0 ? id & 0xFFFF : (id & 0xFFFF) << 3;
    }

    private boolean isSmall(final int id) {
        return (id & 0x80000000) == 0;
    }

    private boolean isSmall(final String string) {
        return string.length() <= 255;
    }

    private static char getChar(final byte[] buffer, final int offset) {
        final int byte0 = buffer[offset] & 0xFF;
        final int byte1 = buffer[offset + 1] & 0xFF;
        return (char) (byte0 << 8 | byte1);
    }

    private static void putChar(final byte[] buffer, final int offset, final char value) {
        buffer[offset] = (byte) (value >>> 8);
        buffer[offset + 1] = (byte) value;
    }

    private static int getInt(final byte[] buffer, final int offset) {
        final int byte0 = buffer[offset] & 0xFF;
        final int byte1 = buffer[offset + 1] & 0xFF;
        final int byte2 = buffer[offset + 2] & 0xFF;
        final int byte3 = buffer[offset + 3] & 0xFF;
        return byte0 << 24 | byte1 << 16 | byte2 << 8 | byte3;
    }

    private static void putInt(final byte[] buffer, final int offset, final int value) {
        buffer[offset] = (byte) (value >>> 24);
        buffer[offset + 1] = (byte) (value >>> 16);
        buffer[offset + 2] = (byte) (value >>> 8);
        buffer[offset + 3] = (byte) value;
    }

}
