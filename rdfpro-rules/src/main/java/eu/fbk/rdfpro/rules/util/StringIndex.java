package eu.fbk.rdfpro.rules.util;

import java.nio.ByteBuffer;
import java.util.List;

public final class StringIndex {

    private static final int SMALL_BUFFER_SIZE = 64 * 1024;

    private static final int LARGE_BUFFER_SIZE = SMALL_BUFFER_SIZE * 8;

    private List<ByteBuffer> smallBuffers;

    private List<ByteBuffer> largeBuffers;

    private int smallNextID;

    private int largeNextID;

    private int[] table;

    private int size;

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

        final boolean small = isSmall(id);
        final int page = getPage(id);
        int offset = getOffset(id);

        final ByteBuffer buffer;
        int length;
        if (small) {
            buffer = this.smallBuffers.get(page);
            length = buffer.get(offset);
            offset++;
        } else {
            buffer = this.largeBuffers.get(page);
            length = buffer.getInt(offset);
            offset += 4;
        }

        final StringBuilder builder = new StringBuilder();
        while (builder.length() < length) {
            final byte b = buffer.get(offset++);
            if (b != 0) {
                builder.append((char) b);
            } else {
                builder.append(buffer.getChar(offset));
                offset += 2;
            }
        }
        return builder.toString();
    }

    public int length(final int id) {
        final boolean small = isSmall(id);
        final int page = getPage(id);
        final int offset = getOffset(id);
        return small ? this.smallBuffers.get(page).get(offset) //
                : this.largeBuffers.get(page).getInt(offset);
    }

    public boolean equals(final int id, final String string) {

        final boolean idSmall = isSmall(id);
        final boolean stringSmall = isSmall(string);
        if (idSmall != stringSmall) {
            return false;
        }

        final int page = getPage(id);
        int offset = getOffset(id);

        final ByteBuffer buffer;
        int length;
        if (idSmall) {
            buffer = this.smallBuffers.get(page);
            length = buffer.get(offset);
            offset++;
        } else {
            buffer = this.largeBuffers.get(page);
            length = buffer.getInt(offset);
            offset += 4;
        }

        if (length != string.length()) {
            return false;
        }

        for (int i = 0; i < length; ++i) {
            char c;
            final byte b = buffer.get(offset++);
            if (b != 0) {
                c = (char) b;
            } else {
                c = buffer.getChar(offset);
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
        int offset = (hash & 0x7FFFFFFF) % (this.table.length / 2) * 2;
        while (true) {
            int id = this.table[offset];
            if (id == 0) {
                if (!canAppend) {
                    return 0;
                } else if (this.size > this.table.length / 4) { // enforce load factor < .5
                    rehash();
                    return lookup(string, canAppend); // repeat after rehashing
                }
                id = append(string);
                this.table[offset] = id;
                this.table[offset + 1] = hash;
                ++this.size;
                return id;
            }
            if (this.table[offset + 1] == hash && equals(id, string)) {
                return id;
            }
            offset += 2;
            if (offset >= this.table.length) {
                offset = 0;
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

        ByteBuffer buffer;
        int offset;
        int id;
        if (small) {
            offset = getOffset(this.smallNextID);
            if (offset + 1 + length * 3 > SMALL_BUFFER_SIZE) {
                this.smallBuffers.add(ByteBuffer.allocate(SMALL_BUFFER_SIZE));
                this.smallNextID = getID(true, this.smallBuffers.size() - 1, 0);
                offset = 0;
            }
            id = this.smallNextID;
            buffer = this.smallBuffers.get(this.smallBuffers.size() - 1);
            buffer.put(offset, (byte) length);
            offset++;
        } else {
            offset = getOffset(this.largeNextID);
            if (offset + 4 + length * 3 > LARGE_BUFFER_SIZE) {
                this.largeBuffers.add(ByteBuffer.allocate(LARGE_BUFFER_SIZE));
                this.largeNextID = getID(false, this.largeBuffers.size() - 1, 0);
                offset = 0;
            }
            id = this.largeNextID;
            buffer = this.largeBuffers.get(this.largeBuffers.size() - 1);
            buffer.putInt(offset, length);
            offset += 4;
        }

        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            if (ch > 0 && ch <= 255) {
                buffer.put(offset++, (byte) ch);
            } else {
                buffer.put(offset++, (byte) 0);
                buffer.putChar(offset, ch);
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
        return small ? (page & 0x7FFF) << 16 | offset & 0xFFFF : 0x80000000
                | (page & 0x7FFF) << 16 | offset + 7 >> 3 & 0xFFFF;
    }

    private int getPage(final int id) {
        return id >> 16 & 0x7FFF;
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

}
