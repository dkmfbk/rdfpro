package eu.fbk.rdfpro.rules.util;

import java.util.Arrays;

abstract class Buffer {

    public static Buffer newFixedBuffer(final byte[] bytes) {
        return new FixedBuffer(bytes);
    }

    public static Buffer newResizableBuffer() {
        return new ResizableBuffer();
    }

    public abstract byte read(long offset);

    public abstract short readShort(final long offset);

    public abstract int readInt(long offset);

    public abstract long readNumber(final long offset, final int length);

    public abstract String readString(final long offset, final int length);

    public abstract void write(final long offset, final byte b);

    public abstract void writeShort(final long offset, final short n);

    public abstract void writeInt(final long offset, final int n);

    public abstract void writeLong(final long offset, final long n);

    public abstract void writeNumber(final long offset, final int length, final long n);

    public abstract void writeBytes(long offset, final byte[] bytes, int index, int length);

    public abstract void writeBuffer(long offset, final Buffer buffer, long bufferOffset,
            long length);

    public abstract int writeString(long offset, String s);

    public abstract boolean equalString(final long offset, final int length, final String s);

    private static final class FixedBuffer extends Buffer {

        private final byte[] buffer;

        FixedBuffer(final byte[] buffer) {
            this.buffer = buffer;
        }

        @Override
        public byte read(final long offset) {
            return this.buffer[(int) offset];
        }

        @Override
        public short readShort(final long offset) {
            final int index = (int) offset;
            return (short) (this.buffer[index] << 8 | this.buffer[index + 1] & 0xFF);
        }

        @Override
        public int readInt(final long offset) {
            final int index = (int) offset;
            return this.buffer[index] << 24 | (this.buffer[index + 1] & 0xFF) << 16
                    | (this.buffer[index + 2] & 0xFF) << 8 | this.buffer[index + 3] & 0xFF;
        }

        @Override
        public long readNumber(final long offset, final int length) {
            int index = (int) offset;
            long result = 0;
            for (int i = 0; i < length; ++i) {
                result = result << 8 | this.buffer[index++] & 0xFFL;
            }
            return result;
        }

        @Override
        public String readString(final long offset, final int length) {
            final StringBuilder builder = new StringBuilder();
            int index = (int) offset;
            int temp = 0;
            for (int i = 0; i < length; ++i) {
                final int b = this.buffer[index++] & 0xFF;
                if (temp != 0) {
                    if (temp < 0) {
                        temp = b;
                    } else {
                        builder.append((char) (temp << 8 | b));
                        temp = 0;
                    }
                } else if (b == 0) {
                    temp = -1;
                } else {
                    builder.append((char) b);
                }
            }
            return builder.toString();
        }

        @Override
        public void write(final long offset, final byte b) {
            this.buffer[(int) offset] = b;
        }

        @Override
        public void writeShort(final long offset, final short n) {
            final int index = (int) offset;
            this.buffer[index] = (byte) (n >>> 8);
            this.buffer[index + 1] = (byte) n;
        }

        @Override
        public void writeInt(final long offset, final int n) {
            final int index = (int) offset;
            this.buffer[index] = (byte) (n >>> 24);
            this.buffer[index + 1] = (byte) (n >>> 16);
            this.buffer[index + 2] = (byte) (n >>> 8);
            this.buffer[index + 3] = (byte) n;
        }

        @Override
        public void writeLong(final long offset, final long n) {
            final int index = (int) offset;
            this.buffer[index] = (byte) (n >>> 56);
            this.buffer[index + 1] = (byte) (n >>> 48);
            this.buffer[index + 2] = (byte) (n >>> 40);
            this.buffer[index + 3] = (byte) (n >>> 32);
            this.buffer[index + 4] = (byte) (n >>> 24);
            this.buffer[index + 5] = (byte) (n >>> 16);
            this.buffer[index + 6] = (byte) (n >>> 8);
            this.buffer[index + 7] = (byte) n;
        }

        @Override
        public void writeNumber(final long offset, final int length, final long n) {
            int index = (int) offset;
            for (int i = 1; i <= length; ++i) {
                this.buffer[index++] = (byte) (n >>> (length - i << 3));
            }
        }

        @Override
        public void writeBytes(final long offset, final byte[] bytes, final int index,
                final int length) {
            System.arraycopy(bytes, index, this.buffer, (int) offset, length);
        }

        @Override
        public void writeBuffer(final long thisOffset, final Buffer buffer,
                final long bufferOffset, long length) {

            if (buffer instanceof FixedBuffer) {
                writeBytes(thisOffset, ((FixedBuffer) buffer).buffer, (int) bufferOffset,
                        (int) length);
            }

            final ResizableBuffer rbuffer = (ResizableBuffer) buffer;
            int thisIndex = (int) thisOffset;
            int bufferIndex = (int) (bufferOffset >>> 16);
            int byteIndex = (int) bufferOffset & 0xFFFF;
            while (true) {
                final int len = (int) Math.min(length, 0x10000 - byteIndex);
                final byte[] bytes = rbuffer.buffer(bufferIndex);
                System.arraycopy(bytes, byteIndex, this.buffer, thisIndex, len);
                length -= len;
                if (length == 0) {
                    break;
                }
                thisIndex += len;
                byteIndex = 0;
                ++bufferIndex;
            }
        }

        @Override
        public int writeString(final long offset, final String s) {
            int index = (int) offset;
            final int length = s.length();
            int byteLength = length;
            for (int i = 0; i < length; ++i) {
                final char ch = s.charAt(i);
                if (ch > 0 && ch <= 127) {
                    this.buffer[index++] = (byte) ch;
                } else {
                    byteLength += 2;
                    this.buffer[index++] = 0;
                    this.buffer[index++] = (byte) (ch >>> 8);
                    this.buffer[index++] = (byte) ch;
                }
            }
            return byteLength;
        }

        @Override
        public boolean equalString(final long offset, final int length, final String s) {

            final int strLength = s.length();
            if (strLength == 0) {
                return length == 0;
            } else if (strLength > length) {
                return false;
            }

            int index = (int) offset;
            int temp = -1;
            int strIndex = 0;
            for (int i = 0; i < length; ++i) {
                final byte b = this.buffer[index++];
                if (temp >= 0) {
                    if (temp == Integer.MAX_VALUE) {
                        temp = b & 0xFF;
                    } else {
                        final char ch = (char) (temp << 8 | b & 0xFF);
                        if (strIndex == strLength || s.charAt(strIndex++) != ch) {
                            return false;
                        }
                        temp = -1;
                    }
                } else if (b == 0) {
                    temp = Integer.MAX_VALUE;
                } else {
                    final char ch = (char) b;
                    if (strIndex == strLength || s.charAt(strIndex++) != ch) {
                        return false;
                    }
                }
            }

            return strIndex == strLength;
        }

    }

    private static final class ResizableBuffer extends Buffer {

        private byte[][] buffers;

        ResizableBuffer() {
            this.buffers = new byte[4][];
        }

        @Override
        public byte read(final long offset) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            return buffer(bufferIndex)[byteIndex];
        }

        @Override
        public short readShort(final long offset) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            if (byteIndex < 0x10000 - 1) {
                final byte[] buffer = buffer(bufferIndex);
                return (short) (buffer[byteIndex] << 8 | buffer[byteIndex + 1] & 0xFF);
            } else {
                return (short) readNumber(offset, 2);
            }
        }

        @Override
        public int readInt(final long offset) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            if (byteIndex < 0x10000 - 3) {
                final byte[] buffer = buffer(bufferIndex);
                return buffer[byteIndex] << 24 | (buffer[byteIndex + 1] & 0xFF) << 16
                        | (buffer[byteIndex + 2] & 0xFF) << 8 | buffer[byteIndex + 3] & 0xFF;
            } else {
                return (int) readNumber(offset, 4);
            }
        }

        @Override
        public long readNumber(final long offset, final int length) {
            int bufferIndex = (int) (offset >>> 16);
            int byteIndex = (int) offset & 0xFFFF;
            byte[] buffer = buffer(bufferIndex);
            long result = 0;
            for (int i = 0; i < length; ++i) {
                result = result << 8 | buffer[byteIndex++] & 0xFFL;
                if (byteIndex == 0x10000) {
                    buffer = buffer(++bufferIndex);
                    byteIndex = 0;
                }
            }
            return result;
        }

        @Override
        public String readString(final long offset, final int length) {

            assert offset >= 0;
            assert length >= 0;

            final StringBuilder builder = new StringBuilder();
            int bufferIndex = (int) (offset >>> 16);
            int byteIndex = (int) offset & 0xFFFF;
            byte[] buffer = buffer(bufferIndex);
            int temp = 0;
            for (int i = 0; i < length; ++i) {
                final int b = buffer[byteIndex++] & 0xFF;
                if (byteIndex == 0x10000) {
                    buffer = buffer(++bufferIndex);
                    byteIndex = 0;
                }
                if (temp != 0) {
                    if (temp < 0) {
                        temp = b;
                    } else {
                        builder.append((char) (temp << 8 | b));
                        temp = 0;
                    }
                } else if (b == 0) {
                    temp = -1;
                } else {
                    builder.append((char) b);
                }
            }

            return builder.toString();
        }

        @Override
        public void write(final long offset, final byte b) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            final byte[] buffer = buffer(bufferIndex);
            buffer[byteIndex] = b;
        }

        @Override
        public void writeShort(final long offset, final short n) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            if (byteIndex < 0x10000 - 1) {
                final byte[] buffer = buffer(bufferIndex);
                buffer[byteIndex] = (byte) (n >>> 8);
                buffer[byteIndex + 1] = (byte) n;
            } else {
                writeNumber(offset, 2, n);
            }
        }

        @Override
        public void writeInt(final long offset, final int n) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            if (byteIndex < 0x10000 - 3) {
                final byte[] buffer = buffer(bufferIndex);
                buffer[byteIndex] = (byte) (n >>> 24);
                buffer[byteIndex + 1] = (byte) (n >>> 16);
                buffer[byteIndex + 2] = (byte) (n >>> 8);
                buffer[byteIndex + 3] = (byte) n;
            } else {
                writeNumber(offset, 4, n);
            }
        }

        @Override
        public void writeLong(final long offset, final long n) {
            final int bufferIndex = (int) (offset >>> 16);
            final int byteIndex = (int) offset & 0xFFFF;
            if (byteIndex < 0x10000 - 7) {
                final byte[] buffer = buffer(bufferIndex);
                buffer[byteIndex] = (byte) (n >>> 56);
                buffer[byteIndex + 1] = (byte) (n >>> 48);
                buffer[byteIndex + 2] = (byte) (n >>> 40);
                buffer[byteIndex + 3] = (byte) (n >>> 32);
                buffer[byteIndex + 4] = (byte) (n >>> 24);
                buffer[byteIndex + 5] = (byte) (n >>> 16);
                buffer[byteIndex + 6] = (byte) (n >>> 8);
                buffer[byteIndex + 7] = (byte) n;
            } else {
                writeNumber(offset, 8, n);
            }
        }

        @Override
        public void writeNumber(final long offset, final int length, final long n) {
            int bufferIndex = (int) (offset >>> 16);
            int byteIndex = (int) offset & 0xFFFF;
            byte[] buffer = buffer(bufferIndex);
            for (int i = 1; i <= length; ++i) {
                buffer[byteIndex++] = (byte) (n >>> (length - i << 3));
                if (byteIndex == 0x10000) {
                    buffer = buffer(++bufferIndex);
                    byteIndex = 0;
                }
            }
        }

        @Override
        public void writeBytes(final long offset, final byte[] bytes, int index, int length) {
            int bufferIndex = (int) (offset >>> 16);
            int byteIndex = (int) offset & 0xFFFF;
            while (true) {
                final int len = Math.min(length, 0x10000 - byteIndex);
                final byte[] buffer = buffer(bufferIndex);
                System.arraycopy(bytes, index, buffer, byteIndex, len);
                length -= len;
                if (length == 0) {
                    break;
                }
                index += len;
                byteIndex = 0;
                ++bufferIndex;
            }
        }

        @Override
        public void writeBuffer(final long thisOffset, final Buffer buffer,
                final long bufferOffset, long length) {

            if (buffer instanceof FixedBuffer) {
                writeBytes(thisOffset, ((FixedBuffer) buffer).buffer, (int) bufferOffset,
                        (int) length);

            } else {
                int thisBufferIndex = (int) (thisOffset >>> 16);
                int thisByteIndex = (int) thisOffset & 0xFFFF;
                int otherBufferIndex = (int) (bufferOffset >>> 16);
                int otherByteIndex = (int) bufferOffset & 0xFFFF;

                while (true) {
                    final int len = (int) Math.min(length,
                            Math.min(0x10000 - otherByteIndex, 0x10000 - thisByteIndex));
                    final byte[] thisBuffer = buffer(thisBufferIndex);
                    final byte[] otherBuffer = ((ResizableBuffer) buffer).buffer(otherBufferIndex);
                    System.arraycopy(otherBuffer, otherByteIndex, thisBuffer, thisByteIndex, len);
                    length -= len;
                    if (length == 0) {
                        break;
                    }
                    thisByteIndex += len;
                    if (thisByteIndex == 0x10000) {
                        ++thisBufferIndex;
                        thisByteIndex = 0;
                    }
                    otherByteIndex += len;
                    if (otherByteIndex == 0x10000) {
                        ++otherBufferIndex;
                        otherByteIndex = 0;
                    }
                }
            }
        }

        @Override
        public int writeString(final long offset, final String s) {

            assert offset >= 0L;
            assert s != null;

            int bufferIndex = (int) (offset >>> 16);
            int byteIndex = (int) offset & 0xFFFF;
            byte[] buffer = buffer(bufferIndex);
            final int length = s.length();
            int byteLength = length;
            for (int i = 0; i < length; ++i) {
                final char ch = s.charAt(i);
                if (ch > 0 && ch <= 127) {
                    buffer[byteIndex++] = (byte) ch;
                    if (byteIndex == 0x10000) {
                        buffer = buffer(++bufferIndex);
                        byteIndex = 0;
                    }
                } else {
                    byteLength += 2;
                    for (final byte b : new byte[] { 0, (byte) (ch >>> 8), (byte) ch }) {
                        buffer[byteIndex++] = b;
                        if (byteIndex == 0x10000) {
                            buffer = buffer(++bufferIndex);
                            byteIndex = 0;
                        }
                    }
                }
            }

            return byteLength;
        }

        @Override
        public boolean equalString(final long offset, final int length, final String s) {

            assert offset >= 0;
            assert length >= 0;
            assert s != null;

            final int strLength = s.length();
            if (strLength == 0) {
                return length == 0;
            } else if (strLength > length) {
                return false;
            }

            int bufferIndex = (int) (offset >>> 16);
            int byteIndex = (int) offset & 0xFFFF;
            byte[] buffer = buffer(bufferIndex);
            int temp = -1;
            int strIndex = 0;
            for (int i = 0; i < length; ++i) {
                final byte b = buffer[byteIndex++];
                if (byteIndex == 0x10000) {
                    buffer = buffer(++bufferIndex);
                    byteIndex = 0;
                }
                if (temp >= 0) {
                    if (temp == Integer.MAX_VALUE) {
                        temp = b & 0xFF;
                    } else {
                        final char ch = (char) (temp << 8 | b & 0xFF);
                        if (strIndex == strLength || s.charAt(strIndex++) != ch) {
                            return false;
                        }
                        temp = -1;
                    }
                } else if (b == 0) {
                    temp = Integer.MAX_VALUE;
                } else {
                    final char ch = (char) b;
                    if (strIndex == strLength || s.charAt(strIndex++) != ch) {
                        return false;
                    }
                }
            }

            return strIndex == strLength;
        }

        private byte[] buffer(final int index) {
            assert index >= 0;
            final byte[][] buffers = this.buffers;
            if (index < buffers.length) {
                final byte[] buffer = buffers[index];
                if (buffer != null) {
                    return buffer;
                }
            }
            return bufferHelper(index);
        }

        private synchronized byte[] bufferHelper(final int index) {
            if (index >= this.buffers.length) {
                this.buffers = Arrays.copyOf(this.buffers, this.buffers.length << 1);
            }
            byte[] buffer = this.buffers[index];
            if (buffer == null) {
                buffer = new byte[64 * 1024];
                this.buffers[index] = buffer;
            }
            return buffer;
        }

    }

}
