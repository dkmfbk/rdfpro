/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti <francesco.corcoglioniti@gmail.com> with support by
 * Marco Rospocher, Marco Amadori and Michele Mostarda.
 * 
 * To the extent possible under law, the author has dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.io.IOException;
import java.io.Serializable;

public final class Hash implements Serializable, Comparable<Hash> {

    private static final long serialVersionUID = 1L;

    private final long high;

    private final long low;

    private Hash(final long high, final long low) {
        this.high = high;
        this.low = low;
    }

    public static Hash fromLongs(final long high, final long low) {
        return new Hash(high, low);
    }

    public static Hash murmur3(final String... args) {

        long h1 = 0;
        long h2 = 0;
        int length = 0;

        long l1 = 0;
        long l2 = 0;
        int index = 0;

        long cur = 0;
        for (int i = 0; i < args.length; ++i) {
            final boolean lastArg = i == args.length - 1;
            final String arg = args[i];
            for (int j = 0; j < arg.length(); ++j) {
                final long c = arg.charAt(j) & 0xFFFFL;
                cur = cur | c << index % 4 * 16;
                boolean process = false;
                if (lastArg && j == arg.length() - 1) {
                    l1 = index <= 3 ? cur : l1;
                    l2 = index > 3 ? cur : l2;
                    cur = 0;
                    process = true;
                } else if (index == 3) {
                    l1 = cur;
                    cur = 0;
                } else if (index == 7) {
                    l2 = cur;
                    cur = 0;
                    process = true;
                }
                if (process) {
                    l1 *= 0x87c37b91114253d5L;
                    l1 = Long.rotateLeft(l1, 31);
                    l1 *= 0x4cf5ad432745937fL;
                    h1 ^= l1;
                    h1 = Long.rotateLeft(h1, 27);
                    h1 += h2;
                    h1 = h1 * 5 + 0x52dce729;
                    l2 *= 0x4cf5ad432745937fL;
                    l2 = Long.rotateLeft(l2, 33);
                    l2 *= 0x87c37b91114253d5L;
                    h2 ^= l2;
                    h2 = Long.rotateLeft(h2, 31);
                    h2 += h1;
                    h2 = h2 * 5 + 0x38495ab5;
                    length += 16;
                    l1 = 0;
                    l2 = 0;
                    index = 0;
                    process = false;
                } else {
                    ++index;
                }
            }
        }

        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;

        h1 ^= h1 >>> 33;
        h1 *= 0xff51afd7ed558ccdL;
        h1 ^= h1 >>> 33;
        h1 *= 0xc4ceb9fe1a85ec53L;
        h1 ^= h1 >>> 33;

        h2 ^= h2 >>> 33;
        h2 *= 0xff51afd7ed558ccdL;
        h2 ^= h2 >>> 33;
        h2 *= 0xc4ceb9fe1a85ec53L;
        h2 ^= h2 >>> 33;

        h1 += h2;
        h2 += h1;

        return new Hash(h1, h2);
    }

    public long getHigh() {
        return this.high;
    }

    public long getLow() {
        return this.low;
    }

    @Override
    public int compareTo(final Hash other) {
        int result = Long.compare(this.high, other.high);
        if (result == 0) {
            result = Long.compare(this.low, other.low);
        }
        return result;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Hash)) {
            return false;
        }
        final Hash other = (Hash) object;
        return this.low == other.low && this.high == other.high;
    }

    @Override
    public int hashCode() {
        final int hh = (int) (this.high >>> 32);
        final int hl = (int) this.high;
        final int lh = (int) (this.low >>> 32);
        final int ll = (int) this.low;
        return ((hh * 37 + hl) * 37 + lh) * 37 + ll;
    }

    public void toString(final Appendable out) throws IOException {
        toStringHelper(out, this.high);
        toStringHelper(out, this.low);
    }

    @Override
    public String toString() {
        try {
            final StringBuilder builder = new StringBuilder(22);
            toString(builder);
            return builder.toString();
        } catch (final IOException ex) {
            throw new Error("Unexpected error (!)", ex);
        }
    }

    private static void toStringHelper(final Appendable out, final long l) throws IOException {
        for (int i = 60; i >= 0; i -= 6) {
            final int n = (int) (l >>> i) & 0x3F;
            if (n < 26) {
                out.append((char) (65 + n));
            } else if (n < 52) {
                out.append((char) (71 + n));
            } else if (n < 62) {
                out.append((char) (n - 4));
            } else if (n == 62) {
                out.append('_');
            } else {
                out.append('-');
            }
        }
    }

}
