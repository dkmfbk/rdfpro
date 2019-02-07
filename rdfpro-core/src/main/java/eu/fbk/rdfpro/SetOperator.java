/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 * 
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro;

import java.io.Serializable;
import java.util.Objects;

/**
 * A set/multiset operator.
 */
public abstract class SetOperator implements Serializable, Comparable<SetOperator> {

    private static final long serialVersionUID = 1L;

    /**
     * Set union. This {@code SetOperator} ignores duplicates and returns a unique copy of an
     * element only if it occurs at least in one of the arguments of the operation. The
     * corresponding textual representation is "u".
     */
    public static final SetOperator UNION = new SetOperator("u") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            for (int i = 0; i < counts.length; ++i) {
                if (counts[i] > 0) {
                    return 1;
                }
            }
            return 0;
        }

    };

    /**
     * Multiset union. This {@code SetOperator} returns an element if it occurs in at least one
     * argument of the operation, with a multiplicity equal to the one of the argument it occurs
     * the most. The corresponding textual representation is "U". See
     * <a href="http://en.wikipedia.org/wiki/Multiset">this page</a> for more information.
     */
    public static final SetOperator UNION_MULTISET = new SetOperator("U") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            int result = counts[0];
            for (int i = 1; i < counts.length; ++i) {
                if (counts[i] > result) {
                    result = counts[i];
                }
            }
            return result;
        }

    };

    /**
     * Multiset sum (a.k.a. union all). This {@code SetOperator} returns an element if it occurs
     * at least in one argument of the operation, with a multiplicity that is the sum of the
     * multiplicities of the element in the multisets coming from the operation arguments. The
     * corresponding textual representation is "a". See
     * <a href="http://en.wikipedia.org/wiki/Multiset">this page</a> for more information.
     */
    public static final SetOperator SUM_MULTISET = new SetOperator("a") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            int result = 0;
            for (int i = 0; i < counts.length; ++i) {
                result += counts[i];
            }
            return result;
        }

    };

    /**
     * Set intersection. This {@code SetOperator} ignores duplicates and returns an element only
     * if it occurs in all the arguments of the operation. In that case, exactly one copy of the
     * element is emitted. The corresponding textual representation is "i".
     */
    public static final SetOperator INTERSECTION = new SetOperator("i") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            for (int i = 0; i < counts.length; ++i) {
                if (counts[i] == 0) {
                    return 0;
                }
            }
            return 1;
        }

    };

    /**
     * Multiset intersection. This {@code SetOperator} returns an element if it occurs in all the
     * arguments of the operation, with a multiplicity equal to the one of the argument it occurs
     * the least. The corresponding textual representation is "I". See
     * <a href="http://en.wikipedia.org/wiki/Multiset">this page</a> for more information.
     */
    public static final SetOperator INTERSECTION_MULTISET = new SetOperator("I") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            int result = counts[0];
            for (int i = 1; i < counts.length; ++i) {
                if (counts[i] < result) {
                    result = counts[i];
                }
            }
            return result;
        }

    };

    /**
     * Set difference. This {@code SetOperator} ignores duplicates and returns the set difference
     * between the first argument (index 0) on one side, and all the other arguments on the other
     * side (i.e., {@code A0 \ A1 ... \An} or equivalently {@code A0 \ (A1 U ... U An)}). At most
     * a copy of a given element is returned. The corresponding textual representation is "d".
     */
    public static final SetOperator DIFFERENCE = new SetOperator("d") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            if (counts[0] == 0) {
                return 0;
            }
            for (int i = 1; i < counts.length; ++i) {
                if (counts[i] > 0) {
                    return 0;
                }
            }
            return 1;
        }

    };

    /**
     * Multiset difference. This {@code SetOperator} returns the multiset difference between the
     * first argument (index 0) on one side, and all the other arguments on the other side (i.e.,
     * {@code A0 \ A1 ... \An} or equivalently {@code A0 \ (A1 + ... + An)} where {@code +}
     * denotes multiset sum). The multiplicity of a returned element is equal to its multiplicity
     * in the first argument minus the sum of its multiplicities in the other arguments (rounded
     * to 0 if negative). The corresponding textual representation is "D". See
     * <a href="http://en.wikipedia.org/wiki/Multiset">this page</a> for more information.
     */
    public static final SetOperator DIFFERENCE_MULTISET = new SetOperator("D") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            int result = counts[0];
            for (int i = 1; i < counts.length && result > 0; ++i) {
                result -= counts[i];
            }
            return result > 0 ? result : 0;
        }

    };

    /**
     * Set symmetric difference. This {@code SetOperator} ignores duplicates and returns all the
     * elements that are present in at least one argument of the operator but not in all of them.
     * For two arguments, this corresponds to evaluating {@code (A0 \ A1) U (A1 \ A0)}. At most a
     * copy of an element is returned. The corresponding textual representation is "s".
     */
    public static final SetOperator SYMMETRIC_DIFFERENCE = new SetOperator("s") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            int result = 0;
            for (int i = 0; i < counts.length; ++i) {
                if (counts[i] > 0) {
                    ++result;
                }
            }
            return result < counts.length ? 1 : 0;
        }

    };

    /**
     * Multiset symmetric difference. This {@code SetOperator} returns the elements that are
     * present in at least one argument of the operator, with a multiplicity that is the
     * difference between the multiplicity in the argument it occurs the most and the multiplicity
     * in the argument it occurs the least. For two arguments, this corresponds to evaluating (the
     * multiset version of) {@code (A0 \ A1) U (A1 \ A0)}. The corresponding textual
     * representation is "S". See <a href="http://en.wikipedia.org/wiki/Multiset">this page</a>
     * for more information.
     */
    public static final SetOperator SYMMETRIC_DIFFERENCE_MULTISET = new SetOperator("S") {

        private static final long serialVersionUID = 1L;

        @Override
        public int apply(final int[] counts) {
            int min = counts[0];
            int max = min;
            for (int i = 1; i < counts.length; ++i) {
                final int n = counts[i];
                if (n < min) {
                    min = n;
                }
                if (n > max) {
                    max = n;
                }
            }
            return max - min;
        }

    };

    /**
     * At least N occurrences. Creates a parametric {@code SetOperator} that checks that the total
     * number of occurrences of a certain element, summed over all the operation arguments and
     * counting duplicates, is AT LEAST N. In that case a single copy of the element is emitted.
     * The corresponding textual representation is "N+".
     *
     * @param n
     *            the threshold, greater or equal to 1
     * @return the created {@code SetOperator} object
     */
    public static SetOperator atLeast(final int n) {
        if (n < 1) {
            throw new IllegalArgumentException("Invalid threshold " + n);
        }
        return new SetOperator(n + "+") {

            private static final long serialVersionUID = 1L;

            @Override
            public int apply(final int[] counts) {
                int result = 0;
                for (int i = 0; i < counts.length; ++i) {
                    result += counts[i];
                    // if (counts[i] > 0) {
                    // ++result;
                    // }
                }
                return result >= n ? 1 : 0;
            }

        };
    }

    /**
     * At most N occurrences. Creates a parametric {@code SetOperator} that checks that the total
     * number of occurrences of a certain element, summed over all the operation arguments and
     * counting duplicates, is AT MOST N. In that case a single copy of the element is emitted.
     * The corresponding textual representation is "N-".
     *
     * @param n
     *            the threshold, greater or equal to 1
     * @return the created {@code SetOperator} object
     */
    public static SetOperator atMost(final int n) {
        if (n < 1) {
            throw new IllegalArgumentException("Invalid threshold " + n);
        }
        return new SetOperator(n + "-") {

            private static final long serialVersionUID = 1L;

            @Override
            public int apply(final int[] counts) {
                int result = 0;
                for (int i = 0; i < counts.length; ++i) {
                    result += counts[i];
                }
                return result <= n ? 1 : 0;
            }

        };
    }

    private final String string;

    private SetOperator(final String string) {
        this.string = string;
    }

    private Object writeReplace() {
        return new SerializedForm(this.string);
    }

    /**
     * Applies the operator. The method is called for a given element, supplying it the
     * multiplicities of the element in each argument (a set or a multiset) of the operation. The
     * method returns the multiplicity of the element in the operation result.
     *
     * @param multiplicities
     *            the multiplicities of the element in the operation arguments (non-null,
     *            non-empty array)
     * @return the resulting multiplicity
     */
    public abstract int apply(int[] multiplicities);

    /**
     * {@inheritDoc} Comparison is based on the textual representation of set operators.
     */
    @Override
    public final int compareTo(final SetOperator other) {
        return this.string.compareTo(other.string);
    }

    /**
     * {@inheritDoc} Equality is implemented based on the textual representation of set operators.
     */
    @Override
    public final boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof SetOperator)) {
            return false;
        }
        final SetOperator other = (SetOperator) object;
        return this.string.equals(other.string);
    }

    /**
     * {@inheritDoc} The hash code is computed based on the textual representation of the set
     * operator.
     */
    @Override
    public final int hashCode() {
        return this.string.hashCode();
    }

    /**
     * {@inheritDoc} Returns the textual representation of the set operator that uniquely
     * identifies it.
     */
    @Override
    public final String toString() {
        return this.string;
    }

    /**
     * Returns the {@code SetOperator} for the textual representation supplied.
     *
     * @param string
     *            the textual representation, not null
     * @return the corresponding {@code SetOperator}, upon success
     */
    public static SetOperator valueOf(final String string) {
        Objects.requireNonNull(string);
        for (final SetOperator operation : new SetOperator[] { UNION, UNION_MULTISET, SUM_MULTISET,
                INTERSECTION, INTERSECTION_MULTISET, DIFFERENCE, DIFFERENCE_MULTISET,
                SYMMETRIC_DIFFERENCE, SYMMETRIC_DIFFERENCE_MULTISET }) {
            if (operation.string.equals(string)) {
                return operation;
            }
        }
        if (string.endsWith("+")) {
            final int n = Integer.parseInt(string.substring(0, string.length() - 1));
            return atLeast(n);
        }
        if (string.endsWith("-")) {
            final int n = Integer.parseInt(string.substring(0, string.length() - 1));
            return atMost(n);
        }
        throw new IllegalArgumentException("Unknown set operator '" + string + "'");
    }

    static final class SerializedForm implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String string;

        public SerializedForm(final String string) {
            this.string = string;
        }

        Object readResolve() {
            return SetOperator.valueOf(this.string);
        }

    }

}