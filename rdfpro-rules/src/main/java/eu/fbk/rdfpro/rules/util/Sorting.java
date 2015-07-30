package eu.fbk.rdfpro.rules.util;

import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public final class Sorting {

    private Sorting() {
    }

    public static <T> void sort(final ArrayComparator<T> comparator, final T[] array) {
        sort(comparator, array, 0, array.length);
    }

    public static <T> void sort(final ArrayComparator<T> comparator, final T[] array,
            final int lo, final int hi) {
        ForkJoinPool.commonPool().invoke(new SortTask<T>(comparator, array, lo, hi));
    }

    public interface ArrayComparator<T> {

        int size();

        int compare(T[] leftArray, int leftIndex, T[] rightArray, int rightIndex);

    }

    private static class SortTask<T> extends RecursiveAction {

        private static final long serialVersionUID = 1;

        private static final int PARALELLSORT_THRESHOLD = 1000;

        private static final int INSERTIONSORT_THRESHOLD = 7;

        private final ArrayComparator<T> comparator;

        private final T[] data;

        private final int lo;

        private final int hi;

        SortTask(final ArrayComparator<T> comparator, final T[] data, final int lo, final int hi) {
            this.comparator = comparator;
            this.data = data;
            this.lo = lo;
            this.hi = hi;
        }

        @Override
        protected void compute() {
            final int size = this.comparator.size();
            if (this.hi - this.lo < PARALELLSORT_THRESHOLD * size) {
                final T[] aux = Arrays.copyOfRange(this.data, this.lo, this.hi);
                mergeSort(this.comparator, aux, this.data, this.lo, this.hi, -this.lo);
            } else {
                final int mid = (this.lo / size + this.hi / size >>> 1) * size;
                invokeAll(new SortTask<T>(this.comparator, this.data, this.lo, mid),
                        new SortTask<T>(this.comparator, this.data, mid, this.hi));
                final T[] buffer = Arrays.copyOfRange(this.data, this.lo, mid);
                int leftIndex = 0;
                int rightIndex = mid;
                int outIndex = this.lo;
                while (leftIndex < buffer.length) {
                    if (rightIndex == this.hi
                            || this.comparator.compare(buffer, leftIndex, this.data, rightIndex) < 0) {
                        System.arraycopy(buffer, leftIndex, this.data, outIndex, size);
                        leftIndex += size;
                    } else {
                        System.arraycopy(this.data, rightIndex, this.data, outIndex, size);
                        rightIndex += size;
                    }
                    outIndex += size;
                }
            }
        }

        private static <T> void mergeSort(final ArrayComparator<T> comparator, final T[] src,
                final T[] dest, final int destLo, final int destHi, final int off) {

            final int size = comparator.size();

            if (destHi - destLo < INSERTIONSORT_THRESHOLD) {
                for (int i = destLo; i < destHi; i += size) {
                    for (int j = i; j > destLo && comparator.compare(dest, j - size, dest, j) > 0; j -= size) {
                        for (int k = 0; k < size; ++k) {
                            final T temp = dest[j + k];
                            dest[j + k] = dest[j + k - size];
                            dest[j + k - size] = temp;
                        }
                    }
                }
                return;
            }

            final int srcLo = destLo + off;
            final int srcHi = destHi + off;
            final int srcMid = (srcLo >>> 2) + (srcHi >>> 2) >>> 1 << 2;
            mergeSort(comparator, dest, src, srcLo, srcMid, -off);
            mergeSort(comparator, dest, src, srcMid, srcHi, -off);

            int destIndex = destLo;
            int srcLeftIndex = srcLo;
            int srcRightIndex = srcMid;

            while (destIndex < destHi) {
                if (srcRightIndex >= srcHi || srcLeftIndex < srcMid
                        && comparator.compare(src, srcLeftIndex, src, srcRightIndex) <= 0) {
                    System.arraycopy(src, srcLeftIndex, dest, destIndex, 4);
                    srcLeftIndex += 4;
                } else {
                    System.arraycopy(src, srcRightIndex, dest, destIndex, 4);
                    srcRightIndex += 4;
                }
                destIndex += 4;
            }
        }

    }

}
