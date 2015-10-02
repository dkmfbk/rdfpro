package eu.fbk.rdfpro.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;

final class Iterators {

    private static final Logger LOGGER = LoggerFactory.getLogger(Iterators.class);

    @Nullable
    public static <T> Iterator<T> unmodifiable(@Nullable final Iterator<T> iterator) {
        return iterator == null ? null : new UnmodifiableIterator<T>(iterator);
    }

    public static <T> Iterator<T> concat(
            final Iterator<? extends Iterator<? extends T>> iteratorSupplier) {
        return new ConcatIterator<T>(Objects.requireNonNull(iteratorSupplier));
    }

    public static <T> Iterator<T> filter(final Iterator<T> iterator,
            final Predicate<? super T> predicate) {
        return new FilterIterator<T>(Objects.requireNonNull(iterator),
                Objects.requireNonNull(predicate));
    }

    public static <T, R> Iterator<R> transform(final Iterator<T> iterator,
            final Function<? super T, ? extends R> transformer) {
        return new TransformIterator<T, R>(Objects.requireNonNull(iterator),
                Objects.requireNonNull(transformer));
    }

    public static <T> Iterator<T> forIteration(final Iteration<? extends T, ?> iteration) {
        return new IterationIterator<T>(iteration);
    }

    public static <T, E extends Exception> CloseableIteration<T, E> toIteration(
            final Iterator<T> iterator) {
        return new IteratorIteration<T, E>(iterator);
    }

    private static final class UnmodifiableIterator<T> implements Iterator<T>, AutoCloseable {

        private final Iterator<T> iterator;

        UnmodifiableIterator(final Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public T next() {
            return this.iterator.next();
        }

        @Override
        public void close() {
            if (this.iterator instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) this.iterator).close();
                } catch (final Throwable ex) {
                    LOGGER.error("Could not close iterator", ex);
                }
            }
        }

    }

    private static final class ConcatIterator<T> implements Iterator<T>, AutoCloseable {

        private final Iterator<? extends Iterator<? extends T>> iteratorSupplier;

        @Nullable
        private Iterator<? extends T> currentIterator;

        @Nullable
        private Iterator<? extends T> removeIterator;

        private boolean eof;

        ConcatIterator(final Iterator<? extends Iterator<? extends T>> iteratorSupplier) {
            this.iteratorSupplier = iteratorSupplier;
            this.currentIterator = null;
            this.removeIterator = null;
            this.eof = false;
        }

        @Override
        public boolean hasNext() {
            if (this.eof) {
                return false;
            }
            while (true) {
                if (this.currentIterator != null) {
                    if (this.currentIterator.hasNext()) {
                        return true;
                    } else if (this.currentIterator != this.removeIterator) {
                        IO.closeQuietly(this.currentIterator);
                    }
                }
                if (!this.iteratorSupplier.hasNext()) {
                    this.eof = true;
                    return false;
                }
                this.currentIterator = this.iteratorSupplier.next();
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final T element = this.currentIterator.next();
            if (this.removeIterator != this.currentIterator) {
                IO.closeQuietly(this.removeIterator);
            }
            this.removeIterator = this.currentIterator;
            return element;
        }

        @Override
        public void remove() {
            if (this.removeIterator == null) {
                throw new NoSuchElementException();
            }
            this.removeIterator.remove();
            this.removeIterator = null;
        }

        @Override
        public void close() throws Exception {
            this.eof = true;
            IO.closeQuietly(this.removeIterator);
            IO.closeQuietly(this.currentIterator);
            IO.closeQuietly(this.iteratorSupplier);
        }

    }

    private static final class FilterIterator<T> implements Iterator<T>, AutoCloseable {

        private final Iterator<? extends T> iterator;

        private final Predicate<? super T> predicate;

        private T next;

        private boolean eof;

        private boolean removable;

        FilterIterator(final Iterator<? extends T> iterator, final Predicate<? super T> predicate) {
            this.iterator = iterator;
            this.predicate = predicate;
            this.next = null;
            this.eof = false;
            this.removable = false;
        }

        @Override
        public boolean hasNext() {
            if (!this.eof) {
                if (this.next != null) {
                    return true;
                }
                while (this.next == null && this.iterator.hasNext()) {
                    final T element = this.iterator.next();
                    this.removable = false;
                    if (this.predicate.test(element)) {
                        this.next = element;
                        return true;
                    }
                }
                this.eof = true;
            }
            return false;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final T result = this.next;
            this.next = null;
            this.removable = true;
            return result;
        }

        @Override
        public void remove() {
            if (!this.removable) {
                throw new NoSuchElementException();
            }
            this.iterator.remove();
            this.removable = false;
        }

        @Override
        public void close() throws Exception {
            IO.closeQuietly(this.iterator);
        }

    }

    private static final class TransformIterator<T, R> implements Iterator<R>, AutoCloseable {

        private final Iterator<T> iterator;

        private final Function<? super T, ? extends R> transformer;

        TransformIterator(final Iterator<T> iterator,
                final Function<? super T, ? extends R> transformer) {
            this.iterator = iterator;
            this.transformer = transformer;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public R next() {
            return this.transformer.apply(this.iterator.next());
        }

        @Override
        public void remove() {
            this.iterator.remove();
        }

        @Override
        public void close() throws Exception {
            IO.closeQuietly(this.iterator);
        }

    }

    private static final class IterationIterator<T> implements Iterator<T>, AutoCloseable {

        private final Iteration<? extends T, ?> iteration;

        IterationIterator(final Iteration<? extends T, ?> iteration) {
            this.iteration = iteration;
        }

        @Override
        public boolean hasNext() {
            try {
                return this.iteration.hasNext();
            } catch (RuntimeException | Error ex) {
                close();
                throw ex;
            } catch (final Throwable ex) {
                close();
                throw new RuntimeException(ex);
            }
        }

        @Override
        public T next() {
            try {
                return this.iteration.next();
            } catch (RuntimeException | Error ex) {
                close();
                throw ex;
            } catch (final Throwable ex) {
                close();
                throw new RuntimeException(ex);
            }
        }

        @Override
        public void close() {
            if (this.iteration instanceof CloseableIteration<?, ?>) {
                try {
                    ((CloseableIteration<? extends T, ?>) this.iteration).close();
                } catch (final Throwable ex) {
                    LOGGER.error("Could not close iteration", ex);
                }
            }
        }

    }

    private static final class IteratorIteration<T, E extends Exception> implements
            CloseableIteration<T, E> {

        private final Iterator<T> iterator;

        IteratorIteration(final Iterator<T> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() throws E {
            return this.iterator.hasNext();
        }

        @Override
        public T next() throws E {
            return this.iterator.next();
        }

        @Override
        public void remove() throws E {
            this.iterator.remove();
        }

        @Override
        public void close() throws E {
            IO.closeQuietly(this.iterator);
        }

    }

}
