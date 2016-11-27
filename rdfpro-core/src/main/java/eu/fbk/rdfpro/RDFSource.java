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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Environment;

/**
 * A source of RDF data.
 * <p>
 * An {@code RDFSource} provides repeatable access to RDF data consisting of RDF statements but
 * also namespace declarations and comments. Source data is assumed immutable and can be accessed
 * multiple times in different ways:
 * </p>
 * <ul>
 * <li>calling {@link #emit(RDFHandler, int)} that allows full access to all the kinds of data
 * items (statements, namespaces, comments) and allow for optimizing multiple passes over source
 * data;</li>
 * <li>calling methods of the {@link Iterable} interface, i.e., {@link #iterator()} and
 * {@link #forEach(Consumer)}; they allow access only to statements and may not be as efficient as
 * {@code emit()}.</li>
 * <li>obtaining a {@link Stream} using methods {@link #stream()} and {@link #parallelStream()},
 * as can be done on Java {@code Collection}s.</li>
 * </ul>
 * <p>
 * In order to implement this interface it is enough to implement method
 * {@link #emit(RDFHandler, int)}, as default implementations are provided for the remaining
 * methods. However, consider overriding also methods {@link #spliterator()} (and possibly
 * {@link #iterator()}) in case you have access to a better {@code Splititerator} implementation,
 * as the supplied one requires to launch a background thread performing
 * {@link #emit(RDFHandler, int)} (this is not necessarily bad, as it decouples data extraction
 * from data consumption); in general it is not necessary to override other methods.
 * </p>
 */
public interface RDFSource extends Iterable<Statement> {

    /**
     * Emits the RDF data of the source to the {@code RDFHandler} supplied, performing the
     * requested number of passes. For each pass, the {@code handler} method {@code startRDF()} is
     * called, followed by possibly concurrent invocations to {@code handleXXX()} methods and
     * finally an invocation to {@code endRDF()}. Any exception causes this process to be
     * interrupted. In any case, both after the successful completion of the requested passes or
     * after a failure, method {@link AutoCloseable#close()} is called on the {@code handler} in
     * case it implements interface {@link AutoCloseable} (or a sub-interface), thus giving it the
     * possibility to release allocated resources.
     *
     * @param handler
     *            the handler where to emit RDF data (statements, comments, namespaces)
     * @param passes
     *            the number of passes to perform on source data
     * @throws RDFSourceException
     *             in case of specific errors in retrieving data from this source
     * @throws RDFHandlerException
     *             in case of specific errors in the invoked {@link RDFHandler}
     */
    void emit(RDFHandler handler, int passes) throws RDFSourceException, RDFHandlerException;

    /**
     * {@inheritDoc} This default implementation delegates to {@link #spliterator()}. This
     * baseline implementation is as good as the underlying {@code Spliterator} (wrapping is not
     * expensive); it should be overridden if a more efficient can be produced.
     */
    @Override
    default Iterator<Statement> iterator() {
        return Spliterators.iterator(spliterator());
    }

    /**
     * {@inheritDoc} This default implementation either delegates to {@link #forEach(Consumer)},
     * in case {@link Spliterator#forEachRemaining(Consumer)} is immediately called (more
     * efficient), or run {@link #emit(RDFHandler, int)} in a separate thread, populating a queue
     * from where the returned {@code Spliterator} retrieves its elements. This baseline
     * implementation should be overridden whenever possible.
     */
    @Override
    default Spliterator<Statement> spliterator() {
        return new Spliterator<Statement>() {

            private BlockingQueue<Object> queue;

            private boolean done;

            @Override
            public long estimateSize() {
                return Long.MAX_VALUE; // size unknown
            }

            @Override
            public int characteristics() {
                return Spliterator.NONNULL | Spliterator.IMMUTABLE; // no other guarantees
            }

            @Override
            public void forEachRemaining(final Consumer<? super Statement> action) {
                Objects.requireNonNull(action);
                if (this.queue != null) {
                    while (tryAdvance(action)) {
                    }
                } else if (!this.done) {
                    forEach(action);
                    this.done = true;
                }
            }

            @Override
            public boolean tryAdvance(final Consumer<? super Statement> action) {
                if (this.queue != null || triggerEmit()) {
                    try {
                        final Object object = this.queue.take();
                        if (object instanceof Statement) {
                            action.accept((Statement) object);
                            return true;
                        } else if (object instanceof Throwable) {
                            throw (Throwable) object;
                        } else {
                            this.queue = null;
                            this.done = true;
                        }
                    } catch (final Throwable ex) {
                        this.queue = null; // will ultimately kill the emit thread
                        this.done = true;
                        if (ex instanceof RuntimeException) {
                            throw (RuntimeException) ex;
                        } else if (ex instanceof Error) {
                            throw (Error) ex;
                        }
                        throw new RuntimeException(ex);
                    }
                }
                return false;
            }

            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public Spliterator<Statement> trySplit() {
                if (this.queue != null || triggerEmit()) {
                    final List<Object> list = new ArrayList<Object>(1024);
                    this.queue.drainTo(list);
                    final int last = list.size() - 1;
                    if (last >= 0) {
                        if (!(list.get(last) instanceof Statement)) {
                            this.queue.offer(list.remove(last));
                        }
                        return (Spliterator) list.spliterator();
                    }
                }
                return null;
            }

            private boolean triggerEmit() {
                if (this.done) {
                    return false;
                }
                this.queue = new ArrayBlockingQueue<Object>(1024);
                Environment.getPool().execute(new Runnable() {

                    @Override
                    public void run() {
                        doEmit();
                    }

                });
                return true;
            }

            private void doEmit() {
                try {
                    emit(new AbstractRDFHandler() {

                        @Override
                        public void handleStatement(final Statement statement)
                                throws RDFHandlerException {
                            try {
                                queue.put(statement);
                            } catch (final InterruptedException ex) {
                                throw new RDFHandlerException("Interrupted", ex);
                            }
                        }

                    }, 1);
                    this.queue.put(this.queue); // queue object used as EOF marker

                } catch (final Throwable ex) {
                    try {
                        this.queue.put(ex);
                    } catch (final Throwable ex2) {
                        this.queue = null; // last resort to break iteration
                    }
                }
            }

        };
    }

    /**
     * {@inheritDoc} This default implementation delegates to {@link #emit(RDFHandler, int)},
     * enforcing a sequential invocation (but possibly by different threads) of the supplied
     * {@code action}. Iteration order is unspecified and may differ across successive invocations
     * of this method. There is usually no need to override this method.
     *
     * @throws RDFSourceException
     *             in case of errors in retrieving data from this source
     */
    @Override
    default void forEach(final Consumer<? super Statement> action) throws RDFSourceException {
        try {
            emit(new AbstractRDFHandler() {

                @Override
                public synchronized void handleStatement(final Statement statement)
                        throws RDFHandlerException {
                    action.accept(statement);
                }

            }, 1);
        } catch (final RDFHandlerException ex) {
            // Not thrown by Consumer -> RDFHandler adapter
            throw new Error("Unexpected exception (!)", ex);
        }
    }

    /**
     * Returns a sequential Java 8 {@code Stream} over the statements of this {@code RDFSource}.
     * This method is meant for integration with the Java 8 Stream API. Its default implementation
     * builds on top of the {@link Spliterator} returned by {@link #spliterator()}. There is
     * usually no need to override this method.
     *
     * @return a sequential {@code Stream} over the statements of this {@code RDFSource}
     */
    default Stream<Statement> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /**
     * Returns a parallel Java 8 {@code Stream} over the statements of this {@code RDFSource}.
     * This method is meant for integration with the Java 8 Stream API. Its default implementation
     * builds on top of the {@link Spliterator} returned by {@link #spliterator()}. There is
     * usually no need to override this method.
     *
     * @return a parallel {@code Stream} over the statements of this {@code RDFSource}
     */
    default Stream<Statement> parallelStream() {
        return StreamSupport.stream(spliterator(), true);
    }

}
