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
package eu.fbk.rdfpro;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Environment;

// assumptions
// - lifecycle: start, followed by handleXXX in parallel, followed by end
// - on error, further invocation to API method will result in exceptions
// - no specific way to interrupt computation (CTRL-C from outside)

/**
 * A generic RDF stream transformer.
 * <p>
 * An {@code RDFProcessor} is a reusable Java component that consumes an input stream of RDF
 * {@link Statement}s in one or more passes, produces an output stream of statements and may have
 * side effect like writing RDF data.
 * </p>
 * <p>
 * An {@code RDFProcessor} can be used by means of a number of methods:
 * </p>
 * <ul>
 * <li>{@link #apply(RDFSource, RDFHandler, int)} and
 * {@link #applyAsync(RDFSource, RDFHandler, int)} apply the {@code RDFProcessor} to data from a
 * given {@code RDFSource}, emitting the results to a supplied {@code RDFHandler} in one or more
 * passes;</li>
 * <li>{@link #wrap(RDFSource)} wraps an {@code RDFSource}, returning a new {@code RDFSource} that
 * post-process returned data with the {@code RDFProcessor};</li>
 * <li>{@link #wrap(RDFHandler)} wraps an {@code RDFHandler}, returning a new {@code RDFHandler}
 * that pre-process input data with the {@code RDFProcessor}.</li>
 * </ul>
 * <p>
 * The transformation encapsulated by an {@code RDFProcessor} may require multiple passes on input
 * data before results can be emitted. To this end, method {@link #getExtraPasses()} declares how
 * many extra passes a {@code RDFProcessor} needs on its input with method before the result is
 * produced in successive passes.
 * </p>
 * <p>
 * Implementers of this interface should provide an implementation of {@link #wrap(RDFHandler)}
 * and, optionally, of {@link #getExtraPasses()} in case their {@code RDFProcessor} requires extra
 * passes. Other methods have reasonable default implementations that do not need (in principle)
 * to be overridden.
 * </p>
 * <p>
 * Implementations of this interface should be thread-safe, as it is allowed for its methods to be
 * called concurrently by multiple threads (still, no particular contention is expected so basic
 * synchronization is enough).
 * </p>
 */
public interface RDFProcessor {

    /**
     * Returns the number of extra passes required by the processor before starting emitting
     * output data. This default implementation returns 0 (the common case).
     *
     * @return then number of extra passes, not negative
     */
    default int getExtraPasses() {
        return 0; // may be overridden
    }

    /**
     * Wraps the supplied {@code RDFSource} so to post-process data of the source with this
     * {@code RDFProcessor} before returning it. Note that this method does not perform any real
     * work, apart creating a wrapper. This default implementation delegates to
     * {@link #wrap(RDFHandler)}.
     *
     * @param source
     *            the {@code RDFSource} to wrap, not null
     * @return the wrapped {@code RDFSource}
     */
    default RDFSource wrap(final RDFSource source) {

        Objects.requireNonNull(source);

        return new RDFSource() {

            @Override
            public void emit(final RDFHandler handler, final int passes)
                    throws RDFSourceException, RDFHandlerException {
                Objects.requireNonNull(handler);
                final int totalPasses = passes + getExtraPasses();
                if (passes > 0 && totalPasses > 0) {
                    final RDFHandler wrappedHandler = wrap(handler);
                    source.emit(wrappedHandler, totalPasses);
                }
            }

        };
    }

    /**
     * Wraps the supplied {@code RDFHandler} so to pre-process data fed to it with this
     * {@code RDFProcessor}. Note that this method does not perform any real work, apart creating
     * a wrapper.
     *
     * @param handler
     *            the {@code RDFHandler} to wrap, not null
     * @return the wrapped {@code RDFHandler}
     */
    RDFHandler wrap(RDFHandler handler);

    /**
     * Applies the processor to the supplied {@code RDFSource}, emitting output data to the
     * specified {@code RDFHandler} in one or more passes. This default implementation is based on
     * {@link #wrap(RDFHandler)}.
     *
     * @param input
     *            the input {@code RDFSource}, not null
     * @param output
     *            the output {@code RDFHandler}, not null
     * @param passes
     *            the requested number of passes to be fed to the supplied {@code RDFHandler} (if
     *            zero nothing will be done)
     * @throws RDFSourceException
     *             on error in retrieving data from the source
     * @throws RDFHandlerException
     *             on error in the supplied {@code RDFHandler} or inside the {@code RDFProcessor}
     */
    default void apply(final RDFSource input, final RDFHandler output, final int passes)
            throws RDFSourceException, RDFHandlerException {
        if (passes > 0) {
            input.emit(wrap(output), passes + getExtraPasses());
        } else if (passes < 0) {
            throw new IllegalArgumentException("Invalid number of passes " + passes);
        } else {
            Objects.requireNonNull(input);
            Objects.requireNonNull(passes);
        }
    }

    /**
     * Asynchronously applies the processor to the supplied {@code RDFSource}, emitting output
     * data to the specified {@code RDFHandler} in one or more passes. This method operates
     * similarly to {@link #apply(RDFSource, RDFHandler, int)}, but immediately returns providing
     * a {@link CompletableFuture} that can be used to track the result of the computation. This
     * default implementation is based on {@link #apply(RDFSource, RDFHandler, int)} (and thus
     * indirectly on {@link #wrap(RDFHandler)}) and makes use of the common thread pool provided
     * by {@link Environment#getPool()}.
     *
     * @param input
     *            the input {@code RDFSource}, not null
     * @param output
     *            the output {@code RDFHandler}, not null
     * @param passes
     *            the requested number of passes to be fed to the supplied {@code RDFHandler} (if
     *            zero nothing will be done)
     * @return a {@code CompletableFuture} that can be used for querying the state of the
     *         asynchronous computation and possibly execute a callback when it ends (note that
     *         cancellation is not supported).
     */
    default CompletableFuture<Void> applyAsync(final RDFSource input, final RDFHandler output,
            final int passes) {
        final CompletableFuture<Void> future = new CompletableFuture<Void>();
        Environment.getPool().execute(new Runnable() {

            @Override
            public void run() {
                if (!future.isDone()) {
                    try {
                        apply(input, output, passes);
                        future.complete(null);
                    } catch (final Throwable ex) {
                        future.completeExceptionally(ex);
                    }
                }
            }

        });
        return future;
    }

}
