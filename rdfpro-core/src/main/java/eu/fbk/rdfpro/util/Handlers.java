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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerWrapper;

/**
 * Utility methods dealing with {@code RDFHandler}s.
 */
public final class Handlers {

    private static final HandlerBase NOP_HANDLER = new HandlerBase();

    private Handlers() {
    }

    /**
     * Returns a no-operation {@code RDFHandler} that does nothing.
     *
     * @return a no-operation handler (singleton instance)
     */
    public static RDFHandler nop() {
        return NOP_HANDLER;
    }

    /**
     * Returns an {@code RDFHandler} that populates the supplied collection with the received
     * statements. Access to the collection is externally synchronized, so the collection does not
     * need to be thread-safe.
     *
     * @param collection
     *            the collection to populate
     * @param dropContext
     *            true if contexts should be dropped (so just triples are stored)
     * @return the created {@code RDFHandler}
     */
    public static RDFHandler collect(final Collection<? super Statement> collection,
            final boolean dropContext) {
        Util.checkNotNull(collection);
        return new HandlerBase() {

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                Statement t = statement;
                if (dropContext && statement.getContext() != null) {
                    t = Statements.VALUE_FACTORY.createStatement(statement.getSubject(),
                            statement.getPredicate(), statement.getObject());
                }
                synchronized (collection) {
                    collection.add(t);
                }
            }

        };
    }

    /**
     * Returns an {@code RDFHandler wrapper} that tracks the number of statements received so far
     * using a {@code Tracker} object.
     *
     * @param handler
     *            the handler to wrap
     * @param tracker
     *            the tracker object
     * @return an {@code RDFHandler wrapper} that intercepts and tracks statements as they are
     *         sent to the wrapped handler
     */
    public static RDFHandler track(final RDFHandler handler, final Tracker tracker) {
        Util.checkNotNull(tracker);
        return new HandlerWrapper(handler) {

            @Override
            public void startRDF() throws RDFHandlerException {
                super.startRDF();
                tracker.start();
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                tracker.increment();
                super.handleStatement(statement);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                tracker.end();
                super.endRDF();
            }
        };
    }

    /**
     * Wraps the supplied {@code RDFHandler} (if necessary) so to buffer incoming statements and
     * use additional threads for their processing. An {@code RDFHandler} that has already been
     * decoupled is not wrapped again.
     *
     * @param handler
     *            the handler to wrap
     * @return the (possibly) wrapped handler
     */
    public static RDFHandler decouple(final RDFHandler handler) {
        if (handler == NOP_HANDLER || handler instanceof DecoupleHandler) {
            return handler;
        }
        return new DecoupleHandler(handler);
    }

    /**
     * Returns an {@code RDFHandler} that dispatches incoming calls to all the supplied
     * {@code RDFHandler}s. If no {@code RDFHandler} is supplied, {@link #nop()} is returned.
     *
     * @param handlers
     *            the handlers to dispatch to
     * @return a dispatching {@code RDFHandler}
     */
    public static RDFHandler dispatchAll(final RDFHandler... handlers) {
        final List<RDFHandler> nonNopHandlers = new ArrayList<RDFHandler>();
        for (final RDFHandler handler : handlers) {
            if (Util.checkNotNull(handler) != nop()) {
                nonNopHandlers.add(handler);
            }
        }
        if (nonNopHandlers.isEmpty()) {
            return nop();
        }
        if (nonNopHandlers.size() == 1) {
            return nonNopHandlers.get(0);
        }
        if (nonNopHandlers.size() == 2) { // frequent case specifically optimized for
            return new HandlerBase() {

                private final RDFHandler first = nonNopHandlers.get(0);

                private final RDFHandler second = nonNopHandlers.get(1);

                @Override
                public void startRDF() throws RDFHandlerException {
                    this.first.startRDF();
                    this.second.startRDF();
                }

                @Override
                public void handleComment(final String comment) throws RDFHandlerException {
                    this.first.handleComment(comment);
                    this.second.handleComment(comment);
                }

                @Override
                public void handleNamespace(final String prefix, final String uri)
                        throws RDFHandlerException {
                    this.first.handleNamespace(prefix, uri);
                    this.second.handleNamespace(prefix, uri);
                }

                @Override
                public void handleStatement(final Statement statement) throws RDFHandlerException {
                    this.first.handleStatement(statement);
                    this.second.handleStatement(statement);
                }

                @Override
                public void endRDF() throws RDFHandlerException {
                    this.first.endRDF();
                    this.second.endRDF();
                }

                @Override
                public void close() {
                    Util.closeQuietly(this.first);
                    Util.closeQuietly(this.second);
                }

            };
        }
        return new HandlerBase() {

            private final RDFHandler[] handlers = nonNopHandlers
                    .toArray(new RDFHandler[nonNopHandlers.size()]);

            @Override
            public void startRDF() throws RDFHandlerException {
                for (final RDFHandler handler : this.handlers) {
                    handler.startRDF();
                }
            }

            @Override
            public void handleComment(final String comment) throws RDFHandlerException {
                for (final RDFHandler handler : this.handlers) {
                    handler.handleComment(comment);
                }
            }

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException {
                for (final RDFHandler handler : this.handlers) {
                    handler.handleNamespace(prefix, uri);
                }
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                for (final RDFHandler handler : this.handlers) {
                    handler.handleStatement(statement);
                }
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                for (final RDFHandler handler : this.handlers) {
                    handler.endRDF();
                }
            }

            @Override
            public void close() {
                for (final RDFHandler handler : this.handlers) {
                    Util.closeQuietly(handler);
                }
            }

        };
    }

    /**
     *
     * Returns an {@code RDFHandler} that dispatches incoming calls to the supplied
     * {@code RDFHandler}s in a round robin fashion. Round robin dispatching concerns only
     * statements and comments, while namespaces and start and end notifications are forwarded to
     * all the handlers. If no {@code RDFHandler} is supplied, {@link #nop()} is returned.
     *
     * @param handlers
     *            the handlers to dispatch to
     * @return a dispatching {@code RDFHandler}
     */
    public static RDFHandler dispatchRoundRobin(final RDFHandler... handlers) {
        Util.checkNotNull(handlers);
        if (handlers.length == 0) {
            return nop();
        }
        if (handlers.length == 1) {
            return handlers[0];
        }
        return new HandlerBase() {

            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public void startRDF() throws RDFHandlerException {
                for (final RDFHandler handler : handlers) {
                    handler.startRDF();
                }
            }

            @Override
            public void handleComment(final String comment) throws RDFHandlerException {
                pickHandler().handleComment(comment);
            }

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException {
                for (final RDFHandler handler : handlers) {
                    handler.handleNamespace(prefix, uri);
                }
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                pickHandler().handleStatement(statement);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                Throwable exception = null;
                for (final RDFHandler handler : handlers) {
                    try {
                        handler.endRDF();
                    } catch (final Throwable ex) {
                        if (exception == null) {
                            exception = ex;
                        } else {
                            exception.addSuppressed(ex);
                        }
                    }
                }
                if (exception != null) {
                    Util.propagateIfPossible(exception, RDFHandlerException.class);
                    throw new RDFHandlerException(exception);
                }
            }

            @Override
            public void close() {
                for (final RDFHandler handler : handlers) {
                    Util.closeQuietly(handler);
                }
            }

            private RDFHandler pickHandler() {
                return handlers[this.counter.getAndIncrement() % handlers.length];
            }

        };
    }

    /**
     * Wraps the supplied {@code RDFHandler} intercepting and discarding calls to
     * {@code startRDF()} and {@code endRDF()} methods.
     *
     * @param handler
     *            the handler to wrap
     * @return the wrapped handler
     */
    public static RDFHandler ignoreStartEnd(final RDFHandler handler) {
        if (handler == NOP_HANDLER) {
            return handler;
        }
        return new HandlerWrapper(handler) {

            @Override
            public void startRDF() throws RDFHandlerException {
            }

            @Override
            public void endRDF() throws RDFHandlerException {
            }

        };
    }

    /**
     * Wraps the supplied {@code RDFHandler} discarding any calls following {@code endRDF()}.
     * Calls to {@code close()} are always propagated, however.
     *
     * @param handler
     *            the handler to wrap
     * @return the wrapped handler
     */
    public static RDFHandler ignoreExtraPasses(final RDFHandler handler) {
        if (handler == NOP_HANDLER) {
            return handler;
        }
        return new HandlerWrapper(handler) {

            private RDFHandler passHandler = this.handler;

            @Override
            public void startRDF() throws RDFHandlerException {
                this.passHandler.startRDF();
            }

            @Override
            public void handleComment(final String comment) throws RDFHandlerException {
                this.passHandler.handleComment(comment);
            }

            @Override
            public void handleNamespace(final String prefix, final String uri)
                    throws RDFHandlerException {
                this.passHandler.handleNamespace(prefix, uri);
            }

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {
                this.passHandler.handleStatement(statement);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                try {
                    this.passHandler.endRDF();
                } finally {
                    this.passHandler = NOP_HANDLER;
                }
            }

        };
    }

    /**
     * Wraps the supplied {@code RDFHandler} adding the specified suffix to BNodes in incoming
     * statements. This method can be used to prevent BNode clashes when multiple sources are
     * read.
     *
     * @param handler
     *            the handler to wrap
     * @param suffix
     *            the suffix to add to encountered BNodes
     * @return the wrapped handler
     */
    public static RDFHandler rewriteBNodes(final RDFHandler handler, final String suffix) {
        Util.checkNotNull(suffix);
        if (handler == NOP_HANDLER) {
            return handler;
        }
        return new HandlerWrapper(handler) {

            @Override
            public void handleStatement(final Statement statement) throws RDFHandlerException {

                if (statement.getSubject() instanceof BNode
                        || statement.getObject() instanceof BNode
                        || statement.getContext() instanceof BNode) {

                    final Resource s = statement.getSubject();
                    final URI p = statement.getPredicate();
                    final Value o = statement.getObject();
                    final Resource c = statement.getContext();

                    final ValueFactory vf = Statements.VALUE_FACTORY;

                    final Resource sr = s instanceof BNode ? vf.createBNode(((BNode) s).getID()
                            + suffix) : s;
                    final Value or = o instanceof BNode ? vf.createBNode(((BNode) o).getID()
                            + suffix) : o;
                    final Resource cr = c instanceof BNode ? vf.createBNode(((BNode) c).getID()
                            + suffix) : c;

                    if (cr == null) {
                        super.handleStatement(vf.createStatement(sr, p, or));
                    } else {
                        super.handleStatement(vf.createStatement(sr, p, or));
                    }

                } else {
                    super.handleStatement(statement);
                }
            }

        };
    }

    /**
     * Base implementation of {@code RDFHandler} and {@code Closeable} providing empty
     * implementation of all their methods.
     *
     * <p>
     * The implementation of {@code Closeable} allows for this class and its subclasses to be
     * notified by RDFpro runtime when they are no more needed, so that allocated resources can be
     * released if necessary. For this reason, it may be convenient to start from this class when
     * implementing your specialized {@code RDFHandler}.
     * </p>
     */
    public static class HandlerBase implements RDFHandler, Closeable {

        /**
         * Does nothing.
         */
        @Override
        public void startRDF() throws RDFHandlerException {
        }

        /**
         * Does nothing.
         */
        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
        }

        /**
         * Does nothing.
         */
        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
        }

        /**
         * Does nothing.
         */
        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
        }

        /**
         * Does nothing.
         */
        @Override
        public void endRDF() throws RDFHandlerException {
        }

        /**
         * Does nothing.
         */
        @Override
        public void close() {
        }

    }

    /**
     * Base implementation of an {@code RDFHandler} + {@code Closeable} wrapper, supporting also
     * the delegation of method {@code close()}.
     *
     * <p>
     * This class wraps a {@code RDFHandler} and delegates all the methods to that instance. If
     * the wrapped {@code RDFHandler} is {@code Closeable}, then also calls to
     * {@link Closeable#close()} are delegated, with errors logged but ignored. Differently from
     * Sesame {@link RDFHandlerWrapper}, this class wraps a unique {@code RDFHandler} and thus
     * does not need array traversal (and its overhead) to notify a pool of {@code RDFHandlers}.
     * </p>
     */
    public static class HandlerWrapper extends HandlerBase {

        /** The wrapped {@code RDFHandler}. */
        protected final RDFHandler handler;

        /**
         * Creates a new instance wrapping the supplied {@code RDFHandler}.
         *
         * @param handler
         *            the {@code RDFHandler} to wrap
         */
        protected HandlerWrapper(final RDFHandler handler) {
            this.handler = Util.checkNotNull(handler);
        }

        /**
         * Delegates to the wrapped {@code RDFHandler}.
         */
        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
        }

        /**
         * Delegates to the wrapped {@code RDFHandler}.
         */
        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        /**
         * Delegates to the wrapped {@code RDFHandler}.
         */
        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        /**
         * Delegates to the wrapped {@code RDFHandler}.
         */
        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.handler.handleStatement(statement);
        }

        /**
         * Delegates to the wrapped {@code RDFHandler}.
         */
        @Override
        public void endRDF() throws RDFHandlerException {
            this.handler.endRDF();
        }

        /**
         * Delegates to the wrapped {@code RDFHandler}, if it implements {@code Closeable}.
         */
        @Override
        public void close() {
            Util.closeQuietly(this.handler);
        }

    }

    private static final class DecoupleHandler extends HandlerWrapper {

        private static final int BUFFER_SIZE = 4 * 1024;

        private final Set<Thread> incomingThreads;

        private final List<Future<?>> futures;

        private final AtomicInteger index;

        private final AtomicInteger size;

        private Throwable exception;

        private Statement[] buffer;

        private int fraction;

        DecoupleHandler(final RDFHandler handler) {
            super(handler);
            this.incomingThreads = new HashSet<Thread>(); // equals/hashCode based on identity
            this.futures = new ArrayList<Future<?>>();
            this.index = new AtomicInteger(0);
            this.size = new AtomicInteger(0);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.incomingThreads.clear();
            this.futures.clear();
            this.exception = null;
            this.buffer = new Statement[BUFFER_SIZE];
            this.index.set(0);
            this.size.set(0);
            this.fraction = 1;
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            final int i = this.index.getAndIncrement();
            if (i % this.fraction == 0) {
                final int index = i / this.fraction;
                if (index < BUFFER_SIZE) {
                    this.buffer[index] = statement;
                    final int size = this.size.incrementAndGet();
                    if (size == BUFFER_SIZE) {
                        this.incomingThreads.add(Thread.currentThread());
                        checkNotFailed();
                        schedule();
                    }
                    return;
                }
            }
            super.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            process(true);
            List<Future<?>> futuresToWaitFor;
            synchronized (this.futures) {
                futuresToWaitFor = new ArrayList<Future<?>>(this.futures);
            }
            for (final Future<?> future : futuresToWaitFor) {
                while (!future.isDone()) {
                    try {
                        future.get();
                    } catch (final Throwable ex) {
                        // Ignore
                    }
                }
            }
            synchronized (this) {
                checkNotFailed();
            }
            super.endRDF();
        }

        @Override
        public void close() {
            super.close();
            synchronized (this.futures) {
                for (final Future<?> future : this.futures) {
                    future.cancel(false);
                }
            }
        }

        private void schedule() throws RDFHandlerException {
            synchronized (this.futures) {
                for (final Iterator<Future<?>> i = this.futures.iterator(); i.hasNext();) {
                    final Future<?> future = i.next();
                    if (future.isDone()) {
                        i.remove();
                    }
                }
                this.futures.add(Util.getPool().submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            process(false);
                        } catch (final Throwable ex) {
                            @SuppressWarnings("resource")
                            final DecoupleHandler h = DecoupleHandler.this;
                            synchronized (h) {
                                if (h.exception == null) {
                                    h.exception = ex;
                                } else {
                                    h.exception.addSuppressed(ex);
                                }
                                synchronized (h.futures) {
                                    for (final Future<?> future : h.futures) {
                                        future.cancel(false);
                                    }
                                }
                            }
                        }
                    }

                }));
            }
        }

        private void process(final boolean force) throws RDFHandlerException {
            Statement[] statements;
            int count;
            synchronized (this) {
                statements = this.buffer;
                count = this.size.get();
                if (count == 0 || count < BUFFER_SIZE && !force) {
                    return;
                }
                this.buffer = new Statement[BUFFER_SIZE];
                this.size.set(0);
            }

            // Here we adapt the fraction of incoming statements that are buffered, based on the
            // numbers of threads we detected entered the decoupler. The fraction is chosen so to
            // divide the work evenly between N incoming threads and #CORES - N background
            // threads. In practice, the fraction moves quickly to 1000 (multiple threads in)
            // or to 1 (single thread in).
            final int numThreads = this.incomingThreads.size();
            final int newFraction = numThreads >= Util.CORES ? 1000 : Util.CORES
                    / (Util.CORES - numThreads);
            this.index.set(BUFFER_SIZE * Math.max(this.fraction, newFraction));
            synchronized (this) {
                this.fraction = newFraction;
            }
            this.index.set(0);

            for (int i = 0; i < count; ++i) {
                super.handleStatement(statements[i]);
            }
        }

        private void checkNotFailed() throws RDFHandlerException {
            if (this.exception != null) {
                Util.propagateIfPossible(this.exception, RDFHandlerException.class);
                throw new RDFHandlerException(this.exception);
            }
        }

    }

}
