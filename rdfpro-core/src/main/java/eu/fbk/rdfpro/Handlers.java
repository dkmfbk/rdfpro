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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;

final class Handlers {

    private static final NopHandler NOP_INSTANCE = new NopHandler();

    private Handlers() {
    }

    public static RDFHandler nop() {
        return NOP_INSTANCE;
    }

    public static RDFHandler track(final RDFHandler handler, final Logger logger,
            @Nullable final String startMessage, @Nullable final String endMessage,
            @Nullable final String statusKey, @Nullable final String statusMessage) {
        return new TrackHandler(handler, logger, startMessage, endMessage, statusKey,
                statusMessage);
    }

    public static RDFHandler decouple(final RDFHandler handler) {
        if (handler == NOP_INSTANCE || handler instanceof DecoupleHandler) {
            return handler;
        } else {
            return new DecoupleHandler(handler);
        }
    }

    public static RDFHandler duplicate(final RDFHandler first, final RDFHandler second) {
        if (first == NOP_INSTANCE) {
            return second;
        } else if (second == NOP_INSTANCE) {
            return first;
        } else {
            return new DuplicateHandler(first, second);
        }
    }

    public static RDFHandler collect(final Collection<? super Statement> collection,
            final boolean dropContext) {
        return new CollectorHandler(collection, dropContext);
    }

    public static RDFHandler roundRobin(final Iterable<? extends RDFHandler> handlers) {
        final Iterator<? extends RDFHandler> i = handlers.iterator();
        if (!i.hasNext()) {
            return NOP_INSTANCE;
        }
        final RDFHandler handler = i.next();
        if (!i.hasNext()) {
            return handler;
        } else {
            return new RoundRobinHandler(handlers);
        }
    }

    public static RDFHandler dropStartEnd(final RDFHandler handler) {
        if (handler == NOP_INSTANCE || handler instanceof DropStartEndHandler) {
            return handler;
        } else {
            return new DropStartEndHandler(handler);
        }
    }

    public static RDFHandler dropExtraPasses(final RDFHandler handler) {
        if (handler == NOP_INSTANCE || handler instanceof DropExtraPassesHandler) {
            return handler;
        } else {
            return new DropExtraPassesHandler(handler);
        }
    }

    public static RDFHandler rewriteBNodes(final RDFHandler handler, final String suffix) {
        if (handler == NOP_INSTANCE) {
            return handler;
        } else {
            return new RewriteBNodesHandler(handler, suffix);
        }
    }

    // TODO: check at the end if we still need AbstractHandler and DelegatingHandler

    public static abstract class AbstractHandler implements RDFHandler, Closeable {

        @Override
        public void startRDF() throws RDFHandlerException {
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
        }

        @Override
        public void endRDF() throws RDFHandlerException {
        }

        @Override
        public void close() {
        }

    }

    public static abstract class DelegatingHandler implements RDFHandler, Closeable {

        final RDFHandler delegate;

        DelegatingHandler(final RDFHandler delegate) {
            this.delegate = Util.checkNotNull(delegate);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.delegate.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.delegate.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.delegate.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.delegate.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.delegate.endRDF();
        }

        @Override
        public void close() {
            Util.closeQuietly(this.delegate);
        }

    }

    private static final class NopHandler implements RDFHandler, Closeable {

        @Override
        public void startRDF() throws RDFHandlerException {
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
        }

        @Override
        public void endRDF() throws RDFHandlerException {
        }

        @Override
        public void close() throws IOException {
        }

    }

    private static final class TrackHandler implements RDFHandler, Closeable {

        private final Logger logger;

        @Nullable
        private final String startMessage;

        @Nullable
        private final String endMessage;

        @Nullable
        private final String statusKey;

        @Nullable
        private final String statusMessage;

        private final AtomicLong counter;

        private long counterAtTs = 0;

        private long ts0;

        private long ts1;

        private long ts;

        private long chunkSize;

        private final RDFHandler handler;

        public TrackHandler(final RDFHandler handler, @Nullable final Logger logger,
                @Nullable final String startMessage, @Nullable final String endMessage,
                @Nullable final String statusKey, @Nullable final String statusMessage) {
            this.handler = Util.checkNotNull(handler);
            this.logger = Util.checkNotNull(logger);
            this.startMessage = startMessage;
            this.endMessage = endMessage;
            this.statusKey = statusKey;
            this.statusMessage = statusMessage;
            this.counter = new AtomicLong(0L);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
            this.counter.set(0L);
            this.counterAtTs = 0;
            this.ts0 = 0;
            this.ts1 = 0;
            this.ts = 0;
            this.chunkSize = 1L;
            if (this.startMessage != null) {
                this.logger.info(this.startMessage);
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            long counter = this.counter.getAndIncrement();
            if (counter % this.chunkSize == 0 && this.statusMessage != null) {
                synchronized (this) {
                    ++counter;
                    final long ts = System.currentTimeMillis();
                    this.ts1 = ts;
                    if (counter == 1) {
                        this.ts0 = ts;
                        this.ts = ts;
                    }
                    final long delta = ts - this.ts0;
                    if (delta > 0) {
                        final long avgThroughput = counter * 1000 / delta;
                        this.chunkSize = avgThroughput < 10 ? 1
                                : avgThroughput < 10000 ? avgThroughput / 10 : 1000;
                        if (ts / 1000 - this.ts / 1000 >= 1) {
                            final long throughput = (counter - this.counterAtTs) * 1000
                                    / (ts - this.ts);
                            this.ts = ts;
                            this.counterAtTs = counter;
                            Util.registerStatus(this.statusKey, String.format(this.statusMessage,
                                    counter - 1, throughput, avgThroughput));
                        }
                    }
                }
            }
            this.handler.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (this.statusMessage != null) {
                Util.registerStatus(this.statusKey, null);
            }
            if (this.endMessage != null) {
                final long ts = this.ts1;
                final long avgThroughput = this.counter.get() * 1000 / (ts - this.ts0 + 1);
                if (this.logger.isInfoEnabled()) {
                    this.logger.info(String.format(this.endMessage, this.counter.get(),
                            avgThroughput));
                }
            }
            this.handler.endRDF();
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
        }

    }

    private static final class DecoupleHandler implements RDFHandler, Closeable {

        private static final int BUFFER_SIZE = 4 * 1024;

        private final RDFHandler handler;

        private final Set<Thread> incomingThreads;

        private final List<Future<?>> futures;

        private final AtomicInteger index;

        private final AtomicInteger size;

        private Throwable exception;

        private Statement[] buffer;

        private int fraction;

        DecoupleHandler(final RDFHandler handler) {
            this.handler = Util.checkNotNull(handler);
            this.incomingThreads = new HashSet<Thread>(); // equals/hashCode based on identity
            this.futures = new ArrayList<Future<?>>();
            this.index = new AtomicInteger(0);
            this.size = new AtomicInteger(0);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
            this.incomingThreads.clear();
            this.futures.clear();
            this.exception = null;
            this.buffer = new Statement[BUFFER_SIZE];
            this.index.set(0);
            this.size.set(0);
            this.fraction = 1;
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
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
            this.handler.handleStatement(statement);
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
            this.handler.endRDF();
        }

        @Override
        public void close() {
            Util.closeQuietly(this.handler);
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
                this.futures.add(Threads.getBackgroundPool().submit(new Runnable() {

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
            final int newFraction = numThreads >= Threads.CORES ? 1000 : Threads.CORES
                    / (Threads.CORES - numThreads);
            this.index.set(BUFFER_SIZE * Math.max(this.fraction, newFraction));
            synchronized (this) {
                this.fraction = newFraction;
            }
            this.index.set(0);

            for (int i = 0; i < count; ++i) {
                this.handler.handleStatement(statements[i]);
            }
        }

        private void checkNotFailed() throws RDFHandlerException {
            if (this.exception != null) {
                Util.propagateIfPossible(this.exception, RDFHandlerException.class);
                throw new RDFHandlerException(this.exception);
            }
        }

    }

    private static final class DuplicateHandler implements RDFHandler, Closeable {

        private final RDFHandler first;

        private final RDFHandler second;

        DuplicateHandler(final RDFHandler first, final RDFHandler second) {
            this.first = Util.checkNotNull(first);
            this.second = Util.checkNotNull(second);
        }

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
        public void close() throws IOException {
            Util.closeQuietly(this.first);
            Util.closeQuietly(this.second);
        }

    }

    private static final class RoundRobinHandler implements RDFHandler, Closeable {

        private final RDFHandler[] handlers;

        private final AtomicInteger counter;

        RoundRobinHandler(final Iterable<? extends RDFHandler> handlers) {
            final List<RDFHandler> list = new ArrayList<RDFHandler>();
            for (final RDFHandler handler : handlers) {
                list.add(handler);
            }
            this.handlers = list.toArray(new RDFHandler[list.size()]);
            this.counter = new AtomicInteger(0);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            for (final RDFHandler handler : this.handlers) {
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
            for (final RDFHandler handler : this.handlers) {
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
            for (final RDFHandler handler : this.handlers) {
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
        public void close() throws IOException {
            for (final RDFHandler handler : this.handlers) {
                Util.closeQuietly(handler);
            }
        }

        private RDFHandler pickHandler() {
            return this.handlers[this.counter.getAndIncrement() % this.handlers.length];
        }

    }

    private static final class DropStartEndHandler implements RDFHandler, Closeable {

        private final RDFHandler handler;

        DropStartEndHandler(final RDFHandler handler) {
            this.handler = Util.checkNotNull(handler);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.handler.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
        }

    }

    private static final class DropExtraPassesHandler implements RDFHandler, Closeable {

        private RDFHandler sink;

        DropExtraPassesHandler(final RDFHandler sink) {
            this.sink = Util.checkNotNull(sink);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.sink.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.sink.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.sink.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.sink.handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.sink.endRDF();
            } finally {
                this.sink = NOP_INSTANCE;
            }
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.sink);
        }

    }

    private static final class CollectorHandler implements RDFHandler, Closeable {

        private final Collection<? super Statement> collection;

        private final boolean dropContext;

        CollectorHandler(final Collection<? super Statement> collection, final boolean dropContext) {
            this.collection = Util.checkNotNull(collection);
            this.dropContext = dropContext;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            Statement t = statement;
            if (this.dropContext && statement.getContext() != null) {
                t = Util.FACTORY.createStatement(statement.getSubject(), statement.getPredicate(),
                        statement.getObject());
            }
            synchronized (this.collection) {
                this.collection.add(t);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
        }

        @Override
        public void close() throws IOException {
        }

    }

    private static final class RewriteBNodesHandler implements RDFHandler, Closeable {

        private final RDFHandler handler;

        private final String suffix;

        RewriteBNodesHandler(final RDFHandler handler, final String suffix) {
            this.handler = Util.checkNotNull(handler);
            this.suffix = Util.checkNotNull(suffix);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            if (statement.getSubject() instanceof BNode || statement.getObject() instanceof BNode
                    || statement.getContext() instanceof BNode) {

                final Resource s = statement.getSubject();
                final URI p = statement.getPredicate();
                final Value o = statement.getObject();
                final Resource c = statement.getContext();

                final Resource sr = s instanceof BNode ? Util.FACTORY.createBNode(((BNode) s)
                        .getID() + this.suffix) : s;
                final Value or = o instanceof BNode ? Util.FACTORY.createBNode(((BNode) o).getID()
                        + this.suffix) : o;
                final Resource cr = c instanceof BNode ? Util.FACTORY.createBNode(((BNode) c)
                        .getID() + this.suffix) : c;

                if (cr == null) {
                    this.handler.handleStatement(Util.FACTORY.createStatement(sr, p, or));
                } else {
                    this.handler.handleStatement(Util.FACTORY.createStatement(sr, p, or));
                }

            } else {
                this.handler.handleStatement(statement);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.handler.endRDF();
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
        }

    }

}
