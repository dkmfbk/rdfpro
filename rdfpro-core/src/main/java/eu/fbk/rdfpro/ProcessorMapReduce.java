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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.Sorter;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;

final class ProcessorMapReduce implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorMapReduce.class);

    private static final int MIN_RUNNABLE_STATEMENTS = 256; // min 256 statements per runnable

    private static final int MAX_RUNNABLE_MULTIPLIER = 4; // 4 runnables enqueued per core

    private final Mapper mapper;

    private final Reducer reducer;

    private final boolean deduplicate;

    ProcessorMapReduce(final Mapper mapper, final Reducer reducer, final boolean deduplicate) {
        this.mapper = Objects.requireNonNull(mapper);
        this.reducer = Objects.requireNonNull(reducer);
        this.deduplicate = deduplicate;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(Objects.requireNonNull(handler));
    }

    private final class Handler extends AbstractRDFHandlerWrapper implements Consumer<Object[]> {

        private final List<Value> jobKeys;

        private final List<Statement[]> jobStatements;

        private int jobSize;

        private Value currentKey;

        private final List<Statement> currentStatements;

        private final AtomicReference<Throwable> exceptionHolder;

        private final int semaphoreSize;

        private final Semaphore semaphore;

        private Sorter<Object[]> sorter;

        private final Tracker tracker;

        Handler(final RDFHandler handler) {
            super(handler);
            this.jobKeys = new ArrayList<Value>();
            this.jobStatements = new ArrayList<Statement[]>();
            this.jobSize = 0;
            this.currentKey = null;
            this.currentStatements = new ArrayList<Statement>();
            this.exceptionHolder = new AtomicReference<Throwable>();
            this.semaphoreSize = MAX_RUNNABLE_MULTIPLIER * Environment.getCores();
            this.semaphore = new Semaphore(this.semaphoreSize);
            this.sorter = Sorter.newTupleSorter(true, Value.class, Value.class, Value.class,
                    Value.class, Value.class, Long.class);
            this.tracker = new Tracker(LOGGER, null, //
                    "%d reductions (%d red/s avg)", //
                    "%d reductions (%d red/s, %d red/s avg)");
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            try {
                this.sorter.start(ProcessorMapReduce.this.deduplicate);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // dropped
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            final Value[] keys = ProcessorMapReduce.this.mapper.map(statement);
            for (final Value key : keys) {
                if (Mapper.BYPASS_KEY.equals(key)) {
                    super.handleStatement(statement); // bypass
                } else {
                    final Value s = statement.getSubject();
                    final Value p = statement.getPredicate();
                    final Value o = statement.getObject();
                    final Value c = statement.getContext();
                    final boolean skey = Objects.equals(s, key);
                    final boolean pkey = Objects.equals(p, key);
                    final boolean okey = Objects.equals(o, key);
                    final boolean ckey = Objects.equals(c, key);
                    final Object[] record = new Object[6];
                    record[0] = key;
                    record[1] = skey ? null : s;
                    record[2] = pkey ? null : p;
                    record[3] = okey ? null : o;
                    record[4] = ckey ? null : c;
                    record[5] = new Long((skey ? 0x08 : 0) | (pkey ? 0x04 : 0) | (okey ? 0x02 : 0)
                            | (ckey ? 0x01 : 0));
                    try {
                        this.sorter.emit(record);
                    } catch (final IOException ex) {
                        throw new RDFHandlerException(ex);
                    }
                }
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.tracker.start();
                this.sorter.end(false, this);
                flush(true);
                this.semaphore.acquire(this.semaphoreSize);
                this.tracker.end();
                super.endRDF();
            } catch (final InterruptedException | IOException ex) {
                throw new RDFHandlerException(ex);
            } finally {
                this.sorter.close();
                this.sorter = null;
            }
        }

        @Override
        public void accept(final Object[] record) {

            final Value key = (Value) record[0];
            final int mask = ((Number) record[5]).intValue();

            final Resource s = (Resource) ((mask & 0x08) != 0 ? key : record[1]);
            final URI p = (URI) ((mask & 0x04) != 0 ? key : record[2]);
            final Value o = (Value) ((mask & 0x02) != 0 ? key : record[3]);
            final Resource c = (Resource) ((mask & 0x01) != 0 ? key : record[4]);

            final ValueFactory vf = Statements.VALUE_FACTORY;
            final Statement statement = c == null ? vf.createStatement(s, p, o) //
                    : vf.createStatement(s, p, o, c);

            if (!key.equals(this.currentKey)) {
                try {
                    flush(false);
                } catch (final Throwable ex) {
                    throw new RuntimeException(ex);
                }
                this.currentKey = key;
                this.currentStatements.clear();
            }

            this.currentStatements.add(statement);
        }

        private void flush(final boolean done) throws RDFHandlerException, InterruptedException {

            final int numStmt = this.currentStatements.size();
            if (numStmt > 0) {
                this.jobKeys.add(this.currentKey);
                this.jobStatements.add(this.currentStatements.toArray(new Statement[numStmt]));
                this.jobSize += numStmt;
            }

            final int len = this.jobKeys.size();
            if (len == 0 || !done && this.jobSize < MIN_RUNNABLE_STATEMENTS) {
                return;
            }

            final Throwable exception = this.exceptionHolder.get();
            if (exception != null) {
                if (exception instanceof RDFHandlerException) {
                    throw (RDFHandlerException) exception;
                } else if (exception instanceof RuntimeException) {
                    throw (RuntimeException) exception;
                } else if (exception instanceof Error) {
                    throw (Error) exception;
                }
                throw new RDFHandlerException(exception);
            }

            final Value[] jobKeys = this.jobKeys.toArray(new Value[len]);
            final Statement[][] jobStatements = this.jobStatements.toArray(new Statement[len][]);
            this.jobKeys.clear();
            this.jobStatements.clear();
            this.jobSize = 0;
            this.semaphore.acquire(); // will block if too many runnables were submitted
            try {
                Environment.getPool().execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            for (int i = 0; i < len; ++i) {
                                ProcessorMapReduce.this.reducer.reduce(jobKeys[i],
                                        jobStatements[i], Handler.this.handler);
                                Handler.this.tracker.increment();
                            }
                        } catch (final Throwable ex) {
                            synchronized (Handler.this.exceptionHolder) {
                                final Throwable exception = Handler.this.exceptionHolder.get();
                                if (exception != null) {
                                    exception.addSuppressed(ex);
                                } else {
                                    Handler.this.exceptionHolder.set(ex);
                                }
                            }
                        } finally {
                            Handler.this.semaphore.release();
                        }
                    }

                });
            } catch (final Throwable ex) {
                this.semaphore.release();
                throw ex;
            }
        }

    }

}
