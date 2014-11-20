package eu.fbk.rdfpro.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Sorter;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Tracker;
import eu.fbk.rdfpro.util.Util;

public class MapReduceProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapReduceProcessor.class);

    private static final int MAX_PENDING_JOBS = 2 * Util.CORES;

    private final boolean deduplicate;

    private final int targetJobSize;

    private final Mapper mapper;

    private final Reducer reducer;

    static MapReduceProcessor doCreate(final String... args) {
        // TODO: command line parsing
        return null;
    }

    public MapReduceProcessor(final boolean deduplicate, final int jobSize, final Mapper mapper,
            final Reducer reducer) {
        this.deduplicate = deduplicate;
        this.targetJobSize = Math.max(jobSize, 1);
        this.mapper = Util.checkNotNull(mapper);
        this.reducer = Util.checkNotNull(reducer);
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        return new Handler(sink);
    }

    private class Handler extends HandlerWrapper implements Consumer<Object[]> {

        private final List<Value> jobKeys;

        private final List<Statement[]> jobStatements;

        private int jobSize;

        private Value currentKey;

        private final List<Statement> currentStatements;

        private final AtomicReference<Throwable> exceptionHolder;

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
            this.semaphore = new Semaphore(MAX_PENDING_JOBS);
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
                this.sorter.start(MapReduceProcessor.this.deduplicate);
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
            final Value[] keys = MapReduceProcessor.this.mapper.map(statement);
            for (final Value key : keys) {
                if (key == null) {
                    super.handleStatement(statement); // bypass
                } else {
                    final Value s = statement.getSubject();
                    final Value p = statement.getPredicate();
                    final Value o = statement.getObject();
                    final Value c = statement.getContext();
                    final boolean skey = s == key;
                    final boolean pkey = p == key;
                    final boolean okey = o == key;
                    final boolean ckey = c == key;
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
                this.semaphore.acquire(MAX_PENDING_JOBS);
                this.tracker.end();
                super.endRDF();
            } catch (final Throwable ex) {
                Util.propagateIfPossible(ex, RDFHandlerException.class);
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

        private void flush(final boolean done) throws Throwable {

            final int numStmt = this.currentStatements.size();
            if (numStmt > 0) {
                this.jobKeys.add(this.currentKey);
                this.jobStatements.add(this.currentStatements.toArray(new Statement[numStmt]));
                this.jobSize += numStmt;
            }

            final int len = this.jobKeys.size();
            if (len == 0 || !done && this.jobSize < MapReduceProcessor.this.targetJobSize) {
                return;
            }

            final Throwable exception = this.exceptionHolder.get();
            if (exception != null) {
                Util.propagateIfPossible(exception, RDFHandlerException.class);
                throw new RDFHandlerException(exception);
            }

            final Value[] jobKeys = this.jobKeys.toArray(new Value[len]);
            final Statement[][] jobStatements = this.jobStatements.toArray(new Statement[len][]);
            this.jobKeys.clear();
            this.jobStatements.clear();
            this.jobSize = 0;
            this.semaphore.acquire();
            try {
                Util.getPool().execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            for (int i = 0; i < len; ++i) {
                                MapReduceProcessor.this.reducer.reduce(jobKeys[i],
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

    public interface Mapper {

        Value[] map(Statement statement) throws RDFHandlerException;

        public static Mapper forComponents(final String components) {

            final String comp = components.trim().toLowerCase();

            final int mask = (comp.contains("s") ? 0x08 : 0) | (comp.contains("p") ? 0x04 : 0)
                    | (comp.contains("o") ? 0x02 : 0) | (comp.contains("c") ? 0x01 : 0);

            if (mask == 0x08 || mask == 0x04 || mask == 0x02 || mask == 0x01) {
                return new Mapper() {

                    @Override
                    public Value[] map(final Statement statement) throws RDFHandlerException {
                        switch (mask) {
                        case 0x08:
                            return new Value[] { statement.getSubject() };
                        case 0x04:
                            return new Value[] { statement.getPredicate() };
                        case 0x02:
                            return new Value[] { statement.getObject() };
                        case 0x01:
                            return new Value[] { statement.getContext() };
                        default:
                            throw new Error();
                        }
                    }

                };
            }

            return new Mapper() {

                @Override
                public Value[] map(final Statement statement) throws RDFHandlerException {

                    final boolean hasSubj = (mask & 0x80) != 0;
                    final boolean hasPred = (mask & 0x40) != 0;
                    final boolean hasObj = (mask & 0x20) != 0;
                    final boolean hasCtx = (mask & 0x10) != 0;

                    int header = 0;
                    int count = 0;

                    if (hasSubj) {
                        final int bits = classify(statement.getSubject());
                        header |= bits << 24;
                        count += bits & 0xF;
                    }
                    if (hasPred) {
                        final int bits = classify(statement.getPredicate());
                        header |= bits << 16;
                        count += bits & 0xF;
                    }
                    if (hasObj) {
                        final int bits = classify(statement.getObject());
                        header |= bits << 8;
                        count += bits & 0xF;
                    }
                    if (hasCtx) {
                        final int bits = classify(statement.getContext());
                        header |= bits;
                        count += bits & 0xF;
                    }

                    final String[] strings = new String[count];
                    int index = 0;
                    strings[index++] = Integer.toString(header);
                    if (hasSubj) {
                        index = add(strings, index, statement.getSubject());
                    }
                    if (hasPred) {
                        index = add(strings, index, statement.getPredicate());
                    }
                    if (hasObj) {
                        index = add(strings, index, statement.getObject());
                    }
                    if (hasCtx) {
                        index = add(strings, index, statement.getContext());
                    }

                    return new Value[] { Statements.VALUE_FACTORY.createBNode(Util
                            .murmur3str(strings)) };
                }

                private int classify(final Value value) {
                    if (value == null) {
                        return 0;
                    } else if (value instanceof BNode) {
                        return 0x11;
                    } else if (value instanceof URI) {
                        return 0x21;
                    }
                    final Literal l = (Literal) value;
                    if (l.getDatatype() != null) {
                        return 0x42;
                    } else if (l.getLabel() != null) {
                        return 0x52;
                    } else {
                        return 0x31;
                    }
                }

                private int add(final String[] strings, int index, final Value value) {
                    if (value instanceof URI || value instanceof BNode) {
                        strings[index++] = value.stringValue();
                    } else if (value instanceof Literal) {
                        final Literal l = (Literal) value;
                        strings[index++] = l.getLabel();
                        if (l.getDatatype() != null) {
                            strings[index++] = l.getDatatype().stringValue();
                        } else if (l.getLanguage() != null) {
                            strings[index++] = l.getLanguage();
                        }
                    }
                    return index;
                }

            };
        }
    }

    public interface Reducer {

        void reduce(Value key, Statement[] statements, RDFHandler handler)
                throws RDFHandlerException;

    }

}
