package eu.fbk.rdfpro;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.QuadModel;

final class ProcessorTSV implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorTSV.class);

    private final Path path;

    private final TupleExpr query;

    private final List<String> variables;

    @Nullable
    private final Mapper mapper;

    ProcessorTSV(final Path path, final TupleExpr query, @Nullable final Mapper mapper) {
        this.path = Objects.requireNonNull(path);
        this.query = Objects.requireNonNull(query);
        this.variables = extractVariables(query);
        this.mapper = mapper;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        RDFHandler tsvHandler = new Handler();
        if (this.mapper != null) {
            tsvHandler = new ProcessorMapReduce(this.mapper, (Handler) tsvHandler, false)
                    .wrap(tsvHandler);
        }
        return RDFHandlers.dispatchAll(handler, tsvHandler);
    }

    private static List<String> extractVariables(final TupleExpr query) {
        if (query instanceof UnaryTupleOperator) {
            UnaryTupleOperator node = (UnaryTupleOperator) query;
            while (!(node instanceof Projection) && node.getArg() instanceof UnaryTupleOperator) {
                node = (UnaryTupleOperator) node.getArg();
            }
            if (node instanceof Projection) {
                final List<String> variables = new ArrayList<>();
                for (final ProjectionElem elem : ((Projection) node).getProjectionElemList()
                        .getElements()) {
                    variables.add(elem.getTargetName());
                }
                return variables;
            }
        }
        throw new IllegalArgumentException("Invalid query: " + query);
    }

    final class Handler extends AbstractRDFHandler implements Reducer {

        @Nullable
        private OutputStream stream;

        @Nullable
        private List<Writer> writers;

        @Nullable
        private ThreadLocal<Writer> threadWriter;

        @Nullable
        private QuadModel model;

        @Nullable
        private AtomicInteger rows;

        private void evaluateQuery(final QuadModel model) throws RDFHandlerException {
            try {
                final Writer writer = this.threadWriter.get();
                final Iterator<BindingSet> i = model.evaluate(ProcessorTSV.this.query, null, null);
                while (i.hasNext()) {
                    final BindingSet bindings = i.next();
                    String separator = "";
                    for (final String variable : ProcessorTSV.this.variables) {
                        writer.write(separator);
                        final Value value = bindings.getValue(variable);
                        if (value instanceof Literal) {
                            final String label = ((Literal) value).getLabel();
                            for (int j = 0; j < label.length(); ++j) {
                                final char c = label.charAt(j);
                                writer.write(c == '\t' || c == '\n' ? ' ' : c);
                            }
                        } else if (value instanceof IRI) {
                            writer.write(((IRI) value).stringValue());
                        } else if (value instanceof BNode) {
                            writer.write(((BNode) value).getID());
                        }
                        separator = "\t";
                    }
                    writer.write("\n");
                    this.rows.incrementAndGet();
                }
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            try {
                this.stream = IO.write(ProcessorTSV.this.path.toString());
                this.writers = new ArrayList<>();
                this.threadWriter = new ThreadLocal<Writer>() {

                    @Override
                    protected Writer initialValue() {
                        final Writer writer = IO
                                .utf8Writer(IO.parallelBuffer(Handler.this.stream, (byte) '\n'));
                        synchronized (Handler.this.writers) {
                            Handler.this.writers.add(writer);
                        }
                        return writer;
                    }

                };
                this.rows = new AtomicInteger(0);
            } catch (final IOException ex) {
                IO.closeQuietly(this.stream);
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            if (this.model == null) {
                this.model = QuadModel.create();
            }
            this.model.add(statement);
        }

        @Override
        public void reduce(final Value key, final Statement[] statements, final RDFHandler handler)
                throws RDFHandlerException {

            // TODO
            boolean found = false;
            for (Statement statement : statements) {
                if (statement.getPredicate().stringValue().equals("ex:type")) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return;
            }

            final QuadModel model = QuadModel.create();
            model.addAll(Arrays.asList(statements));
            evaluateQuery(model);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                if (this.model != null) {
                    evaluateQuery(this.model);
                }
                LOGGER.info("{} rows emitted", this.rows.get());
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            if (this.writers != null) {
                for (final Writer writer : this.writers) {
                    IO.closeQuietly(writer);
                }
            }
            IO.closeQuietly(this.stream);
            this.threadWriter = null;
            this.writers = null;
            this.stream = null;
            this.model = null;
            this.rows = null;
        }

    }

}
