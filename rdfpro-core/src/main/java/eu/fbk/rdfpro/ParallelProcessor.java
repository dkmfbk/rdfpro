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
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ParallelProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParallelProcessor.class);

    private final Merging merging;

    private final RDFProcessor[] processors;

    ParallelProcessor(final Merging merging, final RDFProcessor... processors) {
        if (processors.length == 0) {
            throw new IllegalArgumentException("Empty processor list supplied");
        }
        this.merging = Util.checkNotNull(merging);
        this.processors = processors.clone();
    }

    @Override
    public int getExtraPasses() {
        int result = 0;
        for (final RDFProcessor processor : this.processors) {
            result = Math.max(result, processor.getExtraPasses());
        }
        return result;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {

        final int numProcessors = this.processors.length;

        if (this.merging == Merging.UNION_ALL && numProcessors == 1) {
            return this.processors[0].getHandler(handler);
        }

        CollectorHandler collector;
        if (this.merging == Merging.UNION_ALL) {
            collector = new CollectorHandler(handler, numProcessors);
        } else if (this.merging == Merging.UNION_QUADS) {
            collector = new SorterCollectorHandler(handler, numProcessors, true);
        } else if (this.merging == Merging.UNION_TRIPLES) {
            collector = new UniqueTriplesCollectorHandler(handler, numProcessors);
        } else if (this.merging == Merging.INTERSECTION) {
            collector = new IntersectionCollectorHandler(handler, numProcessors);
        } else if (this.merging == Merging.DIFFERENCE) {
            collector = new DifferenceCollectorHandler(handler, numProcessors);
        } else {
            throw new Error();
        }

        final RDFHandler[] processorHandlers = new RDFHandler[numProcessors];
        final int[] extraPasses = new int[numProcessors];
        for (int i = 0; i < numProcessors; ++i) {
            final RDFProcessor processor = this.processors[i];
            extraPasses[i] = processor.getExtraPasses();
            if (this.merging == Merging.INTERSECTION || this.merging == Merging.DIFFERENCE) {
                processorHandlers[i] = processor.getHandler(new LabellerHandler(collector, i));
            } else {
                processorHandlers[i] = processor.getHandler(collector);
            }
        }

        return new DispatcherHandler(processorHandlers, extraPasses);
    }

    private static final class DispatcherHandler implements RDFHandler, Closeable {

        private final RDFHandler[] handlers;

        private final int[] extraPasses;

        private RDFHandler[] passHandlers;

        private int passIndex; // counting downward to zero

        DispatcherHandler(final RDFHandler[] handlers, final int[] extraPasses) {

            int maxExtraPasses = 0;
            for (int i = 0; i < extraPasses.length; ++i) {
                maxExtraPasses = Math.max(maxExtraPasses, extraPasses[i]);
            }

            this.handlers = handlers;
            this.extraPasses = extraPasses;
            this.passHandlers = null;
            this.passIndex = maxExtraPasses;
        }

        @Override
        public void startRDF() throws RDFHandlerException {

            if (this.passIndex == 0) {
                this.passHandlers = this.handlers;
            } else {
                final List<RDFHandler> list = new ArrayList<RDFHandler>();
                for (int i = 0; i < this.handlers.length; ++i) {
                    if (this.extraPasses[i] >= this.passIndex) {
                        list.add(this.handlers[i]);
                    }
                }
                this.passHandlers = list.toArray(new RDFHandler[list.size()]);
                --this.passIndex;
            }

            for (final RDFHandler handler : this.passHandlers) {
                handler.startRDF();
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleComment(comment);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleNamespace(prefix, uri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.handleStatement(statement);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            for (final RDFHandler handler : this.passHandlers) {
                handler.endRDF();
            }
        }

        @Override
        public void close() throws IOException {
            for (final RDFHandler handler : this.handlers) {
                Util.closeQuietly(handler);
            }
        }

    }

    private static final class LabellerHandler implements RDFHandler, Closeable {

        private final CollectorHandler handler;

        private final int label;

        LabellerHandler(final CollectorHandler handler, final int label) {
            this.handler = handler;
            this.label = label;
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
            this.handler.handleStatement(statement, this.label);
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

    private static class CollectorHandler implements RDFHandler, Closeable {

        final RDFHandler handler;

        final int numProcessors;

        private int pendingProcessors;

        CollectorHandler(final RDFHandler handler, final int numLabels) {
            this.handler = handler;
            this.numProcessors = numLabels;
            this.pendingProcessors = 0;
        }

        @Override
        public final void startRDF() throws RDFHandlerException {
            if (this.pendingProcessors <= 0) {
                this.pendingProcessors = this.numProcessors;
                this.handler.startRDF();
                doStartRDF();
            }
        }

        @Override
        public final void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public final void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public final void handleStatement(final Statement statement) throws RDFHandlerException {
            handleStatement(statement, 0);
        }

        public final void handleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            doHandleStatement(statement, label);
        }

        @Override
        public final void endRDF() throws RDFHandlerException {
            --this.pendingProcessors;
            if (this.pendingProcessors == 0) {
                doEndRDF();
                this.handler.endRDF();
            }
        }

        @Override
        public final void close() {
            doClose();
            Util.closeQuietly(this.handler);
        }

        void doStartRDF() throws RDFHandlerException {
        }

        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            this.handler.handleStatement(statement);
        }

        void doEndRDF() throws RDFHandlerException {
        }

        void doClose() {
        }

    }

    private static class SorterCollectorHandler extends CollectorHandler {

        private final boolean parallelDecode;

        private final Encoder encoder;

        private final Tracker writeTracker;

        private final Tracker readTracker;

        private Sorter sorter;

        SorterCollectorHandler(final RDFHandler handler, final int numLabels,
                final boolean parallelDecode) {
            super(handler, numLabels);
            this.parallelDecode = parallelDecode;
            this.encoder = new Encoder();
            this.writeTracker = new Tracker(LOGGER, null, //
                    "%d triples to sort (%d tr/s avg)", "2" + toString(), //
                    "%d triples to sort (%d tr/s, %d tr/s avg)");
            this.readTracker = new Tracker(LOGGER, null, //
                    "%d triples from sort (%d tr/s avg)", "3" + toString(), //
                    "%d triples from sort (%d tr/s, %d tr/s avg)");
            this.sorter = null;
        }

        @Override
        void doStartRDF() throws RDFHandlerException {
            this.sorter = new Sorter(this.parallelDecode) {

                @Override
                protected void decode(final Reader reader) throws Throwable {
                    while (true) {
                        final Statement statement = SorterCollectorHandler.this.encoder
                                .read(reader);
                        if (statement == null) {
                            return; // EOF
                        }
                        final int label = reader.read();
                        if (label != 0) {
                            reader.read(); // read the delimiter
                        }
                        doHandleStatementSorted(statement, label);
                        SorterCollectorHandler.this.readTracker.increment();
                    }
                }

            };
            try {
                this.sorter.start();
                this.writeTracker.start();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            try {
                final Writer writer = this.sorter.getWriter();
                this.encoder.write(writer, statement);
                if (label > 0) {
                    writer.write(label);
                }
                writer.write(0);
                this.writeTracker.increment();
            } catch (final Throwable ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doEndRDF() throws RDFHandlerException {
            try {
                this.writeTracker.end();
                this.readTracker.start();
                this.sorter.end();
                this.readTracker.end();
                this.sorter = null;
            } catch (final Throwable ex) {
                Util.propagateIfPossible(ex, RDFHandlerException.class);
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doClose() {
            Util.closeQuietly(this.sorter);
        }

        void doHandleStatementSorted(final Statement statement, final int label) throws Throwable {
            this.handler.handleStatement(statement);
        }

    }

    private static final class UniqueTriplesCollectorHandler extends SorterCollectorHandler {

        private final Map<Resource, List<Statement>> contextsStatements;

        private final Map<ContextSet, Resource> mergedContexts;

        @Nullable
        private Resource lastContext; // optimization to avoid a map lookup at every statement

        @Nullable
        private Statement statement;

        @Nullable
        private Resource statementSubj;

        @Nullable
        private Resource statementCtx; // if there is only a context;

        private final Set<Resource> statementContexts; // if there are multiple contexts

        @Nullable
        private URI statementPred;

        @Nullable
        private Value statementObj;

        UniqueTriplesCollectorHandler(final RDFHandler handler, final int numProcessors) {
            super(handler, numProcessors, false);
            this.contextsStatements = new HashMap<Resource, List<Statement>>();
            this.mergedContexts = new HashMap<ContextSet, Resource>();
            this.lastContext = null;
            this.statementSubj = null;
            this.statementPred = null;
            this.statementObj = null;
            this.statementContexts = new HashSet<Resource>();
        }

        @Override
        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            final Resource context = statement.getContext();
            if (context != null && !context.equals(this.lastContext)) {
                this.lastContext = context;
                final List<Statement> statements = this.contextsStatements.get(context);
                if (statements == null) {
                    // Start with shared immutable list to avoid consuming memory for contexts
                    // without statements
                    this.contextsStatements.put(context, Collections.<Statement>emptyList());
                }
            }
            super.doHandleStatement(statement, label);
        }

        @Override
        void doHandleStatementSorted(final Statement statement, final int label) throws Throwable {

            final Resource subj = statement.getSubject();
            final URI pred = statement.getPredicate();
            final Value obj = statement.getObject();
            final Resource ctx = statement.getContext();

            List<Statement> contextStatements = this.contextsStatements.get(subj);
            if (contextStatements != null) {
                if (contextStatements.isEmpty()) {
                    contextStatements = new ArrayList<Statement>();
                    this.contextsStatements.put(subj, contextStatements);
                }
                contextStatements.add(statement); // context data buffered and emitted later

            } else if (subj.equals(this.statementSubj) && pred.equals(this.statementPred)
                    && obj.equals(this.statementObj)) {
                if (this.statementCtx != null) {
                    if (ctx == null) {
                        this.statementCtx = null;
                        this.statement = statement;
                    } else {
                        if (this.statementContexts.isEmpty()) {
                            // we add the context firstly seen only now, so to avoid useless work
                            // in the frequent case the input contains almost unique statements
                            this.statementContexts.add(this.statementCtx);
                        }
                        this.statementContexts.add(ctx);
                    }
                }

            } else {
                flush();
                this.statement = statement;
                this.statementSubj = subj;
                this.statementPred = pred;
                this.statementObj = obj;
                this.statementCtx = ctx;
                this.statementContexts.clear();
            }
        }

        @Override
        void doEndRDF() throws RDFHandlerException {
            super.doEndRDF();
            flush();
            for (final List<Statement> statements : this.contextsStatements.values()) {
                for (final Statement statement : statements) {
                    this.handler.handleStatement(statement);
                }
            }
            for (final Map.Entry<ContextSet, Resource> entry : this.mergedContexts.entrySet()) {
                final ContextSet set = entry.getKey();
                final Resource context = entry.getValue();
                final Set<Statement> statements = new HashSet<Statement>();
                for (final Resource source : set.contexts) {
                    for (final Statement statement : this.contextsStatements.get(source)) {
                        statements.add(Util.FACTORY.createStatement(context,
                                statement.getPredicate(), statement.getObject(),
                                statement.getContext()));
                    }
                }
                for (final Statement statement : statements) {
                    this.handler.handleStatement(statement);
                }
            }
        }

        private void flush() throws RDFHandlerException {
            if (this.statement != null) {
                Statement statement;
                if (this.statementCtx == null || this.statementContexts.size() <= 1) {
                    statement = this.statement;
                } else {
                    final Resource mergedContext = mergeContexts(this.statementContexts);
                    statement = mergedContext.equals(this.statement.getContext()) ? this.statement
                            : Util.FACTORY.createStatement(this.statementSubj, this.statementPred,
                                    this.statementObj, mergedContext);
                }
                this.handler.handleStatement(statement);
            }
        }

        @Nullable
        private Resource mergeContexts(final Set<Resource> contexts) {

            final int size = contexts.size();
            final Resource[] contextArray = new Resource[size];
            contexts.toArray(contextArray);
            final ContextSet set = new ContextSet(contextArray);

            Resource context = this.mergedContexts.get(set);
            if (context == null) {
                final String[] args = new String[contexts.size()];
                String namespace = null;
                int index = 0;
                for (final Resource source : contexts) {
                    args[index++] = source.stringValue();
                    if (source instanceof URI) {
                        final String ns = ((URI) source).getNamespace();
                        if (namespace == null) {
                            namespace = ns;
                        } else {
                            final int length = Math.min(ns.length(), namespace.length());
                            for (int i = 0; i < length; ++i) {
                                if (ns.charAt(i) != namespace.charAt(i)) {
                                    namespace = ns.substring(0, i);
                                    break;
                                }
                            }
                        }
                    }
                }
                Arrays.sort(args);
                if (namespace == null || "".equals(namespace)) {
                    namespace = "urn:graph:";
                } else if (!namespace.endsWith("/") && !namespace.endsWith("#")
                        && !namespace.endsWith(":")) {
                    namespace = namespace + "/";
                }
                final long[] hc = Util.murmur3(args);
                final String localName = Util.toString(hc);
                context = Util.FACTORY.createURI(namespace, localName);
                this.mergedContexts.put(set, context);
            }
            return context;
        }

        private static final class ContextSet {

            Resource[] contexts;

            int hash;

            ContextSet(final Resource[] contexts) {

                int hash = 0;
                for (final Resource context : contexts) {
                    hash += context.hashCode();
                }

                this.contexts = contexts;
                this.hash = hash;
            }

            @Override
            public boolean equals(final Object object) {
                if (object == this) {
                    return true;
                }
                if (!(object instanceof ContextSet)) {
                    return false;
                }
                final ContextSet other = (ContextSet) object;
                if (this.hash != other.hash || this.contexts.length != other.contexts.length) {
                    return false;
                }
                final boolean[] matched = new boolean[this.contexts.length];
                outer: for (int i = 0; i < this.contexts.length; ++i) {
                    final Resource thisContext = this.contexts[i];
                    for (int j = 0; j < this.contexts.length; ++j) {
                        if (!matched[j]) {
                            final Resource otherContext = other.contexts[j];
                            if (thisContext.equals(otherContext)) {
                                matched[j] = true;
                                continue outer;
                            }
                        }
                    }
                    return false;
                }
                return true;
            }

            @Override
            public int hashCode() {
                return this.hash;
            }

        }

    }

    private static final class IntersectionCollectorHandler extends SorterCollectorHandler {

        @Nullable
        private Statement statement;

        private int count;

        IntersectionCollectorHandler(final RDFHandler handler, final int numLabels) {
            super(handler, numLabels, false);
            this.statement = null;
            this.count = 0;
        }

        @Override
        void doHandleStatementSorted(final Statement statement, final int label) throws Throwable {
            if (statement.equals(this.statement)
                    && Objects.equals(statement.getContext(), this.statement.getContext())) {
                ++this.count;
            } else {
                if (this.statement != null && this.count == this.numProcessors) {
                    this.handler.handleStatement(statement);
                }
                this.statement = statement;
                this.count = 1;
            }
        }

    }

    private static final class DifferenceCollectorHandler extends SorterCollectorHandler {

        @Nullable
        private Statement statement;

        private int maxLabel;

        DifferenceCollectorHandler(final RDFHandler handler, final int numLabels) {
            super(handler, numLabels, false);
            this.statement = null;
            this.maxLabel = 0;
        }

        @Override
        void doHandleStatementSorted(final Statement statement, final int label) throws Throwable {
            if (statement.equals(this.statement)
                    && Objects.equals(statement.getContext(), this.statement.getContext())) {
                if (label > this.maxLabel) {
                    this.maxLabel = label;
                }
            } else {
                if (this.statement != null && this.maxLabel == 0) {
                    this.handler.handleStatement(statement);
                }
                this.statement = statement;
                this.maxLabel = label;
            }
        }

    }

    private static final class Encoder {

        private static final char EOV = 1; // end of value

        private static final int TYPE_URI = 0;

        private static final int TYPE_SHORTENED_URI = 1;

        private static final int TYPE_ENCODED_URI = 2;

        private static final int TYPE_BNODE = 3;

        private static final int TYPE_PLAIN_LITERAL = 4;

        private static final int TYPE_TYPED_LITERAL = 5;

        private static final int TYPE_LANG_LITERAL = 6;

        private static final int TYPE_NONE = 7;

        private final Index<URI> predicateIndex;

        private final Index<URI> typeIndex;

        private final Index<URI> contextIndex;

        private final Index<URI> datatypeIndex;

        private final Index<String> prefixIndex;

        private final Index<String> langIndex;

        Encoder() {
            this.predicateIndex = new Index<URI>(64 * 1024);
            this.typeIndex = new Index<URI>(64 * 1024);
            this.contextIndex = new Index<URI>(64 * 1024);
            this.datatypeIndex = new Index<URI>(1024);
            this.prefixIndex = new Index<String>(64 * 1024);
            this.langIndex = new Index<String>(1024);
        }

        void write(final Writer writer, final Statement statement) throws IOException {

            final Resource s = statement.getSubject();
            final URI p = statement.getPredicate();
            final Value o = statement.getObject();
            final Resource c = statement.getContext();

            final Val sv = encode(s, null);
            final Val pv = encode(p, this.predicateIndex);
            final Val ov = encode(o, p.equals(RDF.TYPE) ? this.typeIndex : null);
            final Val cv = encode(c, this.contextIndex);

            final int mask1 = (sv.type << 5 | pv.type << 3 | ov.type) + 1; // +1 so not to be 0
            final int mask2 = cv.type + 1; // +1 so not to be 0

            writer.write(mask1);
            write(writer, sv);
            write(writer, pv);
            write(writer, ov);
            writer.write(mask2);
            write(writer, cv);
        }

        private void write(final Writer writer, final Val val) throws IOException {
            if (val.type == TYPE_SHORTENED_URI || val.type == TYPE_ENCODED_URI
                    || val.type == TYPE_TYPED_LITERAL || val.type == TYPE_LANG_LITERAL) {
                writer.write(val.index);
            }
            if (val.type != TYPE_ENCODED_URI && val.type != TYPE_NONE) {
                final String s = val.string;
                for (int i = 0; i < s.length(); ++i) {
                    final char ch = s.charAt(i);
                    if (ch > 2) {
                        writer.write(ch);
                    } else { // ESCAPE, EOV or EOF
                        writer.write(2);
                        writer.write(ch + 2); // avoid emitting EOV / EOF
                    }
                }
                writer.write(EOV);
            }
        }

        private Val encode(@Nullable final Value value, @Nullable final Index<URI> uriIndex) {

            if (value == null) {
                return new Val(TYPE_NONE, (char) 0, null);

            } else if (value instanceof BNode) {
                return new Val(TYPE_BNODE, (char) 0, ((BNode) value).getID());

            } else if (value instanceof URI) {
                final URI uri = (URI) value;
                if (uriIndex != null) {
                    final Character index = uriIndex.put(uri);
                    if (index != null) {
                        return new Val(TYPE_ENCODED_URI, index, null);
                    }
                }
                final String string = uri.stringValue();
                final int offset = string.lastIndexOf('/', Math.min(string.length(), 128));
                if (offset > 0) {
                    final String prefix = string.substring(0, offset + 1);
                    final Character index = this.prefixIndex.put(prefix);
                    if (index != null) {
                        return new Val(TYPE_SHORTENED_URI, index, string.substring(offset + 1));
                    }
                }
                return new Val(TYPE_URI, (char) 0, string);

            } else if (value instanceof Literal) {
                final Literal literal = (Literal) value;
                if (literal.getDatatype() != null) {
                    final Character index = this.datatypeIndex.put(literal.getDatatype());
                    if (index == null) {
                        throw new Error("Too many datatypes (!)");
                    }
                    return new Val(TYPE_TYPED_LITERAL, index, literal.getLabel());
                }
                if (literal.getLanguage() != null) {
                    final Character index = this.langIndex.put(literal.getLanguage());
                    if (index == null) {
                        throw new Error("Too many languages (!)");
                    }
                    return new Val(TYPE_LANG_LITERAL, index, literal.getLabel());
                }
                return new Val(TYPE_PLAIN_LITERAL, (char) 0, literal.getLabel());

            } else {
                throw new Error();
            }
        }

        @Nullable
        Statement read(final Reader reader) throws IOException {

            int mask1 = reader.read();
            if (mask1 < 0) {
                return null; // EOF
            }

            mask1 -= 1;
            final Val sv = read(reader, mask1 >> 5 & 0x03);
            final Val pv = read(reader, mask1 >> 3 & 0x03);
            final Val ov = read(reader, mask1 & 0x07);

            final int mask2 = reader.read() - 1;
            final Val cv = read(reader, mask2 & 0x07);

            final Resource s = (Resource) decode(sv, null);
            final URI p = (URI) decode(pv, this.predicateIndex);
            final Value o = decode(ov, p.equals(RDF.TYPE) ? this.typeIndex : null);
            final Resource c = (Resource) decode(cv, this.contextIndex);

            return c == null ? Util.FACTORY.createStatement(s, p, o) : Util.FACTORY
                    .createStatement(s, p, o, c);
        }

        private Val read(final Reader reader, final int type) throws IOException {

            String string = null;
            char index = 0;

            if (type == TYPE_SHORTENED_URI || type == TYPE_ENCODED_URI
                    || type == TYPE_TYPED_LITERAL || type == TYPE_LANG_LITERAL) {
                index = (char) reader.read();
            }
            if (type != TYPE_ENCODED_URI && type != TYPE_NONE) {
                final StringBuilder builder = new StringBuilder(256);
                while (true) {
                    char ch = (char) reader.read();
                    if (ch > 2) {
                        builder.append(ch);
                    } else if (ch == 2) {
                        ch = (char) (reader.read() - 2);
                        builder.append(ch);
                    } else {
                        break;
                    }
                }
                string = builder.toString();
            }

            return new Val(type, index, string);
        }

        private Value decode(final Val val, final Index<URI> uriIndex) {

            switch (val.type) {
            case TYPE_URI:
                return Util.FACTORY.createURI(val.string);

            case TYPE_SHORTENED_URI:
                final String prefix = this.prefixIndex.get(val.index);
                return Util.FACTORY.createURI(prefix + val.string);

            case TYPE_ENCODED_URI:
                return uriIndex.get(val.index);

            case TYPE_BNODE:
                return Util.FACTORY.createBNode(val.string);

            case TYPE_PLAIN_LITERAL:
                return Util.FACTORY.createLiteral(val.string);

            case TYPE_TYPED_LITERAL:
                final URI datatype = this.datatypeIndex.get(val.index);
                return Util.FACTORY.createLiteral(val.string, datatype);

            case TYPE_LANG_LITERAL:
                final String lang = this.langIndex.get(val.index);
                return Util.FACTORY.createLiteral(val.string, lang);

            case TYPE_NONE:
                return null;

            default:
                throw new Error();
            }
        }

        private static final class Val {

            final int type;

            final char index;

            final String string;

            Val(final int type, final char index, final String string) {
                this.type = type;
                this.index = index;
                this.string = string;
            }

        }

        private static final class Index<T> {

            private final Map<T, Character> map;

            private final List<T> list;

            private final int size;

            Index(final int size) {
                this.map = new HashMap<T, Character>(size);
                this.list = new ArrayList<T>(size);
                this.size = Math.min(64 * 1024, 64 * 1024 - 2);
            }

            @Nullable
            synchronized Character put(final T element) {
                Character index = this.map.get(element);
                if (index == null && this.list.size() < this.size) {
                    index = (char) (this.list.size() + 1);
                    this.list.add(element);
                    this.map.put(element, index);
                }
                return index;
            }

            @Nullable
            synchronized T get(final char ch) {
                return this.list.get(ch - 1);
            }

        }

    }

}
