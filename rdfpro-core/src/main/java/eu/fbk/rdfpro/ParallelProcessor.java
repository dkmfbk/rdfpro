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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Sorter;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Util;

final class ParallelProcessor extends RDFProcessor {

    private final SetOperation merging;

    private final RDFProcessor[] processors;

    private final int extraPasses;

    ParallelProcessor(final SetOperation merging, final RDFProcessor... processors) {
        if (processors.length == 0) {
            throw new IllegalArgumentException("Empty processor list supplied");
        }

        int extraPasses = 0;
        for (final RDFProcessor processor : processors) {
            extraPasses = Math.max(extraPasses, processor.getExtraPasses());
        }

        this.merging = Util.checkNotNull(merging);
        this.processors = processors.clone();
        this.extraPasses = extraPasses;
    }

    @Override
    public int getExtraPasses() {
        return this.extraPasses;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {

        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        final int numProcessors = this.processors.length;

        if (this.merging == SetOperation.UNION_ALL && numProcessors == 1) {
            return this.processors[0].getHandler(sink);
        }

        CollectorHandler collector;
        if (this.merging == SetOperation.UNION_ALL) {
            collector = new CollectorHandler(sink, numProcessors);
        } else if (this.merging == SetOperation.UNION_QUADS) {
            collector = new SorterCollectorHandler(sink, numProcessors, true);
        } else if (this.merging == SetOperation.UNION_TRIPLES) {
            collector = new UniqueTriplesCollectorHandler(sink, numProcessors);
        } else if (this.merging == SetOperation.INTERSECTION) {
            collector = new IntersectionCollectorHandler(sink, numProcessors);
        } else if (this.merging == SetOperation.DIFFERENCE) {
            collector = new DifferenceCollectorHandler(sink, numProcessors);
        } else {
            throw new Error();
        }

        final RDFHandler[] processorHandlers = new RDFHandler[numProcessors];
        final int[] extraPasses = new int[numProcessors];
        for (int i = 0; i < numProcessors; ++i) {
            final RDFProcessor processor = this.processors[i];
            extraPasses[i] = processor.getExtraPasses();
            if (this.merging == SetOperation.INTERSECTION
                    || this.merging == SetOperation.DIFFERENCE) {
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
        public void close() {
            for (final RDFHandler handler : this.handlers) {
                Util.closeQuietly(handler);
            }
        }

    }

    private static final class LabellerHandler extends HandlerWrapper {

        private final CollectorHandler collector;

        private final int label;

        LabellerHandler(final CollectorHandler handler, final int label) {
            super(handler);
            this.collector = handler;
            this.label = label;
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.collector.handleStatement(statement, this.label);
        }

    }

    private static class CollectorHandler extends HandlerWrapper {

        final int numProcessors;

        private int pendingProcessors;

        CollectorHandler(final RDFHandler handler, final int numLabels) {
            super(handler);
            this.numProcessors = numLabels;
            this.pendingProcessors = 0;
        }

        @Override
        public final void startRDF() throws RDFHandlerException {
            if (this.pendingProcessors <= 0) {
                this.pendingProcessors = this.numProcessors;
                super.startRDF();
                doStartRDF();
            }
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
                super.endRDF();
            }
        }

        @Override
        public final void close() {
            doClose();
            super.close();
        }

        void doStartRDF() throws RDFHandlerException {
        }

        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            super.handleStatement(statement);
        }

        void doEndRDF() throws RDFHandlerException {
        }

        void doClose() {
        }

    }

    private static class SorterCollectorHandler extends CollectorHandler {

        private final boolean parallelDecode;

        private Sorter<Object[]> sorter;

        SorterCollectorHandler(final RDFHandler handler, final int numLabels,
                final boolean parallelDecode) {
            super(handler, numLabels);
            this.parallelDecode = parallelDecode;
        }

        @Override
        void doStartRDF() throws RDFHandlerException {
            this.sorter = Sorter.newTupleSorter(true, Statement.class, Long.class);
            try {
                this.sorter.start(true);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doHandleStatement(final Statement statement, final int label)
                throws RDFHandlerException {
            try {
                this.sorter.emit(new Object[] { statement, label });
            } catch (final Throwable ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        void doEndRDF() throws RDFHandlerException {
            try {
                this.sorter.end(this.parallelDecode, new Consumer<Object[]>() {

                    @Override
                    public void accept(final Object[] record) {
                        try {
                            final Statement statement = (Statement) record[0];
                            final int label = ((Long) record[1]).intValue();
                            doHandleStatementSorted(statement, label);
                        } catch (final Throwable ex) {
                            Util.propagate(ex);
                        }
                    }

                });
                this.sorter.close();
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
                        statements.add(Statements.VALUE_FACTORY.createStatement(context,
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
                            : Statements.VALUE_FACTORY.createStatement(this.statementSubj,
                                    this.statementPred, this.statementObj, mergedContext);
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
                final String localName = Util.murmur3str(args);
                context = Statements.VALUE_FACTORY.createURI(namespace, localName);
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

}
