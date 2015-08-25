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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Sorter;
import eu.fbk.rdfpro.util.StatementDeduplicator;
import eu.fbk.rdfpro.util.Statements;

final class ProcessorUnique implements RDFProcessor {

    private final boolean mergeContexts;

    ProcessorUnique(final boolean mergeContexts) {
        this.mergeContexts = mergeContexts;
    }

    @SuppressWarnings("resource")
    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        Objects.requireNonNull(handler);
        return this.mergeContexts ? new MergeHandler(RDFHandlers.decouple(handler)) //
                : new Handler(handler, true);
    }

    private static final class KeepContextsHandler extends AbstractRDFHandlerWrapper {

        private final int threshold;

        private StatementDeduplicator deduplicator;

        private AtomicInteger count;

        private Sorter<Statement> sorter;

        KeepContextsHandler(final RDFHandler handler) {
            super(handler);
            this.threshold = (int) (Runtime.getRuntime().freeMemory() / 2 / 24);
            this.deduplicator = null;
            this.sorter = null;
            this.count = null;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.deduplicator = StatementDeduplicator.newHashDeduplicator();
            this.count = new AtomicInteger(0);
            this.sorter = Sorter.newStatementSorter(true);
            try {
                this.sorter.start(true);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void handleStatement(final Statement stmt) throws RDFHandlerException {
            try {
                if (this.deduplicator.isNew(stmt)) {
                    final int count = this.count.incrementAndGet();
                }

                this.sorter.emit(statement);
            } catch (final Throwable ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.sorter.end(true, new Consumer<Statement>() {

                    @Override
                    public void accept(final Statement statement) {
                        try {
                            KeepContextsHandler.this.handler.handleStatement(statement);
                        } catch (final RDFHandlerException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                });
                this.sorter.close();
                this.sorter = null;
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
            super.endRDF();
        }

        @Override
        public final void close() {
            IO.closeQuietly(this.sorter);
            super.close();
        }

    }

    private static final class MergeContextsHandler extends AbstractRDFHandlerWrapper {

        private final Map<Resource, List<Statement>> contextsStatements;

        private final Map<ContextSet, Resource> mergedContexts;

        @Nullable
        private Sorter<Statement> sorter;

        @Nullable
        private Statement statement;

        @Nullable
        private Resource statementSubj;

        @Nullable
        private URI statementPred;

        @Nullable
        private Value statementObj;

        @Nullable
        private Resource statementCtx; // if there is only a context;

        private final Set<Resource> statementContexts; // if there are multiple contexts

        public MergeContextsHandler(final RDFHandler handler) {
            super(handler);
            this.sorter = null;
            this.contextsStatements = new ConcurrentHashMap<>();
            this.mergedContexts = new HashMap<>();
            this.statementSubj = null;
            this.statementPred = null;
            this.statementObj = null;
            this.statementCtx = null;
            this.statementContexts = new HashSet<>();
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.sorter = Sorter.newStatementSorter(true);
            try {
                this.sorter.start(true);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            try {
                this.sorter.emit(statement);
            } catch (final Throwable ex) {
                throw new RDFHandlerException(ex);
            }
            final Resource context = statement.getContext();
            if (context != null) {
                this.contextsStatements.putIfAbsent(context, Collections.<Statement>emptyList());
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.sorter.end(false, new Consumer<Statement>() {

                    @Override
                    public void accept(final Statement statement) {
                        try {
                            handleStatementSorted(statement);
                        } catch (final RDFHandlerException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                });
                this.sorter.close();
                this.sorter = null;
                handleEndRDF();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
            super.endRDF();
        }

        void handleStatementSorted(final Statement statement) throws RDFHandlerException {

            final Resource subj = statement.getSubject();
            final URI pred = statement.getPredicate();
            final Value obj = statement.getObject();
            final Resource ctx = statement.getContext();

            List<Statement> contextStatements = this.contextsStatements.get(subj);
            if (contextStatements != null) {
                if (contextStatements.isEmpty()) {
                    contextStatements = new ArrayList<>();
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

        void handleEndRDF() throws RDFHandlerException {
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

            final ContextSet set = new ContextSet(contexts.toArray(new Resource[contexts.size()]));

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
                final String localName = Hash.murmur3(args).toString();
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

    private static class Handler extends AbstractRDFHandlerWrapper {

        private final boolean parallelize;

        private Sorter<Statement> sorter;

        Handler(final RDFHandler handler, final boolean parallelize) {
            super(handler);
            this.parallelize = parallelize;
            this.sorter = null;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.sorter = Sorter.newStatementSorter(true);
            try {
                this.sorter.start(true);
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            try {
                this.sorter.emit(statement);
            } catch (final Throwable ex) {
                throw new RDFHandlerException(ex);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            try {
                this.sorter.end(this.parallelize, new Consumer<Statement>() {

                    @Override
                    public void accept(final Statement statement) {
                        try {
                            handleStatementSorted(statement);
                        } catch (final RDFHandlerException ex) {
                            throw new RuntimeException(ex);
                        }
                    }

                });
                this.sorter.close();
                this.sorter = null;
                handleEndRDF();
            } catch (final IOException ex) {
                throw new RDFHandlerException(ex);
            }
            super.endRDF();
        }

        @Override
        public final void close() {
            IO.closeQuietly(this.sorter);
            super.close();
        }

        void handleStatementSorted(final Statement statement) throws RDFHandlerException {
            this.handler.handleStatement(statement);
        }

        void handleEndRDF() throws RDFHandlerException {
        }

    }

    private static final class MergeHandler extends Handler {

        private final Map<Resource, List<Statement>> contextsStatements;

        private final Map<ContextSet, Resource> mergedContexts;

        @Nullable
        private Statement statement;

        @Nullable
        private Resource statementSubj;

        @Nullable
        private URI statementPred;

        @Nullable
        private Value statementObj;

        @Nullable
        private Resource statementCtx; // if there is only a context;

        private final Set<Resource> statementContexts; // if there are multiple contexts

        public MergeHandler(final RDFHandler handler) {
            super(handler, false);
            this.contextsStatements = new ConcurrentHashMap<>();
            this.mergedContexts = new HashMap<>();
            this.statementSubj = null;
            this.statementPred = null;
            this.statementObj = null;
            this.statementCtx = null;
            this.statementContexts = new HashSet<>();
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            super.handleStatement(statement);
            final Resource context = statement.getContext();
            if (context != null) {
                this.contextsStatements.putIfAbsent(context, Collections.<Statement>emptyList());
            }
        }

        @Override
        void handleStatementSorted(final Statement statement) throws RDFHandlerException {

            final Resource subj = statement.getSubject();
            final URI pred = statement.getPredicate();
            final Value obj = statement.getObject();
            final Resource ctx = statement.getContext();

            List<Statement> contextStatements = this.contextsStatements.get(subj);
            if (contextStatements != null) {
                if (contextStatements.isEmpty()) {
                    contextStatements = new ArrayList<>();
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
        void handleEndRDF() throws RDFHandlerException {
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

            final ContextSet set = new ContextSet(contexts.toArray(new Resource[contexts.size()]));

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
                final String localName = Hash.murmur3(args).toString();
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

}
