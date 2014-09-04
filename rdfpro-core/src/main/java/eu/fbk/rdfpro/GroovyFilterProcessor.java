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
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import groovy.lang.MissingMethodException;
import groovy.lang.Script;
import groovy.util.GroovyScriptEngine;

final class GroovyFilterProcessor extends RDFProcessor {

    private static final GroovyScriptEngine ENGINE;

    private final Class<?> scriptClass;

    private final String[] scriptArgs;

    static {
        try {
            final String[] roots = Util.settingFor("rdfpro.groovy.classpath", "").split("[;:]");
            ENGINE = new GroovyScriptEngine(roots);
            ENGINE.getConfig().setScriptBaseClass(HandlerScript.class.getName());
        } catch (final Throwable ex) {
            throw new Error("Could not initialize Groovy: " + ex.getMessage(), ex);
        }
    }

    GroovyFilterProcessor(final String scriptExprOrFile, final String... scriptArgs) {

        Util.checkNotNull(scriptExprOrFile);

        Class<?> scriptClass = null;
        try {
            scriptClass = ENGINE.loadScriptByName(scriptExprOrFile);
        } catch (final Throwable ex) {
            try {
                final Path path = Files.createTempFile("rdfpro-filter-", ".groovy");
                Files.write(path, scriptExprOrFile.getBytes(Charset.forName("UTF-8")));
                scriptClass = ENGINE.loadScriptByName(path.toUri().toString());
            } catch (final Throwable ex2) {
                throw new Error("Could not compile Groovy script", ex2);
            }
        }

        this.scriptClass = scriptClass;
        this.scriptArgs = scriptArgs.clone();
    }

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        try {
            final HandlerScript script = (HandlerScript) this.scriptClass.newInstance();
            script.doInit(handler, this.scriptArgs);
            return new Handler(handler, script);
        } catch (final Throwable ex) {
            throw new Error("Could not instantiate script class", ex);
        }
    }

    private final class Handler implements RDFHandler, Closeable {

        private final RDFHandler handler;

        private final HandlerScript script;

        Handler(final RDFHandler handler, final HandlerScript script) {
            this.handler = Util.checkNotNull(handler);
            this.script = script;
        }

        @Override
        public synchronized void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
            this.script.doStart();
        }

        @Override
        public synchronized void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public synchronized void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        @Override
        public synchronized void handleStatement(final Statement statement)
                throws RDFHandlerException {
            this.script.doHandle(statement);
        }

        @Override
        public synchronized void endRDF() throws RDFHandlerException {
            this.script.doEnd();
            this.handler.endRDF();
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
        }

    }

    public static class Quad {

        public Resource s;

        public URI p;

        public Value o;

        @Nullable
        public Resource c;

        public Quad(final Resource s, final URI p, final Value o, @Nullable final Resource c) {
            this.s = s;
            this.p = p;
            this.o = o;
            this.c = c;
        }

    }

    public static abstract class HandlerScript extends Script {

        private RDFHandler handler;

        private boolean startEnabled;

        private boolean handleEnabled;

        private boolean endEnabled;

        private boolean insideRun;

        private boolean statementEmitted;

        private Statement statement;

        private Quad quad;

        private int pass;

        protected HandlerScript() {
            this.startEnabled = true;
            this.handleEnabled = true;
            this.endEnabled = true;
            this.pass = 0;
        }

        @Override
        public Object getProperty(final String property) {
            // Directly matching variables this way is faster than storing them in binding object
            if (this.insideRun && property.length() == 1) {
                if ("q".equals(property)) {
                    return this.quad;
                } else if ("s".equals(property)) {
                    return this.quad.s;
                } else if ("p".equals(property)) {
                    return this.quad.p;
                } else if ("o".equals(property)) {
                    return this.quad.o;
                } else if ("c".equals(property)) {
                    return this.quad.c;
                } else if ("t".equals(property)) {
                    return this.quad.p.equals(RDF.TYPE) ? this.quad.o : null;
                } else if ("l".equals(property)) {
                    return this.quad.o instanceof Literal ? ((Literal) this.quad.o).getLanguage()
                            : null;
                } else if ("d".equals(property)) {
                    return this.quad.o instanceof Literal ? ((Literal) this.quad.o).getDatatype()
                            : null;
                }
            }
            if ("__rdfpro__".equals(property)) {
                return true; // flag to signal the script is being called by RDFpro
            }
            return super.getProperty(property);
        }

        @Override
        public void setProperty(final String property, final Object value) {
            // Directly matching variables this way is faster than storing them in binding object
            if (this.insideRun && property.length() == 1) {
                if ("q".equals(property)) {
                    this.quad = (Quad) value;
                } else if ("s".equals(property)) {
                    this.quad.s = (Resource) toRDFValue(value, false);
                } else if ("p".equals(property)) {
                    this.quad.p = (URI) toRDFValue(value, false);
                } else if ("c".equals(property)) {
                    this.quad.c = (Resource) toRDFValue(value, false);
                } else if ("t".equals(property)) {
                    this.quad.o = toRDFValue(value, false);
                    this.quad.p = RDF.TYPE;
                } else {
                    // Following code serves to asseble literals starting from label, lang, dt
                    boolean setLiteral = false;
                    String newLabel = null;
                    String newLang = null;
                    URI newDatatype = null;
                    if ("o".equals(property)) {
                        if (value instanceof Value) {
                            this.quad.o = (Value) value;
                        } else {
                            newLabel = value.toString();
                            setLiteral = true;
                        }
                    } else if ("l".equals(property)) {
                        newLang = value == null ? null : value.toString();
                        setLiteral = true;
                    } else if ("d".equals(property)) {
                        newDatatype = value == null ? null : (URI) toRDFValue(value, false);
                        setLiteral = true;
                    }
                    if (setLiteral) {
                        if (this.quad.o instanceof Literal) {
                            final Literal l = (Literal) this.quad.o;
                            newLabel = newLabel != null ? newLabel : l.getLabel();
                            newLang = newLang != null ? newLang : l.getLanguage();
                            newDatatype = newDatatype != null ? newDatatype : l.getDatatype();
                        }
                        this.quad.o = newLang != null ? Util.FACTORY.createLiteral(newLabel,
                                newLang) : newDatatype != null ? Util.FACTORY.createLiteral(
                                newLabel, newDatatype) : Util.FACTORY.createLiteral(newLabel);
                    }
                }
            }
            super.setProperty(property, value);
        }

        final void doInit(final RDFHandler handler, final String[] args)
                throws RDFHandlerException {
            this.handler = handler;
            tryInvokeMethod("init", Arrays.asList(args));
        }

        final void doStart() throws RDFHandlerException {
            if (this.startEnabled) {
                this.startEnabled = tryInvokeMethod("start", this.pass);
            }
        }

        final void doHandle(final Statement statement) throws RDFHandlerException {

            this.statement = statement;
            this.quad = new Quad(statement.getSubject(), statement.getPredicate(),
                    statement.getObject(), statement.getContext());

            if (this.handleEnabled) {
                if (tryInvokeMethod("handle", this.quad)) {
                    return;
                }
                this.handleEnabled = false;
            }

            this.insideRun = true;
            try {
                this.run();
            } catch (final Throwable ex) {
                Util.propagateIfPossible(ex, RDFHandlerException.class);
                throw new RDFHandlerException(ex);
            } finally {
                this.insideRun = false;
            }
        }

        final void doEnd() throws RDFHandlerException {
            if (this.endEnabled) {
                this.endEnabled = tryInvokeMethod("end", this.pass);
            }
            ++this.pass;
        }

        protected final boolean emit() throws RDFHandlerException {
            if (this.quad.s != this.statement.getSubject()
                    || this.quad.p != this.statement.getPredicate()
                    || this.quad.o != this.statement.getObject()
                    || this.quad.c != this.statement.getContext()) {
                this.statementEmitted = false;
                this.statement = this.quad.c == null ? Util.FACTORY.createStatement(this.quad.s,
                        this.quad.p, this.quad.o) : Util.FACTORY.createStatement(this.quad.s,
                        this.quad.p, this.quad.o, this.quad.c);
            }
            if (this.statementEmitted) {
                return false;
            }
            this.handler.handleStatement(this.statement);
            this.statementEmitted = true;
            return true;
        }

        protected final boolean emitIf(@Nullable final Object condition)
                throws RDFHandlerException {
            return condition == Boolean.TRUE && emit();
        }

        protected final boolean emitIfNot(@Nullable final Object condition)
                throws RDFHandlerException {
            return condition == Boolean.FALSE && emit();
        }

        protected final boolean emit(@Nullable final Quad quad) throws RDFHandlerException {
            if (quad != null && quad.s != null && quad.p != null && quad.o != null) {
                this.handler.handleStatement(quad.c != null ? Util.FACTORY.createStatement(quad.s,
                        quad.p, quad.o, quad.c) : Util.FACTORY.createStatement(quad.s, quad.p,
                        quad.o));
                return true;
            }
            return false;
        }

        protected final boolean emit(@Nullable final Object s, @Nullable final Object p,
                @Nullable final Object o, @Nullable final Object c) throws RDFHandlerException {

            final Value sv = toRDFValue(s, false);
            final Value pv = toRDFValue(p, false);
            final Value ov = toRDFValue(o, true);
            final Value cv = toRDFValue(c, false);

            if (sv instanceof Resource && pv instanceof URI && ov != null) {
                if (cv == null) {
                    this.handler.handleStatement(Util.FACTORY.createStatement((Resource) sv,
                            (URI) pv, ov));
                    return true;
                } else if (cv instanceof Resource) {
                    this.handler.handleStatement(Util.FACTORY.createStatement((Resource) sv,
                            (URI) pv, ov, (Resource) cv));
                    return true;
                }
            }

            return false;
        }

        // QUAD CREATION

        protected final Quad quad(final Object s, final Object p, final Object o, final Object c) {
            final Resource sv = (Resource) toRDFValue(s, false);
            final URI pv = (URI) toRDFValue(p, false);
            final Value ov = toRDFValue(o, true);
            final Resource cv = (Resource) toRDFValue(c, false);
            return new Quad(sv, pv, ov, cv);
        }

        // SPARQL FUNCTIONS

        protected final boolean isIRI(@Nullable final Object arg) {
            return arg instanceof URI;
        }

        protected final boolean isBlank(@Nullable final Object arg) {
            return arg instanceof BNode;
        }

        protected final boolean isLiteral(@Nullable final Object arg) {
            return arg != null && !(arg instanceof Resource);
        }

        protected final boolean isNumeric(@Nullable final Object arg) {
            if (arg instanceof Literal) {
                try {
                    ((Literal) arg).doubleValue();
                    return true;
                } catch (final Throwable ex) {
                    // ignore
                }
            }
            return false;
        }

        @Nullable
        protected final String str(@Nullable final Object arg) {
            return arg == null ? null : arg.toString();
        }

        @Nullable
        protected final String lang(@Nullable final Object arg) {
            if (arg instanceof Literal) {
                final String lang = ((Literal) arg).getLanguage();
                return lang != null ? lang : "";
            }
            return null;
        }

        @Nullable
        protected final URI datatype(@Nullable final Object arg) {
            if (arg instanceof Literal) {
                final Literal l = (Literal) arg;
                final URI dt = l.getDatatype();
                return dt != null ? dt : l.getLanguage() != null ? RDF.LANGSTRING
                        : XMLSchema.STRING;
            }
            return null;
        }

        @Nullable
        protected final URI iri(@Nullable final Object arg) {
            if (arg instanceof URI) {
                return (URI) arg;
            } else if (arg != null) {
                return Util.FACTORY.createURI(arg.toString());
            }
            return null;
        }

        protected final BNode bnode() {
            return Util.FACTORY.createBNode();
        }

        @Nullable
        protected final BNode bnode(@Nullable final Object arg) {
            if (arg instanceof BNode) {
                return (BNode) arg;
            } else if (arg != null) {
                return Util.FACTORY.createBNode(Util.toString(Util.murmur3(arg.toString())));
            }
            return null;
        }

        @Nullable
        protected final Literal strdt(@Nullable final Object value, @Nullable final Object datatype) {
            if (value == null) {
                return null;
            } else if (datatype == null) {
                return Util.FACTORY.createLiteral(value.toString());
            } else if (datatype instanceof URI) {
                return Util.FACTORY.createLiteral(value.toString(), (URI) datatype);
            } else {
                final URI dt = Util.FACTORY.createURI(datatype.toString());
                return Util.FACTORY.createLiteral(value.toString(), dt);
            }
        }

        @Nullable
        protected final Literal strlang(@Nullable final Object value, @Nullable final Object lang) {
            if (value == null) {
                return null;
            } else if (lang == null) {
                return Util.FACTORY.createLiteral(value.toString());
            } else {
                return Util.FACTORY.createLiteral(value.toString(), lang.toString());
            }
        }

        protected final URI uuid() {
            return Util.FACTORY.createURI("urn:uuid:" + UUID.randomUUID().toString());
        }

        protected final Literal struuid() {
            return Util.FACTORY.createLiteral(UUID.randomUUID().toString());
        }

        // TODO: add remaining SPARQL functions

        // UTILITY FUNCTIONS

        private boolean tryInvokeMethod(final String method, final Object arg)
                throws RDFHandlerException {
            try {
                invokeMethod(method, arg);
                return true;
            } catch (final MissingMethodException ex) {
                return false;
            } catch (final Throwable ex) {
                Util.propagateIfPossible(ex, RDFHandlerException.class);
                throw new RDFHandlerException(ex);
            }
        }

        @Nullable
        private Value toRDFValue(final Object object, final boolean mayBeLiteral) {
            if (object instanceof Value) {
                return (Value) object;
            } else if (object == null) {
                return null;
            } else if (mayBeLiteral) {
                return Util.FACTORY.createLiteral(object.toString());
            } else {
                return Util.FACTORY.createURI(object.toString());
            }
        }

    }

}
