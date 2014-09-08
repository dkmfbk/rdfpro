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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.codehaus.groovy.control.customizers.ImportCustomizer;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.turtle.TurtleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import groovy.lang.MissingMethodException;
import groovy.lang.Script;
import groovy.util.GroovyScriptEngine;
import groovy.util.ResourceConnector;
import groovy.util.ResourceException;

final class GroovyFilterProcessor extends RDFProcessor {

    // TODO: optionally, we can provide for two execution modalities, one failing at first error
    // and the other continuing

    private static final Logger LOGGER = LoggerFactory.getLogger(GroovyFilterProcessor.class);

    private static final Logger SCRIPT_LOGGER = LoggerFactory
            .getLogger(GroovyFilterProcessor.class.getName() + ".script"); // can be improved

    private static final GroovyScriptEngine ENGINE;

    private static final DatatypeFactory DATATYPE_FACTORY;

    private final Class<?> scriptClass;

    private final String[] scriptArgs;

    static {
        try {
            final ImportCustomizer customizer = new ImportCustomizer();
            customizer.addStaticStars("eu.fbk.rdfpro.SparqlFunctions");
            final String classpath = Util.settingFor("rdfpro.groovy.classpath", "");
            ENGINE = new GroovyScriptEngine(new Loader(classpath));
            ENGINE.getConfig().setScriptBaseClass(HandlerScript.class.getName());
            ENGINE.getConfig().addCompilationCustomizers(customizer);
            LOGGER.debug("Groovy classpath: " + classpath);
        } catch (final Throwable ex) {
            throw new Error("Could not initialize Groovy: " + ex.getMessage(), ex);
        }
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (final DatatypeConfigurationException ex) {
            throw new Error("Could not instantiate javax.xml.datatype.DatatypeFactory", ex);
        }
    }

    public GroovyFilterProcessor(final String scriptExprOrFile, final String... scriptArgs) {

        Util.checkNotNull(scriptExprOrFile);

        Class<?> scriptClass = null;
        try {
            try {
                scriptClass = ENGINE.loadScriptByName(scriptExprOrFile);
            } catch (final ResourceException ex) {
                final Path path = Files.createTempFile("rdfpro-filter-", ".groovy");
                Files.write(path, scriptExprOrFile.getBytes(Charset.forName("UTF-8")));
                scriptClass = ENGINE.loadScriptByName(path.toUri().toString());
            }
        } catch (final Throwable ex) {
            throw new Error("Could not compile Groovy script", ex);
        }

        this.scriptClass = scriptClass;
        this.scriptArgs = scriptArgs.clone();
    }

    public Object getProperty(final RDFHandler handler, final String name) {
        if (!(handler instanceof Handler)) {
            return null;
        }
        final Handler h = (Handler) handler;
        return h.getScript().getProperty(name);
    }

    public Object setProperty(final RDFHandler handler, final String name, final Object value) {
        if (!(handler instanceof Handler)) {
            return null;
        }
        final Handler h = (Handler) handler;
        final Object oldValue = h.getScript().getProperty(name);
        h.getScript().setProperty(name, value);
        return oldValue;
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

    private static final class Loader implements ResourceConnector {

        private URL[] roots;

        // We cache connections as the Groovy engines access each file multiple times (3 times)
        // for checking its existence and modification time. As we rewrite file content each time
        // we cache the rewritten byte array for performance reasons
        private Map<URL, URLConnection> connections;

        public Loader(final String classpath) {
            try {
                final String[] paths = classpath.split("[;:]");
                this.roots = new URL[paths.length];
                for (int i = 0; i < paths.length; ++i) {
                    final String path = paths[i];
                    if (path.indexOf("://") != -1) {
                        this.roots[i] = new URL(path);
                    } else {
                        this.roots[i] = new File(path).toURI().toURL();
                    }
                }
                this.connections = new HashMap<URL, URLConnection>();

            } catch (final MalformedURLException ex) {
                throw new IllegalArgumentException(ex);
            }
        }

        @Override
        public URLConnection getResourceConnection(final String name) throws ResourceException {

            URLConnection connection = null;
            ResourceException exception = null;

            for (final URL root : this.roots) {
                URL scriptURL = null;
                try {
                    scriptURL = new URL(root, name);
                    connection = this.connections.get(scriptURL);
                    if (connection == null) {
                        connection = new Connection(scriptURL);
                        connection.connect(); // load resource in advance
                        this.connections.put(scriptURL, connection);
                    }
                    if (connection != null) {
                        break;
                    }

                } catch (final MalformedURLException ex) {
                    final String message = "Malformed URL: " + root + ", " + name;
                    exception = exception == null ? new ResourceException(message)
                            : new ResourceException(message, exception);

                } catch (final IOException ex) {
                    connection = null;
                    final String message = "Cannot open URL: " + root + name;
                    exception = exception == null ? new ResourceException(message)
                            : new ResourceException(message, exception);
                }
            }

            if (connection == null) {
                if (exception == null) {
                    exception = new ResourceException("No resource for " + name + " was found");
                }
                throw exception;
            }

            return connection;
        }

        private static String filter(final AtomicInteger counter, final String string) {

            final StringBuilder builder = new StringBuilder();
            final int length = string.length();
            int i = 0;

            try {
                while (i < length) {
                    char c = string.charAt(i);
                    if (c == '<') {
                        final int end = parseURI(string, i);
                        if (end >= 0) {
                            final URI u = (URI) Util.parseValue(string.substring(i, end));
                            builder.append("__iri(").append(counter.getAndIncrement())
                                    .append(", \"").append(u.stringValue()).append("\")");
                            i = end;
                        } else {
                            builder.append(c);
                            ++i;
                        }

                    } else if (TurtleUtil.isPN_CHARS_BASE(c)) {
                        final int end = parseQName(string, i);
                        if (end >= 0) {
                            final URI u = (URI) Util.parseValue(string.substring(i, end));
                            builder.append("__iri(").append(counter.getAndIncrement())
                                    .append(", \"").append(u.stringValue()).append("\")");
                            i = end;
                        } else {
                            do {
                                builder.append(c);
                                c = string.charAt(++i);
                            } while (Character.isLetterOrDigit(c));
                        }

                    } else if (c == '\'' || c == '\"') {
                        final char d = c; // delimiter
                        builder.append(d);
                        do {
                            c = string.charAt(++i);
                            builder.append(c);
                        } while (c != d || string.charAt(i - 1) == '\\');
                        ++i;

                    } else {
                        builder.append(c);
                        ++i;
                    }
                }
            } catch (final Exception ex) {
                throw new IllegalArgumentException("Illegal URI escaping near offset " + i, ex);
            }

            return builder.toString();
        }

        private static int parseURI(final String string, int i) {

            final int len = string.length();

            if (string.charAt(i) != '<') {
                return -1;
            }

            for (++i; i < len; ++i) {
                final char c = string.charAt(i);
                if (c == '<' || c == '\"' || c == '{' || c == '}' || c == '|' || c == '^'
                        || c == '`' || c == '\\' || c == ' ') {
                    return -1;
                }
                if (c == '>') {
                    return i + 1;
                }
            }

            return -1;
        }

        private static int parseQName(final String string, int i) {

            final int len = string.length();
            char c;

            if (!TurtleUtil.isPN_CHARS_BASE(string.charAt(i))) {
                return -1;
            }

            for (; i < len; ++i) {
                c = string.charAt(i);
                if (!TurtleUtil.isPN_CHARS(c) && c != '.') {
                    break;
                }
            }

            if (string.charAt(i - 1) == '.' || string.charAt(i) != ':' || i == len - 1) {
                return -1;
            }

            c = string.charAt(++i);
            if (!TurtleUtil.isPN_CHARS_U(c) && c != ':' && c != '%' && !Character.isDigit(c)) {
                return -1;
            }

            for (; i < len; ++i) {
                c = string.charAt(i);
                if (!TurtleUtil.isPN_CHARS(c) && c != '.' && c != ':' && c != '%') {
                    break;
                }
            }

            if (string.charAt(i - 1) == '.') {
                return -1;
            }

            return i;
        }

        private class Connection extends URLConnection {

            private Map<String, List<String>> headers;

            private byte[] bytes;

            Connection(final URL url) {
                super(url);
            }

            @Override
            public void connect() throws IOException {

                if (this.connected) {
                    return;
                }

                final URLConnection conn = this.url.openConnection();
                conn.setAllowUserInteraction(getAllowUserInteraction());
                conn.setConnectTimeout(getConnectTimeout());
                conn.setDefaultUseCaches(getDefaultUseCaches());
                conn.setDoInput(getDoInput());
                conn.setDoOutput(getDoOutput());
                conn.setIfModifiedSince(getIfModifiedSince());
                conn.setReadTimeout(getReadTimeout());
                conn.setUseCaches(getUseCaches());
                for (final Map.Entry<String, List<String>> entry : getRequestProperties()
                        .entrySet()) {
                    final String key = entry.getKey();
                    for (final String value : entry.getValue()) {
                        conn.setRequestProperty(key, value);
                    }
                }
                conn.connect();

                final String encoding = conn.getContentEncoding();
                final Charset charset = Charset.forName(encoding != null ? encoding : "UTF-8");

                final StringBuilder builder = new StringBuilder();
                final InputStream stream = conn.getInputStream();
                final BufferedReader reader = new BufferedReader(new InputStreamReader(stream,
                        charset));
                try {
                    String line;
                    final AtomicInteger counter = new AtomicInteger();
                    while ((line = reader.readLine()) != null) {
                        builder.append(filter(counter, line)).append("\n");
                    }
                } finally {
                    reader.close();
                }

                this.bytes = builder.toString().getBytes(charset);
                this.connected = true;

                this.headers = new HashMap<String, List<String>>(conn.getHeaderFields());

                LOGGER.debug("Loaded {}", getURL());
            }

            @Override
            public Map<String, List<String>> getHeaderFields() {
                return this.headers;
            }

            @Override
            public String getHeaderField(final String name) {
                final List<String> list = this.headers == null ? null : this.headers.get(name);
                return list == null ? null : list.get(list.size() - 1);
            }

            @Override
            public String getHeaderFieldKey(final int n) {
                final Iterator<String> iterator = this.headers.keySet().iterator();
                for (int i = 0; i < n; ++i) {
                    if (iterator.hasNext()) {
                        iterator.next();
                    }
                }
                return iterator.hasNext() ? iterator.next() : null;
            }

            @Override
            public String getHeaderField(final int n) {
                return getHeaderField(getHeaderFieldKey(n));
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return new ByteArrayInputStream(this.bytes);
            }

        }

    }

    private static final class Handler implements RDFHandler, Closeable {

        private final RDFHandler handler;

        private final HandlerScript script;

        Handler(final RDFHandler handler, final HandlerScript script) {
            this.handler = Util.checkNotNull(handler);
            this.script = script;
        }

        public HandlerScript getScript() {
            return this.script;
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

    public static abstract class HandlerScript extends Script {

        private RDFHandler handler;

        private boolean startEnabled;

        private boolean handleEnabled;

        private boolean endEnabled;

        private boolean insideRun;

        private GroovyStatement statement;

        private int pass;

        private URI[] uriConsts;

        protected HandlerScript() {
            this.startEnabled = true;
            this.handleEnabled = true;
            this.endEnabled = true;
            this.pass = 0;
            this.uriConsts = new URI[0];
        }

        @Override
        public Object getProperty(final String property) {
            // Directly matching variables this way is faster than storing them in binding object
            if (this.insideRun && property.length() == 1) {
                if ("q".equals(property)) {
                    return this.statement;
                } else if ("s".equals(property)) {
                    return this.statement.s;
                } else if ("p".equals(property)) {
                    return this.statement.p;
                } else if ("o".equals(property)) {
                    return this.statement.o;
                } else if ("c".equals(property)) {
                    return this.statement.c;
                } else if ("t".equals(property)) {
                    return this.statement.p.equals(RDF.TYPE) ? this.statement.o : null;
                } else if ("l".equals(property)) {
                    return this.statement.o instanceof Literal ? ((Literal) this.statement.o)
                            .getLanguage() : null;
                } else if ("d".equals(property)) {
                    return this.statement.o instanceof Literal ? ((Literal) this.statement.o)
                            .getDatatype() : null;
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
                    this.statement = normalize((Statement) value);
                } else if ("s".equals(property)) {
                    this.statement.s = (Resource) toRDF(value, false);
                } else if ("p".equals(property)) {
                    this.statement.p = (URI) toRDF(value, false);
                } else if ("c".equals(property)) {
                    this.statement.c = (Resource) toRDF(value, false);
                } else if ("t".equals(property)) {
                    this.statement.o = toRDF(value, false);
                    this.statement.p = RDF.TYPE;
                } else {
                    // Following code serves to asseble literals starting from label, lang, dt
                    boolean setLiteral = false;
                    String newLabel = null;
                    String newLang = null;
                    URI newDatatype = null;
                    if ("o".equals(property)) {
                        if (value instanceof Value) {
                            this.statement.o = (Value) value;
                        } else {
                            newLabel = value.toString();
                            setLiteral = true;
                        }
                    } else if ("l".equals(property)) {
                        newLang = value == null ? null : value.toString();
                        setLiteral = true;
                    } else if ("d".equals(property)) {
                        newDatatype = value == null ? null : (URI) toRDF(value, false);
                        setLiteral = true;
                    }
                    if (setLiteral) {
                        if (this.statement.o instanceof Literal) {
                            final Literal l = (Literal) this.statement.o;
                            newLabel = newLabel != null ? newLabel : l.getLabel();
                            newLang = newLang != null ? newLang : l.getLanguage();
                            newDatatype = newDatatype != null ? newDatatype : l.getDatatype();
                        }
                        this.statement.o = newLang != null ? Util.FACTORY.createLiteral(newLabel,
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
            final boolean called = tryInvokeMethod("init", Arrays.asList(args));
            if (called && LOGGER.isDebugEnabled()) {
                LOGGER.debug("Called init() with " + Arrays.asList(args));
            }
        }

        final void doStart() throws RDFHandlerException {
            if (this.startEnabled) {
                this.startEnabled = tryInvokeMethod("start", this.pass);
                if (this.startEnabled && LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Called start() for pass " + this.pass);
                }
            }
        }

        final void doHandle(final Statement statement) throws RDFHandlerException {

            this.statement = normalize(statement);

            if (this.handleEnabled) {
                if (tryInvokeMethod("handle", this.statement)) {
                    return;
                }
                this.handleEnabled = false;
                LOGGER.debug("Using script body (no handle() method)");
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
                if (this.endEnabled && LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Called end() for pass " + this.pass);
                }
            }
            ++this.pass;
        }

        // INTERNAL FUNCTIONS

        protected final URI __iri(final int index, final Object arg) {
            if (index >= this.uriConsts.length) {
                this.uriConsts = Arrays.copyOf(this.uriConsts, index + 1);
            }
            URI uri = this.uriConsts[index];
            if (uri == null) {
                uri = arg instanceof URI ? (URI) arg : Util.FACTORY.createURI(arg.toString());
                this.uriConsts[index] = uri;
            }
            return uri;
        }

        // QUAD CREATION AND EMISSION FUNCTIONS

        protected final Statement quad(final Object s, final Object p, final Object o,
                final Object c) {
            final Resource sv = (Resource) toRDF(s, false);
            final URI pv = (URI) toRDF(p, false);
            final Value ov = toRDF(o, true);
            final Resource cv = (Resource) toRDF(c, false);
            return new GroovyStatement(sv, pv, ov, cv);
        }

        protected final void emit() throws RDFHandlerException {
            this.handler.handleStatement(this.statement);
        }

        protected final boolean emitIf(@Nullable final Object condition)
                throws RDFHandlerException {
            if (condition == Boolean.TRUE) {
                emit();
                return true;
            }
            return false;
        }

        protected final boolean emitIfNot(@Nullable final Object condition)
                throws RDFHandlerException {
            if (condition == Boolean.FALSE) {
                emit();
                return true;
            }
            return false;
        }

        protected final boolean emit(@Nullable final Statement statement)
                throws RDFHandlerException {
            if (!(statement instanceof GroovyStatement) || ((GroovyStatement) statement).isValid()) {
                this.handler.handleStatement(statement);
                return true;
            }
            return false;
        }

        protected final boolean emit(@Nullable final Object s, @Nullable final Object p,
                @Nullable final Object o, @Nullable final Object c) throws RDFHandlerException {

            final Value sv = toRDF(s, false);
            final Value pv = toRDF(p, false);
            final Value ov = toRDF(o, true);
            final Value cv = toRDF(c, false);

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

        // ERROR REPORTING AND LOGGING FUNCTIONS

        protected final void error(@Nullable final Object message) throws RDFHandlerException {
            final String string = message == null ? "ERROR" : message.toString();
            SCRIPT_LOGGER.error(string);
            throw new RDFHandlerException(string);
        }

        protected final void error(@Nullable final Object message, @Nullable final Throwable ex)
                throws RDFHandlerException {
            final String string = message == null ? "ERROR" : message.toString();
            if (ex != null) {
                SCRIPT_LOGGER.error(string, ex);
                throw new RDFHandlerException(string, ex);
            } else {
                SCRIPT_LOGGER.error(string);
                throw new RDFHandlerException(string);
            }
        }

        protected final void log(final Object message) {
            if (message != null) {
                SCRIPT_LOGGER.info(message.toString());
            }
        }

        // TODO consider caching of loaded file components (very optional)
        protected final ValueSet loadSet(final Object file, final Object components) {
            final File inputFile;
            if (file instanceof File) {
                inputFile = (File) file;
            } else {
                inputFile = new File(file.toString());
            }
            final String pattern = components.toString();
            return new ValueSet(Util.createHashSet(pattern, inputFile));
        }

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

    }

    @Nullable
    private static Value toRDF(final Object object, final boolean mayBeLiteral) {
        if (object instanceof Value) {
            return normalize((Value) object);
        }
        if (object == null) {
            return null;
        }
        if (mayBeLiteral) {
            if (object instanceof Long) {
                return new GroovyLiteral(object.toString(), XMLSchema.LONG);
            } else if (object instanceof Integer) {
                return new GroovyLiteral(object.toString(), XMLSchema.INT);
            } else if (object instanceof Short) {
                return new GroovyLiteral(object.toString(), XMLSchema.SHORT);
            } else if (object instanceof Byte) {
                return new GroovyLiteral(object.toString(), XMLSchema.BYTE);
            } else if (object instanceof Double) {
                return new GroovyLiteral(object.toString(), XMLSchema.DOUBLE);
            } else if (object instanceof Float) {
                return new GroovyLiteral(object.toString(), XMLSchema.FLOAT);
            } else if (object instanceof Boolean) {
                return new GroovyLiteral(object.toString(), XMLSchema.BOOLEAN);
            } else if (object instanceof XMLGregorianCalendar) {
                final XMLGregorianCalendar c = (XMLGregorianCalendar) object;
                return new GroovyLiteral(c.toXMLFormat(), XMLDatatypeUtil.qnameToURI(c
                        .getXMLSchemaType()));
            } else if (object instanceof Date) {
                final GregorianCalendar c = new GregorianCalendar();
                c.setTime((Date) object);
                final XMLGregorianCalendar xc = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
                return new GroovyLiteral(xc.toXMLFormat(), XMLDatatypeUtil.qnameToURI(xc
                        .getXMLSchemaType()));
            } else if (object instanceof CharSequence) {
                return new GroovyLiteral(object.toString(), XMLSchema.STRING);
            } else {
                return new GroovyLiteral(object.toString());
            }
        }
        return new GroovyURI(object.toString());
    }

    @Nullable
    private static Literal toLiteral(@Nullable final Object old, @Nullable final String label) {

        if (label == null) {
            return null;
        }

        String lang = null;
        URI dt = null;

        if (old instanceof Literal) {
            final Literal l = (Literal) old;
            lang = l.getLanguage();
            dt = l.getDatatype();
        }

        return lang == null ? new GroovyLiteral(label, lang) : dt != null ? new GroovyLiteral(
                label, dt) : new GroovyLiteral(label);
    }

    @Nullable
    private static GroovyStatement normalize(@Nullable final Statement s) {
        if (s instanceof GroovyStatement) {
            return (GroovyStatement) s;
        } else if (s != null) {
            return new GroovyStatement((Resource) normalize(s.getSubject()),
                    (URI) normalize(s.getPredicate()), normalize(s.getObject()),
                    (Resource) normalize(s.getContext()));
        }
        return null;
    }

    @Nullable
    private static Value normalize(@Nullable final Value v) {
        if (v instanceof URI) {
            return v instanceof GroovyURI ? v : new GroovyURI(v.stringValue());
        } else if (v instanceof BNode) {
            return v instanceof GroovyBNode ? v : new GroovyBNode(v.stringValue());
        } else if (v instanceof Literal) {
            if (v instanceof GroovyLiteral) {
                return v;
            }
            final Literal l = (Literal) v;
            if (l.getLanguage() != null) {
                return new GroovyLiteral(l.getLabel(), l.getLanguage());
            } else if (l.getDatatype() != null) {
                return new GroovyLiteral(l.getLabel(), l.getDatatype());
            } else {
                return new GroovyLiteral(l.getLabel());
            }
        }
        return null;
    }

    public static class GroovyStatement implements Statement {

        private static final long serialVersionUID = 1L;

        @Nullable
        public Resource s;

        @Nullable
        public URI p;

        @Nullable
        public Value o;

        @Nullable
        public Resource c;

        public GroovyStatement(final Resource s, final URI p, final Value o,
                @Nullable final Resource c) {
            this.s = s;
            this.p = p;
            this.o = o;
            this.c = c;
        }

        public boolean isValid() {
            return this.s != null && this.p != null && this.o != null;
        }

        @Override
        public Resource getSubject() {
            return this.s;
        }

        @Override
        public URI getPredicate() {
            return this.p;
        }

        @Override
        public Value getObject() {
            return this.o;
        }

        @Override
        public Resource getContext() {
            return this.c;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            }
            if (object instanceof Statement) {
                final Statement other = (Statement) object;
                return this.o.equals(other.getObject()) && this.s.equals(other.getSubject())
                        && this.p.equals(other.getPredicate());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return 961 * this.s.hashCode() + 31 * this.p.hashCode() + this.o.hashCode();
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder(256);
            builder.append('(');
            builder.append(this.s);
            builder.append(", ");
            builder.append(this.p);
            builder.append(", ");
            builder.append(this.o);
            if (this.c != null) {
                builder.append(" [");
                builder.append(this.c);
                builder.append("]");
            }
            return builder.toString();
        }

    }

    public static final class GroovyURI extends URIImpl implements Comparable<Value> {

        private static final long serialVersionUID = 1L;

        public GroovyURI(final String uriString) {
            super(uriString);
        }

        @Override
        public int compareTo(final Value other) {
            if (other instanceof URI) {
                return stringValue().compareTo(other.stringValue());
            }
            return -1;
        }

    }

    public static final class GroovyBNode extends BNodeImpl implements Comparable<Value> {

        private static final long serialVersionUID = 1L;

        public GroovyBNode(final String id) {
            super(id);
        }

        @Override
        public int compareTo(final Value other) {
            if (other instanceof BNode) {
                return stringValue().compareTo(other.stringValue());
            } else if (other instanceof URI) {
                return 1;
            } else {
                return -1;
            }
        }

    }

    public static final class GroovyLiteral extends LiteralImpl implements Comparable<Value> {

        private static final long serialVersionUID = 1L;

        GroovyLiteral(final String label, @Nullable final URI datatype) {
            super(label, datatype);
        }

        GroovyLiteral(final String label, @Nullable final String language) {
            super(label, language);
        }

        GroovyLiteral(final String label) {
            super(label);
        }

        @Override
        public int compareTo(final Value other) {
            if (other instanceof Literal) {
                int result = 0;
                if (other != this) {
                    final Literal l = (Literal) other;
                    result = getLabel().compareTo(l.getLabel());
                    if (result == 0) {
                        final String lang1 = getLanguage();
                        final String lang2 = l.getLanguage();
                        result = lang1 != null ? lang2 != null ? lang1.compareTo(lang2) : 1
                                : lang2 != null ? -1 : 0;
                        if (result == 0) {
                            final URI dt1 = getDatatype();
                            final URI dt2 = l.getDatatype();
                            result = dt1 != null ? dt2 != null ? dt1.stringValue().compareTo(
                                    dt2.stringValue()) : 1 : dt2 != null ? -1 : 0;
                        }
                    }
                }
                return result;
            }
            return 1;
        }

    }

    public static final class ValueSet {

        private final Set<String> hashSet;

        ValueSet(final Set<String> hashSet) {
            this.hashSet = hashSet;
        }

        public boolean match(final Object value) {
            if (value == null) {
                throw new IllegalArgumentException("value cannot be null.");
            }
            final Value target = value instanceof Value ? (Value) value : toRDF(value, true);
            return this.hashSet.contains(Util.valueToHash(target));
        }

        public boolean match(final Statement statement, final Object components) {
            final String parts = components.toString();
            if (parts.contains("s")) {
                if (this.hashSet.contains(Util.valueToHash(statement.getSubject()))) {
                    return true;
                }
            }
            if (parts.contains("p")) {
                if (this.hashSet.contains(Util.valueToHash(statement.getPredicate()))) {
                    return true;
                }
            }
            if (parts.contains("o")) {
                if (this.hashSet.contains(Util.valueToHash(statement.getObject()))) {
                    return true;
                }
            }
            if (parts.contains("c")) {
                final Value context = statement.getContext();
                if (context != null && this.hashSet.contains(Util.valueToHash(context))) {
                    return true;
                }
            }
            return false;
        }

    }

}