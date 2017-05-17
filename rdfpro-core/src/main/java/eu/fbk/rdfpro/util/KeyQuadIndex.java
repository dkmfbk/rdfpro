package eu.fbk.rdfpro.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.spotify.sparkey.CompressionType;
import com.spotify.sparkey.Sparkey;
import com.spotify.sparkey.SparkeyReader;
import com.spotify.sparkey.SparkeyWriter;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.Mapper;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.Reducer;

public final class KeyQuadIndex implements Closeable {

    private static final byte[] NS_KEY = new byte[] {};

    private static final int HI_END = 0;

    private static final int HI_END_S = 1 << 5;

    private static final int HI_END_P = 2 << 5;

    private static final int HI_END_C = 3 << 5;

    private static final int HI_URI = 4 << 5;

    private static final int HI_LITERAL = 5 << 5;

    private static final int HI_BNODE = 6 << 5;

    private static final int HI_NULL = 7 << 5;

    private static final BiMap<IRI, Integer> DT_MAP;

    private final SparkeyReader reader;

    private final Map<String, Integer> nsMap;

    private final String[] nsArray;

    static {
        final ImmutableBiMap.Builder<IRI, Integer> builder = ImmutableBiMap.builder();
        int index = 0;
        for (final IRI dt : new IRI[] { XMLSchema.DURATION, XMLSchema.DATETIME,
                XMLSchema.DAYTIMEDURATION, XMLSchema.TIME, XMLSchema.DATE, XMLSchema.GYEARMONTH,
                XMLSchema.GYEAR, XMLSchema.GMONTHDAY, XMLSchema.GDAY, XMLSchema.GMONTH,
                XMLSchema.STRING, XMLSchema.BOOLEAN, XMLSchema.BASE64BINARY, XMLSchema.HEXBINARY,
                XMLSchema.FLOAT, XMLSchema.DECIMAL, XMLSchema.DOUBLE, XMLSchema.ANYURI,
                XMLSchema.QNAME, XMLSchema.NOTATION, XMLSchema.NORMALIZEDSTRING, XMLSchema.TOKEN,
                XMLSchema.LANGUAGE, XMLSchema.NMTOKEN, XMLSchema.NMTOKENS, XMLSchema.NAME,
                XMLSchema.NCNAME, XMLSchema.ID, XMLSchema.IDREF, XMLSchema.IDREFS,
                XMLSchema.ENTITY, XMLSchema.ENTITIES, XMLSchema.INTEGER, XMLSchema.LONG,
                XMLSchema.INT, XMLSchema.SHORT, XMLSchema.BYTE, XMLSchema.NON_POSITIVE_INTEGER,
                XMLSchema.NEGATIVE_INTEGER, XMLSchema.NON_NEGATIVE_INTEGER,
                XMLSchema.POSITIVE_INTEGER, XMLSchema.UNSIGNED_LONG, XMLSchema.UNSIGNED_INT,
                XMLSchema.UNSIGNED_SHORT, XMLSchema.UNSIGNED_BYTE }) {
            builder.put(dt, index++);
        }
        DT_MAP = builder.build();
    }

    public KeyQuadIndex(final File file) {
        try {
            this.reader = Sparkey.open(file);
            this.nsArray = Splitter.on('\n')
                    .splitToList(new String(this.reader.getAsByteArray(NS_KEY), Charsets.UTF_8))
                    .toArray(new String[0]);
            this.nsMap = Maps.newHashMap();
            for (int i = 0; i < this.nsArray.length; ++i) {
                this.nsMap.put(this.nsArray[i], i);
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public QuadModel get(final Value key) {
        Objects.requireNonNull(key);
        final QuadModel model = QuadModel.create();
        get(key, model);
        return model;
    }

    public boolean get(final Value key, final RDFHandler handler) throws RDFHandlerException {
        try {
            final byte[] keyBytes = write(this.nsMap, new ByteArrayOutputStream(), key)
                    .toByteArray();
            final byte[] valueBytes;
            synchronized (this.reader) {
                valueBytes = this.reader.getAsByteArray(keyBytes);
            }
            if (valueBytes == null) {
                return false;
            } else {
                read(this.nsArray, new ByteArrayInputStream(valueBytes), handler);
                return true;
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public boolean get(final Value key, final Collection<Statement> model) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(model);
        try {
            return get(key, RDFHandlers.wrap(model));
        } catch (final RDFHandlerException ex) {
            throw new Error(ex);
        }
    }

    public QuadModel getAll(final Iterable<? extends Value> keys) {
        Objects.requireNonNull(keys);
        final QuadModel model = QuadModel.create();
        getAll(keys, model);
        return model;
    }

    public int getAll(final Iterable<? extends Value> keys, final RDFHandler handler)
            throws RDFHandlerException {
        Objects.requireNonNull(keys);
        Objects.requireNonNull(handler);
        int result = 0;
        for (final Value key : keys instanceof Set ? keys : ImmutableSet.copyOf(keys)) {
            final boolean found = get(key, handler);
            result += found ? 1 : 0;
        }
        return result;
    }

    public int getAll(final Iterable<? extends Value> keys, final Collection<Statement> model) {
        Objects.requireNonNull(keys);
        Objects.requireNonNull(model);
        try {
            return getAll(keys, RDFHandlers.wrap(model));
        } catch (final RDFHandlerException ex) {
            throw new Error(ex);
        }
    }

    public QuadModel getRecursive(final Iterable<? extends Value> keys,
            @Nullable final Predicate<Value> matcher) {
        Objects.requireNonNull(keys);
        final QuadModel model = QuadModel.create();
        getRecursive(keys, matcher, model);
        return model;
    }

    public int getRecursive(final Iterable<? extends Value> keys,
            @Nullable final Predicate<Value> matcher, final RDFHandler handler)
            throws RDFHandlerException {

        Objects.requireNonNull(keys);

        final Set<Value> visited = Sets.newHashSet();
        final List<Value> queue = Lists.newLinkedList(keys);

        final RDFHandler sink = new AbstractRDFHandlerWrapper(handler) {

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                super.handleStatement(stmt);
                enqueueIfMatches(stmt.getSubject());
                enqueueIfMatches(stmt.getPredicate());
                enqueueIfMatches(stmt.getObject());
                enqueueIfMatches(stmt.getContext());
            }

            private void enqueueIfMatches(@Nullable final Value value) {
                if (value != null && (matcher == null || matcher.test(value))) {
                    queue.add(value);
                }
            }

        };

        int result = 0;
        while (!queue.isEmpty()) {
            final Value key = queue.remove(0);
            if (visited.add(key)) {
                final boolean found = get(key, sink);
                result += found ? 1 : 0;
            }
        }
        return result;
    }

    public int getRecursive(final Iterable<? extends Value> keys,
            @Nullable final Predicate<Value> matcher, final Collection<Statement> model) {
        Objects.requireNonNull(keys);
        Objects.requireNonNull(model);
        try {
            return getRecursive(keys, matcher, RDFHandlers.wrap(model));
        } catch (final RDFHandlerException ex) {
            throw new Error(ex);
        }
    }

    @Override
    public void close() {
        this.reader.close();
    }

    @Nullable
    private static Value read(final String[] nsArray, final ByteArrayInputStream stream) {

        try {
            final ValueFactory vf = Statements.VALUE_FACTORY;
            final int b = stream.read();
            final int hi = b & 0xE0;

            if (hi == HI_NULL) {
                return null;

            } else if (hi == HI_BNODE) {
                final byte[] id = new byte[(b & 0x1F) << 8 | stream.read()];
                ByteStreams.readFully(stream, id);
                return vf.createBNode(new String(id, Charsets.UTF_8));

            } else if (hi == HI_URI) {
                if ((b & 0x10) != 0) {
                    final byte[] name = new byte[(b & 0xF) << 8 | stream.read()];
                    final String ns = nsArray[stream.read()];
                    ByteStreams.readFully(stream, name);
                    return vf.createIRI(ns, new String(name, Charsets.UTF_8));
                } else {
                    final byte[] str = new byte[(b & 0xF) << 8 | stream.read()];
                    ByteStreams.readFully(stream, str);
                    return vf.createIRI(new String(str, Charsets.UTF_8));
                }

            } else if (hi == HI_LITERAL) {
                byte[] lang = null;
                IRI dt = null;
                if ((b & 0x10) != 0) {
                    lang = new byte[b & 0xF];
                    ByteStreams.readFully(stream, lang);
                } else if ((b & 0x1) != 0) {
                    dt = DT_MAP.inverse().get(stream.read());
                } else if ((b & 0x2) != 0) {
                    dt = (IRI) read(nsArray, stream);
                }
                final byte[] label = new byte[stream.read() << 16 | stream.read() << 8
                        | stream.read()];
                ByteStreams.readFully(stream, label);
                final String labelStr = new String(label, Charsets.UTF_8);
                if (lang != null) {
                    return vf.createLiteral(labelStr, new String(lang, Charsets.UTF_8));
                } else if (dt != null) {
                    return vf.createLiteral(labelStr, dt);
                } else {
                    return vf.createLiteral(labelStr);
                }

            } else {
                throw new Error("Invalid marker: " + b);
            }

        } catch (final IOException ex) {
            throw new Error(ex);
        }
    }

    private static ByteArrayOutputStream write(final Map<String, Integer> nsMap,
            final ByteArrayOutputStream stream, final Value value) {

        if (value == null) {
            stream.write(HI_NULL);

        } else if (value instanceof BNode) {
            final byte[] id = ((BNode) value).getID().getBytes(Charsets.UTF_8);
            Preconditions.checkArgument(id.length <= 0x1FFF);
            stream.write(HI_BNODE | id.length >>> 8);
            stream.write(id.length & 0xFF);
            stream.write(id, 0, id.length);

        } else if (value instanceof IRI) {
            final IRI uri = (IRI) value;
            final Integer nsID = nsMap.get(uri.getNamespace());
            if (nsID != null && nsID <= 0xFF) {
                final byte[] name = uri.getLocalName().getBytes(Charsets.UTF_8);
                Preconditions.checkArgument(name.length <= 0xFFF);
                stream.write(HI_URI | 0x10 | name.length >>> 8);
                stream.write(name.length & 0xFF);
                stream.write(nsID);
                stream.write(name, 0, name.length);
            } else {
                final byte[] str = uri.stringValue().getBytes(Charsets.UTF_8);
                Preconditions.checkArgument(str.length <= 0xFFF);
                stream.write(HI_URI | str.length >>> 8);
                stream.write(str.length & 0xFF);
                stream.write(str, 0, str.length);
            }

        } else if (value instanceof Literal) {
            final Literal lit = (Literal) value;
            if (lit.getLanguage().isPresent()) {
                final byte[] lang = lit.getLanguage().get().getBytes(Charsets.UTF_8);
                Preconditions.checkArgument(lang.length <= 0x0F);
                stream.write(HI_LITERAL | 0x10 | lang.length);
                stream.write(lang, 0, lang.length);
            } else if (lit.getDatatype() == null || lit.getDatatype().equals(XMLSchema.STRING)) {
                stream.write(HI_LITERAL);
            } else {
                final Integer dtID = DT_MAP.get(lit.getDatatype());
                if (dtID != null) {
                    stream.write(HI_LITERAL | 0x01);
                    stream.write(dtID);
                } else {
                    stream.write(HI_LITERAL | 0x2);
                    write(nsMap, stream, lit.getDatatype());
                }
            }
            final byte[] label = lit.getLabel().getBytes(Charsets.UTF_8);
            Preconditions.checkArgument(label.length < 0xFFFFFF);
            stream.write(label.length >>> 16);
            stream.write(label.length >>> 8 & 0xFF);
            stream.write(label.length & 0xFF);
            stream.write(label, 0, label.length);
        }

        return stream;
    }

    private static void read(final String[] nsArray, final ByteArrayInputStream stream,
            final RDFHandler handler) throws RDFHandlerException {
        final Value[] values = new Value[4];
        int index = 0;
        while (true) {
            stream.mark(1);
            final int hi = stream.read() & 0xFF;
            if (hi == HI_END) {
                break;
            } else if (hi == HI_END_C) {
                index = 0;
            } else if (hi == HI_END_S) {
                index = 1;
            } else if (hi == HI_END_P) {
                index = 2;
            } else {
                stream.reset();
            }
            values[index++] = read(nsArray, stream);
            if (index == 4) {
                handler.handleStatement(Statements.VALUE_FACTORY.createStatement(
                        (Resource) values[1], (IRI) values[2], values[3], (Resource) values[0]));
                --index;
            }
        }
    }

    private static ByteArrayOutputStream write(final Map<String, Integer> nsMap,
            final ByteArrayOutputStream stream, final Iterable<Statement> stmts) {
        Statement lastStmt = null;
        for (final Statement stmt : Ordering
                .from(Statements.statementComparator("cspo", Statements.valueComparator()))
                .sortedCopy(stmts)) {
            final boolean sameC = lastStmt != null
                    && Objects.equals(lastStmt.getContext(), stmt.getContext());
            final boolean sameS = sameC && lastStmt.getSubject().equals(stmt.getSubject());
            final boolean sameP = sameS && lastStmt.getPredicate().equals(stmt.getPredicate());
            if (sameP) {
                write(nsMap, stream, stmt.getObject());
            } else if (sameS) {
                stream.write(HI_END_P);
                write(nsMap, stream, stmt.getPredicate());
                write(nsMap, stream, stmt.getObject());
            } else if (sameC) {
                stream.write(HI_END_S);
                write(nsMap, stream, stmt.getSubject());
                write(nsMap, stream, stmt.getPredicate());
                write(nsMap, stream, stmt.getObject());
            } else {
                if (lastStmt != null) {
                    stream.write(HI_END_C);
                }
                write(nsMap, stream, stmt.getContext());
                write(nsMap, stream, stmt.getSubject());
                write(nsMap, stream, stmt.getPredicate());
                write(nsMap, stream, stmt.getObject());
            }
            lastStmt = stmt;
        }
        stream.write(HI_END);
        return stream;
    }

    public static RDFHandler indexer(final File file, final Mapper mapper) {

        Objects.requireNonNull(file);
        Objects.requireNonNull(mapper);

        final Map<String, Integer> nsMap = Maps.newHashMap();

        final AtomicReference<SparkeyWriter> writerRef = new AtomicReference<SparkeyWriter>();
        final Reducer reducer = new Reducer() {

            @Override
            public void reduce(final Value key, final Statement[] stmts, final RDFHandler handler)
                    throws RDFHandlerException {
                final byte[] keyBytes = write(nsMap, new ByteArrayOutputStream(), key)
                        .toByteArray();
                final byte[] stmtsBytes = write(nsMap, new ByteArrayOutputStream(),
                        Arrays.asList(stmts)).toByteArray();
                synchronized (writerRef) {
                    try {
                        writerRef.get().put(keyBytes, stmtsBytes);
                    } catch (final Throwable ex) {
                        throw new RDFHandlerException(ex);
                    }
                }
            }

        };

        final RDFHandler mrHandler = RDFProcessors.mapReduce(mapper, reducer, true)
                .wrap(RDFHandlers.NIL);

        return new AbstractRDFHandlerWrapper(mrHandler) {

            private final Set<String> namespaces = Sets.newConcurrentHashSet();

            @Override
            public void startRDF() throws RDFHandlerException {
                super.startRDF();
            }

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                this.namespaces.add(stmt.getPredicate().getNamespace());
                if (stmt.getSubject() instanceof IRI) {
                    this.namespaces.add(((IRI) stmt.getSubject()).getNamespace());
                }
                if (stmt.getObject() instanceof IRI) {
                    this.namespaces.add(((IRI) stmt.getObject()).getNamespace());
                }
                if (stmt.getContext() instanceof IRI) {
                    this.namespaces.add(((IRI) stmt.getContext()).getNamespace());
                }
                super.handleStatement(stmt);
            }

            @Override
            public void endRDF() throws RDFHandlerException {

                int counter = 0;
                final List<String> nsList = Ordering.natural()
                        .immutableSortedCopy(this.namespaces);
                for (final String namespace : nsList) {
                    nsMap.put(namespace, counter++);
                }

                try {
                    writerRef.set(Sparkey.createNew(file, CompressionType.SNAPPY, 4096));
                    writerRef.get().put(NS_KEY,
                            Joiner.on('\n').join(nsList).getBytes(Charsets.UTF_8));

                    super.endRDF();

                    writerRef.get().flush();
                    writerRef.get().writeHash();
                    writerRef.get().close();
                } catch (final IOException ex) {
                    throw new RDFHandlerException(ex);
                }
            }

        };
    }

}
