/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2015 by Francesco Corcoglioniti with support by Alessio Palmero Aprosio and Marco
 * Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandler;

// codes from 1 to 0xEFFFFFFF denote indexed values (15 GB indexable)
// - from 1 to 0xBFFFFFFF primary buffer can be used for values long less than 128 bytes (12 GB)
// - from 0xC0000000 to 0xEFFFFFFF secondary buffer always used (3 GB, 384M values encodable)
// codes from 0xF0000000 to 0xFFFFFFFF denote embedded values

public abstract class Dictionary implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Dictionary.class);

    private static final int[] PACKING_TYPES;

    private static final SequentialDictionary<IRI> INITIAL_DATATYPE_INDEX;

    private static final int XSD_STRING_INDEX;

    static final IRI XSD_STRING = Statements.normalize(XMLSchema.STRING);

    static final int NULL_CODE = 0xE0000000;

    static final int TYPE_IRI_FULL = 0;

    static final int TYPE_IRI_NAME = 1;

    static final int TYPE_BNODE = 2;

    static final int TYPE_LITERAL_PLAIN = 4;

    static final int TYPE_LITERAL_LANG = 5;

    static final int TYPE_LITERAL_DT = 6;

    final static int PACKING_BIGDECIMAL = 1;

    final static int PACKING_BIGINTEGER = 2;

    final static int PACKING_LONG = 3;

    final static int PACKING_DOUBLE = 4;

    final static int PACKING_BOOLEAN = 5;

    final static int PACKING_DATETIME = 6;

    static {
        final int[] t = new int[26]; // 25 + first slot unused
        final SequentialDictionary<IRI> i = new SequentialDictionary<>(64 * 1024 - 2);
        t[i.encode(Statements.normalize(XMLSchema.DECIMAL))] = Dictionary.PACKING_BIGDECIMAL;
        t[i.encode(Statements.normalize(XMLSchema.INTEGER))] = Dictionary.PACKING_BIGINTEGER;
        t[i.encode(Statements
                .normalize(XMLSchema.NON_POSITIVE_INTEGER))] = Dictionary.PACKING_BIGINTEGER;
        t[i.encode(
                Statements.normalize(XMLSchema.NEGATIVE_INTEGER))] = Dictionary.PACKING_BIGINTEGER;
        t[i.encode(Statements
                .normalize(XMLSchema.NON_NEGATIVE_INTEGER))] = Dictionary.PACKING_BIGINTEGER;
        t[i.encode(
                Statements.normalize(XMLSchema.POSITIVE_INTEGER))] = Dictionary.PACKING_BIGINTEGER;
        t[i.encode(Statements.normalize(XMLSchema.LONG))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.INT))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.SHORT))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.BYTE))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.UNSIGNED_LONG))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.UNSIGNED_INT))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.UNSIGNED_SHORT))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.UNSIGNED_BYTE))] = Dictionary.PACKING_LONG;
        t[i.encode(Statements.normalize(XMLSchema.DOUBLE))] = Dictionary.PACKING_DOUBLE;
        t[i.encode(Statements.normalize(XMLSchema.FLOAT))] = Dictionary.PACKING_DOUBLE;
        t[i.encode(Statements.normalize(XMLSchema.BOOLEAN))] = Dictionary.PACKING_BOOLEAN;
        t[i.encode(Statements.normalize(XMLSchema.DATETIME))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.DATE))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.TIME))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.GYEARMONTH))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.GMONTHDAY))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.GYEAR))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.GMONTH))] = Dictionary.PACKING_DATETIME;
        t[i.encode(Statements.normalize(XMLSchema.GDAY))] = Dictionary.PACKING_DATETIME;

        PACKING_TYPES = t;
        INITIAL_DATATYPE_INDEX = i;
        XSD_STRING_INDEX = i.encode(Statements.normalize(XMLSchema.STRING));

        // Non packable types:
        // XMLSchema.DURATION XMLSchema.DAYTIMEDURATION XMLSchema.STRING XMLSchema.BASE64BINARY
        // XMLSchema.HEXBINARY XMLSchema.ANYURI XMLSchema.QNAME XMLSchema.NOTATION
        // XMLSchema.NORMALIZEDSTRING XMLSchema.TOKEN XMLSchema.LANGUAGE XMLSchema.NMTOKEN
        // XMLSchema.NMTOKENS XMLSchema.NAME XMLSchema.NCNAME XMLSchema.ID XMLSchema.IDREF
        // XMLSchema.IDREFS XMLSchema.ENTITY XMLSchema.ENTITIES
    }

    private final SequentialDictionary<String> namespaceIndex;

    private final SequentialDictionary<String> languageIndex;

    private final SequentialDictionary<IRI> datatypeIndex;

    private final EncodeCacheEntry[] encodeCache;

    private final DecodeCacheEntry[] decodeCache;

    private final AtomicLong encodeEmbeddedCounter;

    private final AtomicLong encodeCacheCounter;

    private final AtomicLong encodeIndexedCounter;

    private final AtomicLong decodeEmbeddedCounter;

    private final AtomicLong decodeCacheCounter;

    private final AtomicLong decodeIndexedCounter;

    Dictionary() {
        this.namespaceIndex = new SequentialDictionary<>(64 * 1024 - 2);
        this.languageIndex = new SequentialDictionary<>(64 * 1024 - 2);
        this.datatypeIndex = new SequentialDictionary<>(Dictionary.INITIAL_DATATYPE_INDEX);
        this.encodeCache = new EncodeCacheEntry[1024 - 1];
        this.decodeCache = new DecodeCacheEntry[1024 - 1];
        this.encodeEmbeddedCounter = new AtomicLong();
        this.encodeCacheCounter = new AtomicLong();
        this.encodeIndexedCounter = new AtomicLong();
        this.decodeEmbeddedCounter = new AtomicLong();
        this.decodeCacheCounter = new AtomicLong();
        this.decodeIndexedCounter = new AtomicLong();
    }

    abstract int doEncode(int type, int index, String string, boolean allocate);

    abstract Value doDecode(int code);

    int doType(final int code) {
        final Value value = this.decode(code);
        if (value instanceof BNode) {
            return Dictionary.TYPE_BNODE;
        } else if (value instanceof IRI) {
            return ((IRI) value).getLocalName().isEmpty() ? Dictionary.TYPE_IRI_FULL
                    : Dictionary.TYPE_IRI_NAME;
        } else {
            final Literal lit = (Literal) value;
            return lit.getLanguage().isPresent() ? Dictionary.TYPE_LITERAL_LANG
                    : lit.getDatatype() != null && !lit.getDatatype().equals(Dictionary.XSD_STRING)
                            ? Dictionary.TYPE_LITERAL_DT : Dictionary.TYPE_LITERAL_PLAIN;
        }
    }

    void doToString(final StringBuilder builder) {
    }

    void doClose() {
    }

    final Value valueFor(final int type, final int index, final String string) {

        final ValueFactory vf = Statements.VALUE_FACTORY;
        switch (type) {
        case TYPE_IRI_FULL:
            return vf.createIRI(string);
        case TYPE_IRI_NAME:
            return vf.createIRI(this.namespaceIndex.decode(index), string);
        case TYPE_BNODE:
            return vf.createBNode(string);
        case TYPE_LITERAL_PLAIN:
            return vf.createLiteral(string);
        case TYPE_LITERAL_DT:
            return vf.createLiteral(string, this.datatypeIndex.decode(index));
        case TYPE_LITERAL_LANG:
            return vf.createLiteral(string, this.languageIndex.decode(index));
        default:
            throw new Error();
        }
    }

    public static Dictionary newMemoryDictionary() {
        return new MemoryDictionary();
    }

    public final RDFHandler encode(final QuadHandler sink) {
        return new AbstractRDFHandler() {

            @Override
            public void startRDF() throws RDFHandlerException {
                sink.start();
            }

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                final int subj = Dictionary.this.encode(stmt.getSubject());
                final int pred = Dictionary.this.encode(stmt.getPredicate());
                final int obj = Dictionary.this.encode(stmt.getObject());
                final int ctx = Dictionary.this.encode(stmt.getContext());
                sink.handle(subj, pred, obj, ctx);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                sink.end();
            }

            @Override
            public void close() {
                sink.close();
            }

        };
    }

    void encodeCachePut(@Nullable final Value value, final int code) {
        final int hash = value.hashCode() & 0x7FFFFFFF;
        final int slotIndex = hash % this.encodeCache.length;
        final EncodeCacheEntry entry = new EncodeCacheEntry(code, hash, value);
        this.encodeCache[slotIndex] = entry;
    }

    public final int encode(@Nullable final Value value) {
        return encode(value, true);
    }

    public final int encode(@Nullable final Value value, final boolean allocate) {

        // Handle default context
        if (value == null) {
            return Dictionary.NULL_CODE;
        }

        // Handle literals whose value can be fully packed in the code
        int index = 0;
        if (value instanceof Literal) {
            final Literal l = (Literal) value;
            if (!l.getLanguage().isPresent()) {
                index = this.datatypeIndex.encode(l.getDatatype());
                if (index < Dictionary.PACKING_TYPES.length) {
                    final int v = Dictionary.pack(Dictionary.PACKING_TYPES[index], l);
                    if (v >= 0) {
                        this.encodeEmbeddedCounter.incrementAndGet();
                        return 0xE0000000 | index << 24 | v;
                    }
                }
            }
        }

        // Lookup a pre-computed code in the cache
        final int hash = value.hashCode();
        final int slotIndex = (hash & 0x7FFFFFFF) % this.encodeCache.length;
        final EncodeCacheEntry entry = this.encodeCache[slotIndex];
        if (entry != null && entry.hash == hash && entry.value.equals(value)) {
            this.encodeCacheCounter.incrementAndGet();
            return entry.code;
        }

        // On cache miss, split the value in type + index + string (datatype index reused)
        int type;
        String string;
        if (value instanceof Literal) {
            final Literal l = (Literal) value;
            string = l.getLabel();
            if (l.getLanguage().isPresent()) {
                type = Dictionary.TYPE_LITERAL_LANG;
                index = this.languageIndex.encode(l.getLanguage().get());
            } else if (index != Dictionary.XSD_STRING_INDEX) {
                type = Dictionary.TYPE_LITERAL_DT;
            } else {
                type = Dictionary.TYPE_LITERAL_PLAIN;
                index = 0;
            }
        } else if (value instanceof IRI) {
            final IRI u = (IRI) value;
            index = this.namespaceIndex.encode(u.getNamespace(), 0);
            if (index > 0) {
                type = Dictionary.TYPE_IRI_NAME;
                string = u.getLocalName();
            } else {
                type = Dictionary.TYPE_IRI_FULL;
                string = u.stringValue();
            }
        } else if (value instanceof BNode) {
            type = Dictionary.TYPE_BNODE;
            string = ((BNode) value).getID();
        } else {
            throw new Error();
        }

        // Delegate and cache the result
        final int code = doEncode(type, index, string, allocate);
        if (code != 0) {
            this.encodeCache[slotIndex] = new EncodeCacheEntry(code, hash, value);
            this.encodeIndexedCounter.incrementAndGet();
        }
        return code;
    }

    public final QuadHandler decode(final RDFHandler sink) {
        return new QuadHandler() {

            @Override
            public void start() throws RDFHandlerException {
                sink.startRDF();
            }

            @Override
            public void handle(final int subj, final int pred, final int obj, final int ctx)
                    throws RDFHandlerException {
                final Resource s = (Resource) Dictionary.this.decode(subj);
                final IRI p = (IRI) Dictionary.this.decode(pred);
                final Value o = Dictionary.this.decode(obj);
                final Resource c = (Resource) Dictionary.this.decode(ctx);
                sink.handleStatement(Statements.VALUE_FACTORY.createStatement(s, p, o, c));
            }

            @Override
            public void end() throws RDFHandlerException {
                sink.endRDF();
            }

            @Override
            public void close() {
                IO.closeQuietly(sink);
            }

        };
    }

    @Nullable
    public final Value decode(final int code) {

        // Handle default context
        if (code == Dictionary.NULL_CODE) {
            return null;
        }

        // Handle packed values
        if (code >>> 29 == 0x7) {
            final int index = (code & 0x1F000000) >>> 24;
            final IRI datatype = this.datatypeIndex.decode(index);
            final String label = Dictionary.unpack(Dictionary.PACKING_TYPES[index],
                    code & 0xFFFFFF);
            final Value value = Statements.VALUE_FACTORY.createLiteral(label, datatype);
            this.decodeEmbeddedCounter.incrementAndGet();
            return value;
        }

        // Lookup the code in the cache
        final int hash = Dictionary.hashInt(code);
        final int slotIndex = (hash & 0x7FFFFFFF) % this.decodeCache.length;
        final DecodeCacheEntry entry = this.decodeCache[slotIndex];
        if (entry != null && entry.code == code) {
            this.decodeCacheCounter.incrementAndGet();
            return entry.value;
        }

        // Delegate and cache the result
        final Value value = doDecode(code);
        this.decodeCache[slotIndex] = new DecodeCacheEntry(code, value);
        this.decodeIndexedCounter.incrementAndGet();
        return value;
    }

    public final boolean isNull(final int code) {
        return code == Dictionary.NULL_CODE;
    }

    public final boolean isResource(final int code) {
        return doType(code) <= Dictionary.TYPE_BNODE;
    }

    public final boolean isIRI(final int code) {
        return doType(code) <= Dictionary.TYPE_IRI_NAME;
    }

    public final boolean isBNode(final int code) {
        return doType(code) == Dictionary.TYPE_BNODE;
    }

    public final boolean isLiteral(final int code) {
        return doType(code) >= Dictionary.TYPE_LITERAL_PLAIN;
    }

    @Override
    public final void close() {
        doClose();
    }

    @Override
    public final String toString() {
        final StringBuilder builder = new StringBuilder(this.getClass().getSimpleName())
                .append(": ");
        final int oldLength = builder.length();
        doToString(builder);
        builder.append(builder.length() > oldLength ? ", " : "");
        builder.append(this.namespaceIndex.size()).append(" namespaces, ")
                .append(this.datatypeIndex.size()).append(" datatypes, ")
                .append(this.languageIndex.size()).append(" languages, ");
        toStringHelper(builder, "encode", this.encodeEmbeddedCounter.get(),
                this.encodeCacheCounter.get(), this.encodeIndexedCounter.get());
        builder.append(", ");
        toStringHelper(builder, "decode", this.decodeEmbeddedCounter.get(),
                this.decodeCacheCounter.get(), this.decodeIndexedCounter.get());
        return builder.toString();
    }

    private void toStringHelper(final StringBuilder builder, final String type,
            final long embedded, final long cached, final long indexed) {
        final long total = embedded + cached + indexed;
        builder.append(total).append(" ").append(type).append(" calls (").append(embedded)
                .append(" embedded, ").append(cached).append(" cached, ").append(indexed)
                .append(" indexed)");
    }

    private static int pack(final int packingType, final Literal literal) {

        switch (packingType) {
        case PACKING_BIGDECIMAL:
            final BigDecimal bd = literal.decimalValue();
            final int scale = bd.scale();
            if (scale < 1 << 4 && scale > -(1 << 4)) {
                final BigInteger unscaled = bd.unscaledValue();
                if (unscaled.bitLength() <= 18) {
                    return scale + (1 << 4) << 19 | unscaled.intValue() + (1 << 18);
                }
            }
            break;

        case PACKING_BIGINTEGER:
            final BigInteger bi = literal.integerValue();
            if (bi.bitLength() <= 23) {
                return bi.intValue() + (1 << 23);
            }
            break;

        case PACKING_LONG:
            final long l = literal.longValue();
            if (l < 1 << 23 && l >= -(1 << 23)) {
                return (int) l + (1 << 23);
            }
            break;

        case PACKING_DOUBLE:
            final long bits = Double.doubleToLongBits(literal.doubleValue());
            if ((bits & 0xFFFFFFFFFFL) == 0L) {
                return (int) (bits >>> 40);
            }
            break;

        case PACKING_BOOLEAN:
            return literal.booleanValue() ? 1 : 0;

        case PACKING_DATETIME:
            final XMLGregorianCalendar c = literal.calendarValue();
            if (c.getTimezone() == DatatypeConstants.FIELD_UNDEFINED
                    && c.getMillisecond() == DatatypeConstants.FIELD_UNDEFINED
                    && c.getSecond() == DatatypeConstants.FIELD_UNDEFINED
                    && c.getMinute() == DatatypeConstants.FIELD_UNDEFINED
                    && c.getHour() == DatatypeConstants.FIELD_UNDEFINED) {
                final int year = c.getYear();
                final int month = c.getMonth();
                final int day = c.getDay();
                if (year == DatatypeConstants.FIELD_UNDEFINED
                        || year < 1 << 14 && year > -(1 << 14)) {
                    return (day != DatatypeConstants.FIELD_UNDEFINED ? day : 0)
                            | (month != DatatypeConstants.FIELD_UNDEFINED ? month : 0) << 5
                            | (year != DatatypeConstants.FIELD_UNDEFINED ? year + (1 << 14)
                                    : 0) << 5 + 4;
                }
            }
            break;
        }

        // Signal failure
        return -1;
    }

    private static String unpack(final int packingType, final int value) {

        switch (packingType) {
        case PACKING_BIGDECIMAL:
            final int scale = (value >> 19 & 0x1F) - (1 << 4);
            final int unscaled = (value & 0x7FFFF) - (1 << 18);
            return new BigDecimal(BigInteger.valueOf(unscaled), scale).toString();

        case PACKING_BIGINTEGER:
            return Integer.toString((value & 0xFFFFFF) - (1 << 23));

        case PACKING_LONG:
            return Integer.toString((value & 0xFFFFFF) - (1 << 23));

        case PACKING_DOUBLE:
            return Double.toString(Double.longBitsToDouble((long) (value & 0xFFFFFF) << 40));

        case PACKING_BOOLEAN:
            return (value & 0x1) != 0 ? "true" : "false";

        case PACKING_DATETIME:
            final XMLGregorianCalendar c = Statements.DATATYPE_FACTORY.newXMLGregorianCalendar();
            final int year = value >>> 5 + 4 & 0x7FFF;
            final int month = value >>> 5 & 0x0F;
            final int day = value & 0x1F;
            c.setDay(day != 0 ? day : DatatypeConstants.FIELD_UNDEFINED);
            c.setMonth(month != 0 ? month : DatatypeConstants.FIELD_UNDEFINED);
            c.setYear(year != 0 ? year - (1 << 14) : DatatypeConstants.FIELD_UNDEFINED);
            return c.toXMLFormat();

        default:
            throw new Error("Unexpected type " + packingType);
        }
    }

    private static int hashInt(final int value) {
        int x = value;
        x = (x >>> 16 ^ x) * 0x45d9f3b;
        x = (x >>> 16 ^ x) * 0x45d9f3b;
        x = x >>> 16 ^ x;
        return x;
    }

    public interface QuadHandler extends AutoCloseable {

        final QuadHandler NIL = new QuadHandler() {

            @Override
            public void handle(final int subj, final int pred, final int obj, final int ctx) {
            }

        };

        default void start() throws RDFHandlerException {
        }

        void handle(int subj, int pred, int obj, int ctx) throws RDFHandlerException;

        default void end() throws RDFHandlerException {
        }

        @Override
        default void close() {
        }

    }

    private static final class EncodeCacheEntry {

        final int code;

        final int hash;

        final Value value;

        EncodeCacheEntry(final int code, final int hash, final Value value) {
            this.code = code;
            this.hash = hash;
            this.value = value;
        }

    }

    private static final class DecodeCacheEntry {

        final int code;

        final Value value;

        DecodeCacheEntry(final int code, final Value value) {
            this.code = code;
            this.value = value;
        }

    }

    private static final class SequentialDictionary<T> {

        private final int capacity;

        private Object[] array;

        private long[] table;

        private int size;

        SequentialDictionary(final int capacity) {
            this.capacity = capacity;
            this.array = new Object[Math.min(capacity + 1, 8)];
            this.table = new long[8];
            this.size = 0;
        }

        SequentialDictionary(final SequentialDictionary<T> index) {
            this.capacity = index.capacity;
            this.array = index.array.clone();
            this.table = index.table.clone();
            this.size = index.size;
        }

        int size() {
            return this.size;
        }

        int encode(final T element, final int resultIfFull) {
            final long[] table = this.table; // may change on rehash
            Object[] array = this.array; // may change on rehash
            final int mask = table.length - 1;
            final int hash = element.hashCode();
            for (int slot = hash & mask;; slot = slot + 1 & mask) {
                final long cell = table[slot];
                if (cell == 0L) {
                    synchronized (this) {
                        if (table == this.table && table[slot] == 0L) {
                            array = this.array; // may have changed in the meanwhile
                            if (this.size == this.capacity) {
                                return resultIfFull;
                            }
                            if (this.size > table.length / 3 * 2) {
                                this.rehash(); // enforce load factor < .66
                            } else {
                                final int index = this.size + 1; // Skip index 0
                                if (index >= array.length) {
                                    array = Arrays.copyOf(array, array.length << 1);
                                    this.array = array;
                                }
                                array[index] = element;
                                table[slot] = (long) index << 32 | hash & 0xFFFFFFFFL;
                                ++this.size;
                                return index;
                            }
                        }
                    }
                    return this.encode(element, resultIfFull); // retry on rehash and concurrent
                                                               // change
                }
                if ((int) cell == hash) {
                    final int index = (int) (cell >>> 32);
                    if (index >= array.length) {
                        return this.encode(element, resultIfFull); // array not aligned to table:
                                                                   // retry
                    }
                    if (element.equals(array[index])) {
                        return index;
                    }
                }
            }
        }

        int encode(final T element) {
            final int index = this.encode(element, -1);
            if (index < 0) {
                throw new IllegalStateException("Full index");
            }
            return index;
        }

        @SuppressWarnings("unchecked")
        T decode(final int index) {
            return (T) this.array[index];
        }

        private void rehash() {
            final long[] oldTable = this.table;
            final long[] newTable = new long[oldTable.length << 1];
            final int newMask = newTable.length - 1;
            for (int oldSlot = 0; oldSlot < oldTable.length; ++oldSlot) {
                final long cell = oldTable[oldSlot];
                if (cell != 0L) {
                    final int hash = (int) cell;
                    int newSlot = hash & newMask;
                    while (newTable[newSlot] != 0L) {
                        newSlot = newSlot + 1 & newMask;
                    }
                    newTable[newSlot] = cell;
                }
            }
            this.table = newTable;
        }

    }

    // TODO
    // - optimize memory layout
    // - encode bigrams in strings
    // - rehashing simultaneous to encoding

    private static final class MemoryDictionary extends Dictionary {

        private final Buffer primaryBuffer;

        private final Buffer secondaryBuffer;

        private long primaryOffset;

        private long secondaryOffset;

        private long[] table;

        private int size;

        private final Object[] locks;

        MemoryDictionary() {
            this.primaryBuffer = Buffer.newResizableBuffer();
            this.secondaryBuffer = Buffer.newResizableBuffer();
            this.primaryOffset = 0L;
            this.secondaryOffset = 0L;
            this.table = new long[512]; // 4K
            this.size = 0;
            this.locks = new Object[Environment.getCores() * 32];
            for (int i = 0; i < this.locks.length; ++i) {
                this.locks[i] = new Object();
            }

            // Write dummy byte just to avoid starting at offset 0
            this.primaryBuffer.write(0, (byte) 0);
            ++this.primaryOffset;
        }

        @Override
        int doEncode(final int type, final int index, final String string,
                final boolean allocate) {

            final int hash = type * 6661 + index * 661 + string.hashCode() & 0x7FFFFFFF;

            final long[] table = this.table; // may concurrently change
            final int mask = table.length - 1;

            for (int slot = hash & mask;; slot = slot + 1 & mask) {
                final long cell = table[slot];
                if (cell == 0L) {
                    if (!allocate) {
                        return 0;
                    }
                    final Buffer buffer = Buffer.newFixedBuffer(new byte[string.length() * 3 + 6]);
                    final int bufferLength = buffer.writeString(4, string);
                    synchronized (this) {
                        if (table == this.table && table[slot] == 0L) {
                            if (this.size > this.table.length / 3 * 2) {
                                rehash(); // enforce load factor < .66
                            } else {
                                final long offset = append(type, index, buffer, 4, bufferLength);
                                final int code = MemoryDictionary.offsetToCode(offset);
                                table[slot] = (long) code << 32 | hash & 0xFFFFFFFFL;
                                ++this.size;
                                return code;
                            }
                        }
                    }
                    return doEncode(type, index, string, allocate); // retry on rehash and
                                                                    // concurrent change
                }
                if ((int) cell == hash) {
                    final int code = (int) (cell >>> 32);
                    final long offset = MemoryDictionary.codeToOffset(code);
                    if (this.equals(offset, type, index, string)) {
                        return code;
                    }
                }
            }
        }

        @SuppressWarnings("unused")
        int doEncode2(final int type, final int index, final String string,
                final boolean allocate) {

            final int hash = type * 6661 + index * 661 + string.hashCode() & 0x7FFFFFFF;

            long[] table = this.table; // may concurrently change
            int mask = table.length - 1;

            for (int slot = hash & mask;; slot = slot + 1 & mask) {
                final long cell = table[slot];
                if (cell == 0L) {
                    if (!allocate) {
                        return 0;
                    }
                    final Buffer buffer = Buffer.newFixedBuffer(new byte[string.length() * 3]);
                    final int bufferLength = buffer.writeString(0, string);
                    boolean proceed = false;
                    synchronized (this.locks[hash & this.locks.length - 1]) {
                        synchronized (this) {
                            if (table == this.table && table[slot] == 0L) {
                                if (this.size > this.table.length / 3 * 2) {
                                    rehash(); // enforce load factor < .66
                                } else {
                                    proceed = true;
                                }
                            }
                        }
                        if (proceed) {
                            final long offset = append2(type, index, buffer, bufferLength);
                            final int code = MemoryDictionary.offsetToCode(offset);
                            synchronized (this) {
                                if (table != this.table || table[slot] != 0L) {
                                    table = this.table;
                                    mask = table.length - 1;
                                    slot = hash & mask;
                                    while (table[slot] != 0) {
                                        slot = slot + 1 & mask;
                                    }
                                }
                                table[slot] = (long) code << 32 | hash & 0xFFFFFFFFL;
                                ++this.size;
                            }
                            return code;
                        }
                    }
                    return doEncode2(type, index, string, allocate); // retry on rehash and
                                                                     // concurrent change
                }
                if ((int) cell == hash) {
                    final int code = (int) (cell >>> 32);
                    final long offset = MemoryDictionary.codeToOffset(code);
                    if (this.equals(offset, type, index, string)) {
                        return code;
                    }
                }
            }
        }

        @Override
        Value doDecode(final int code) {

            // Extract the offset from the code
            long offset = MemoryDictionary.codeToOffset(code);

            // Extract the header stored at the offset
            final byte header = this.primaryBuffer.read(offset);
            assert (header & 0x7) == 0;

            // Extract value type
            final int type = header >>> 5 & 0x07;

            String string;
            int len;
            final boolean local = (header & 0x10) != 0;
            if (local) {
                final boolean fw = (header & 0x08) != 0;
                len = this.primaryBuffer.read(offset + (fw ? 1 : -1)) & 0xFF;
                offset += fw ? 2 : 1;
                string = this.primaryBuffer.readString(offset, len);
                offset += len;
            } else {
                final long off = this.primaryBuffer.readNumber(offset + 1, 5);
                len = this.secondaryBuffer.readInt(off);
                string = this.secondaryBuffer.readString(off + 4, len);
                offset += 6;
            }

            int index = 0;
            if (type == Dictionary.TYPE_IRI_NAME || type == Dictionary.TYPE_LITERAL_LANG
                    || type == Dictionary.TYPE_LITERAL_DT) {
                index = this.primaryBuffer.readShort(offset) & 0xFFFF;
            }

            return valueFor(type, index, string);
        }

        @Override
        int doType(final int code) {
            final long offset = (long) code << 2;
            return this.primaryBuffer.read(offset) >>> 5 & 0x7;
        }

        @Override
        void doToString(final StringBuilder builder) {
            final long primary = this.primaryOffset;
            final long secondary = this.secondaryOffset;
            final long hash = this.table.length * 8L;
            final long total = primary + secondary + hash;
            builder.append(this.size).append(" values, ").append(total).append(" bytes (")
                    .append(hash).append(" hash table, ").append(primary)
                    .append(" primary buffer, ").append(secondary).append(" secondary buffer)");
        }

        private void rehash() {
            // TODO rehashing big table may takes seconds (e.g. 7s for 64M -> 128M entries) and
            // parallelization provides no benefits. The only solution would be to do rehashing
            // one block at a time and in the meanwhile continue serving encode() requests hitting
            // already rehashed blocks.
            final long ts = System.currentTimeMillis();
            final long[] oldTable = this.table;
            final long[] newTable = new long[oldTable.length << 1];
            final int newMask = newTable.length - 1;
            for (int oldSlot = 0; oldSlot < oldTable.length; ++oldSlot) {
                final long cell = oldTable[oldSlot];
                if (cell != 0L) {
                    final int hash = (int) (cell & 0xFFFFFFFF);
                    int newSlot = hash & newMask;
                    while (newTable[newSlot] != 0L) {
                        newSlot = newSlot + 1 & newMask;
                    }
                    newTable[newSlot] = cell;
                }
            }
            this.table = newTable;
            Dictionary.LOGGER.debug("Rehashed from {} to {} entries in {} ms", oldTable.length,
                    newTable.length, System.currentTimeMillis() - ts);
        }

        private long append(final int type, final int index, final Buffer buffer, int bufferStart,
                int bufferLength) {

            final long offset = MemoryDictionary.padOffset(this.primaryOffset);
            final boolean local = bufferLength < 128 && offset <= 1L << 33;
            final int header = type << 5 | (local ? 0x10 : 0);

            if (local) {
                final boolean fw = offset - this.primaryOffset == 0;
                buffer.write(bufferStart - (fw ? 2 : 1), (byte) (header | (fw ? 0x08 : 0)));
                buffer.write(bufferStart - (fw ? 1 : 2), (byte) bufferLength);
                int bufferEnd = bufferStart + bufferLength;
                if (index > 0) {
                    buffer.writeShort(bufferStart + bufferLength, (short) index);
                    bufferEnd += 2;
                }
                bufferStart -= 2;
                final int writeLength = bufferEnd - bufferStart;
                final long writeOffset = offset + (fw ? 0 : -1);
                this.primaryBuffer.writeBuffer(writeOffset, buffer, bufferStart, writeLength);
                this.primaryOffset = writeOffset + writeLength;

            } else {
                final long pointer = this.secondaryOffset;
                buffer.writeInt(bufferStart - 4, bufferLength);
                bufferStart -= 4;
                bufferLength += 4;
                this.secondaryBuffer.writeBuffer(pointer, buffer, bufferStart, bufferLength);
                this.secondaryOffset += bufferLength;
                if (index > 0) {
                    final long n = (long) header << 56 | pointer << 16 | index & 0xFFFFL;
                    this.primaryBuffer.writeLong(offset, n);
                    this.primaryOffset = offset + 8;
                } else {
                    final long n = (long) header << 40 | pointer;
                    this.primaryBuffer.writeNumber(offset, 6, n);
                    this.primaryOffset = offset + 6;
                }
            }

            return offset;
        }

        private long append2(final int type, final int index, final Buffer buffer,
                final int bufferLength) {

            final long offset;
            final long lastOffset;
            final long secondaryOffset;
            final long indexOffset;
            boolean local;

            synchronized (this.primaryBuffer) {
                lastOffset = this.primaryOffset;
                offset = MemoryDictionary.padOffset(lastOffset);
                secondaryOffset = this.secondaryOffset;
                local = bufferLength <= 128 && offset <= 1L << 33;
                if (local) {
                    final boolean fw = offset - lastOffset == 0;
                    indexOffset = offset + (fw ? 2 : 1) + bufferLength;
                } else {
                    indexOffset = offset + 6;
                    this.secondaryOffset += 4 + bufferLength;
                }
                this.primaryOffset = indexOffset + (index > 0 ? 2 : 0);
            }

            int header = type << 5 | (local ? 0x10 : 0);

            if (local) {
                final boolean fw = offset - lastOffset == 0;
                final long lenOffset = offset + (fw ? +1 : -1);
                final long stringOffset = offset + (fw ? 2 : 1);
                this.primaryBuffer.write(lenOffset, (byte) bufferLength);
                this.primaryBuffer.writeBuffer(stringOffset, buffer, 0L, bufferLength);
                header |= fw ? 0x08 : 0;
            } else {
                this.secondaryBuffer.writeInt(secondaryOffset, bufferLength);
                this.secondaryBuffer.writeBuffer(secondaryOffset + 4, buffer, 0L, bufferLength);
                this.secondaryOffset = secondaryOffset + 4 + bufferLength;
                this.primaryBuffer.writeNumber(offset + 1, 5, secondaryOffset);
            }

            this.primaryBuffer.write(offset, (byte) header);

            if (index > 0) {
                this.primaryBuffer.writeShort(indexOffset, (short) index);
            }

            return offset;
        }

        private boolean equals(long offset, final int type, final int index, final String string) {

            // Extract the header
            final byte header = this.primaryBuffer.read(offset);
            assert (header & 0x7) == 0;

            // Check stored type matches supplied type
            if ((header >>> 5 & 0x07) != type) {
                return false;
            }

            // Extract buffer, offset and length of stored string (do not read it yet)
            // also move offset at the end of the string
            int strLen;
            long strOffset;
            Buffer strBuffer;
            final boolean local = (header & 0x10) != 0;
            if (local) {
                final boolean fw = (header & 0x08) != 0;
                strLen = this.primaryBuffer.read(offset + (fw ? 1 : -1)) & 0xFF;
                strOffset = offset + (fw ? 2 : 1);
                strBuffer = this.primaryBuffer;
                offset = strOffset + strLen;
            } else {
                final long off = this.primaryBuffer.readNumber(offset + 1, 5);
                strLen = this.secondaryBuffer.readInt(off);
                strOffset = off + 4;
                strBuffer = this.secondaryBuffer;
                offset += 6;
            }
            assert strLen >= 0;
            assert strOffset >= 0;

            // Check the index, if applicable
            if (type == Dictionary.TYPE_IRI_NAME || type == Dictionary.TYPE_LITERAL_LANG
                    || type == Dictionary.TYPE_LITERAL_DT) {
                final int storedIndex = this.primaryBuffer.readShort(offset) & 0xFFFF;
                if (index != storedIndex) {
                    return false;
                }
            }

            // Check the string
            return strBuffer.equalString(strOffset, strLen, string);
        }

        private static long codeToOffset(final int code) {
            // return (long) (code & 0xFFFFFFF) << 2;
            return (code & 0x80000000) == 0 ? (long) code << 2
                    : ((code & 0xFFFFFFFFL) << 3) - (1L << 33);
        }

        private static int offsetToCode(final long offset) {
            // return (int) (offset >>> 2);
            return (int) (offset < 1L << 33 ? offset >>> 2 : (1L << 30) + offset >>> 3);
        }

        private static long padOffset(final long offset) {
            return offset < 1L << 33 ? offset + 3 & 0xFFFFFFFFFFFFFFFCL
                    : offset + 7 & 0xFFFFFFFFFFFFFFF8L;
        }

    }

}
