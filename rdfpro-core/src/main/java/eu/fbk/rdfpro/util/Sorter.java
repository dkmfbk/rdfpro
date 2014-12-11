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
package eu.fbk.rdfpro.util;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Sorter<T> implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Sorter.class);

    @Nullable
    private Dictionary dictionary;

    @Nullable
    private Process sortProcess;

    @Nullable
    private OutputStream sortOut;

    @Nullable
    private InputStream sortIn;

    @Nullable
    private Tracker writeTracker;

    @Nullable
    private Tracker readTracker;

    @Nullable
    private List<Output> outputs;

    @Nullable
    private ThreadLocal<Output> threadOutput;

    @Nullable
    private List<Input> inputs;

    @Nullable
    private CountDownLatch decodersLatch;

    @Nullable
    private Throwable exception;

    private boolean startable;

    public static Sorter<Statement> newStatementSorter(final boolean compress) {
        return new StatementSorter(compress); // TODO: add configurable component order
    }

    public static Sorter<Object[]> newTupleSorter(final boolean compress, final Class<?>... schema) {
        return new TupleSorter(compress, schema);
    }

    protected Sorter() {
        // No initialization here: done in start()
        this.startable = true;
    }

    public void start(final boolean deduplicate) throws IOException {

        // Check state
        synchronized (this) {
            if (!this.startable) {
                throw new IllegalArgumentException();
            }
            this.startable = false;
        }

        // Allocate dictionary indexes
        this.dictionary = new Dictionary();

        // Setup streams for sending data to sort
        this.outputs = new ArrayList<Output>();
        this.threadOutput = new ThreadLocal<Output>() {

            @Override
            protected Output initialValue() {
                final OutputStream out = IO.parallelBuffer(Sorter.this.sortOut, (byte) 0);
                final Output output = new Output(out, Sorter.this.dictionary);
                synchronized (Sorter.this.outputs) {
                    Sorter.this.outputs.add(output);
                }
                return output;
            }

        };

        // Invoke sort
        final List<String> command = new ArrayList<String>(Arrays.asList(Environment.getProperty(
                "rdfpro.cmd.sort", "sort").split("\\s+")));
        command.add("-z"); // zero-terminated lines
        if (deduplicate) {
            command.add("-u"); // remove duplicates
        }
        final ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("LC_ALL", "C"); // case sensitive sort
        this.sortProcess = builder.start();

        // Retrieve input and output streams
        this.sortOut = this.sortProcess.getOutputStream();
        this.sortIn = this.sortProcess.getInputStream();

        // Launch a task to log STDERR at ERROR level
        Environment.getPool().submit(new Runnable() {

            @Override
            public void run() {
                final BufferedReader in = new BufferedReader(new InputStreamReader(
                        Sorter.this.sortProcess.getErrorStream(), Charset.forName("UTF-8")));
                try {
                    String line;
                    while ((line = in.readLine()) != null) {
                        LOGGER.error("[sort] {}", line);
                    }
                } catch (final Throwable ex) {
                    LOGGER.error("[sort] failed to read from stream", ex);
                } finally {
                    IO.closeQuietly(in);
                }
            }

        });

        // Initialize trackers
        this.writeTracker = new Tracker(LOGGER, null, //
                "%d records to sort (%d rec/s avg)", //
                "%d records to sort (%d rec/s, %d rec/s avg)");
        this.readTracker = new Tracker(LOGGER, null, //
                "%d records from sort (%d rec/s avg)", //
                "%d records from sort (%d rec/s, %d rec/s avg)");

        // Start write tracker
        this.writeTracker.start();
    }

    public void emit(final T element) throws IOException {
        final Output output;
        try {
            output = this.threadOutput.get();
        } catch (final NullPointerException ex) {
            throw new IllegalStateException();
        }
        encode(output, element);
        output.endRecord();
        this.writeTracker.increment();
    }

    public void end(final boolean parallelize, final Consumer<T> consumer) throws IOException {

        // Check state and invalidate thread local to reject further elements
        synchronized (this) {
            if (this.threadOutput == null) {
                throw new IllegalStateException();
            }
            this.threadOutput = null;
            this.writeTracker.end();
        }

        // Log dictionary status
        LOGGER.debug("Dictionary status:\n{}", this.dictionary);

        try {
            // Complete data sending to sort
            try {
                for (final Output output : this.outputs) {
                    output.close();
                }
                this.outputs.clear();
            } finally {
                this.sortOut.close();
            }

            // Consume sort output, possibly using multiple decode threads
            final int decoders = parallelize ? Environment.getCores() : 1;
            this.decodersLatch = new CountDownLatch(decoders);
            this.inputs = new ArrayList<Input>();
            this.readTracker.start();
            if (!parallelize) {
                this.inputs.add(new Input(IO.buffer(this.sortIn), this.dictionary));
                tryDecode(this.inputs.get(0), consumer);
            } else {
                for (int i = 0; i < decoders; ++i) {
                    final InputStream in = IO.parallelBuffer(this.sortIn, (byte) 0);
                    this.inputs.add(new Input(in, this.dictionary));
                }
                for (int i = 1; i < decoders; ++i) {
                    final Input input = this.inputs.get(i);
                    Environment.getPool().execute(new Runnable() {

                        @Override
                        public void run() {
                            tryDecode(input, consumer);
                        }

                    });
                }
                tryDecode(this.inputs.get(0), consumer);
            }
            this.decodersLatch.await();
            this.readTracker.end();

        } catch (final Throwable ex) {
            this.exception = ex;

        } finally {
            // Close streams and propagate exception, if any
            IO.closeQuietly(this.sortIn);
            if (this.inputs != null) {
                for (final Input input : this.inputs) {
                    input.close();
                }
            }
            if (this.exception != null) {
                if (this.exception instanceof IOException) {
                    throw (IOException) this.exception;
                } else if (this.exception instanceof RuntimeException) {
                    throw (RuntimeException) this.exception;
                } else if (this.exception instanceof Error) {
                    throw (Error) this.exception;
                }
                throw new RuntimeException(this.exception);
            }
        }
    }

    private void tryDecode(final Input input, final Consumer<T> consumer) {
        try {
            while (input.nextRecord()) {
                final T element = decode(input);
                consumer.accept(element);
                this.readTracker.increment();
            }
            input.close();
        } catch (final Throwable ex) {
            if (this.exception == null) {
                this.exception = ex;
                for (final Input r : this.inputs) {
                    r.close(); // forces other decoders threads to abort
                }
            }
        } finally {
            this.decodersLatch.countDown();
        }
    }

    @Override
    public void close() {
        try {
            // Kill the sort process, if still active. This will ultimately break ongoing threads
            if (this.sortProcess != null) {
                this.sortProcess.destroy();
            }

        } catch (final Throwable ex) {
            LOGGER.error("Exception caught while killing sort process", ex);

        } finally {
            // Mark as non startable and release everything
            this.startable = false;
            this.dictionary = null;
            this.sortProcess = null;
            this.sortOut = null;
            this.sortIn = null;
            this.outputs = null;
            this.threadOutput = null;
            this.inputs = null;
            this.decodersLatch = null;
            this.exception = null;
        }
    }

    protected abstract void encode(Output output, final T element) throws IOException;

    protected abstract T decode(Input input) throws IOException;

    // bits len len mask hi mask layout
    // 06 01 0x40 0x3F 01 6
    // 13 02 0x80 0x3F 10 6 7
    // 18 03 0xC0 0x0F 1100 4 7 7
    // 25 04 0xD0 0x0F 1101 4 7 7 7
    // 32 05 0xE0 0x0F 1110 4 7 7 7 7
    // 37 06 0xF0 0x03 111100 2 7 7 7 7 7
    // 44 07 0xF4 0x03 111101 2 7 7 7 7 7 7
    // 51 08 0xF8 0x03 111110 2 7 7 7 7 7 7 7
    // 57 09 0xFC 0x01 1111110 1 7 7 7 7 7 7 7 7
    // 64 10 9xFE 0x01 1111111 1 7 7 7 7 7 7 7 7 7

    // 0 7 -- 7
    // 10 6 7 -- 13 = 0x1FFF
    // 11 6 7 7 -- 20

    public static final class Output {

        private final OutputStream out;

        private final Dictionary dictionary;

        private final int[] remaining;

        Output(final OutputStream out, final Dictionary dictionary) {
            this.out = out;
            this.dictionary = dictionary;
            this.remaining = new int[] { -1 };
        }

        void endRecord() throws IOException {
            this.out.write(0);
        }

        void close() throws IOException {
            this.out.close();
        }

        public final void writeStatement(@Nullable final Statement statement,
                final boolean compress) throws IOException {
            if (statement == null) {
                writeValue(null, compress);
            } else {
                writeValue(statement.getSubject(), compress);
                writeValue(statement.getPredicate(), compress);
                writeValue(statement.getObject(), compress);
                writeValue(statement.getContext(), compress);
            }
        }

        public final void writeValue(@Nullable final Value value, final boolean compress)
                throws IOException {
            if (value == null) {
                write(1);
            } else if (value instanceof BNode) {
                writeStringHelper(((BNode) value).getID(), 0, 1);
            } else if (value instanceof Literal) {
                final Literal lit = (Literal) value;
                final URI dt = lit.getDatatype();
                if (dt == null) {
                    final String lang = lit.getLanguage();
                    if (lang == null) {
                        writeStringHelper(lit.getLabel(), 0, 2);
                    } else {
                        final int key = !compress ? -1 : this.dictionary.encodeLanguage(lang);
                        if (key < 0) {
                            writeStringHelper(lit.getLabel(), 0, 3);
                            writeStringHelper(lang, 0, 2);
                        } else {
                            writeStringHelper(lit.getLabel(), 0, 5);
                            writeNumber(key);
                        }
                    }
                } else {
                    final int key = !compress ? -1 : this.dictionary.encodeDatatype(dt);
                    if (key < 0) {
                        writeStringHelper(lit.getLabel(), 0, 3);
                        writeStringHelper(dt.stringValue(), 0, 1);
                    } else {
                        writeStringHelper(lit.getLabel(), 0, 4);
                        writeNumber(key);
                    }
                }
            } else if (value instanceof URI) {
                final URI uri = (URI) value;
                boolean done = false;
                if (compress) {
                    final int key = this.dictionary.encodeURI(uri, this.remaining);
                    if (key >= 0) {
                        final int r = this.remaining[0];
                        if (r < 0) {
                            write(7);
                            writeNumber(key);
                            done = true;
                        } else {
                            writeStringHelper(uri.stringValue(), r, 7);
                            writeNumber(key);
                            done = true;
                        }
                    }
                }
                if (!done) {
                    writeStringHelper(uri.stringValue(), 0, 6);
                }
            }
        }

        public final void writeString(@Nullable final String s) throws IOException {
            if (s == null) {
                write(0x02);
            } else {
                writeStringHelper(s, 0, 0x01);
            }
        }

        public final void writeNumber(final long num) throws IOException {
            if (num < 0L || num > 0x1FFFFFFFFFFFFFFL /* 57 bit */) {
                writeNumberHelper(10, 0xFE, num);
            } else if (num <= 0x3FL /* 6 bit */) {
                writeNumberHelper(1, 0x40, num);
            } else if (num <= 0x1FFFL /* 13 bit */) {
                writeNumberHelper(2, 0x80, num);
            } else if (num <= 0x3FFFFL /* 18 bit */) {
                writeNumberHelper(3, 0xC0, num);
            } else if (num <= 0x1FFFFFFL /* 25 bit */) {
                writeNumberHelper(4, 0xD0, num);
            } else if (num <= 0xFFFFFFFFL /* 32 bit */) {
                writeNumberHelper(5, 0xE0, num);
            } else if (num <= 0x1FFFFFFFFFL /* 37 bit */) {
                writeNumberHelper(6, 0xF0, num);
            } else if (num <= 0xFFFFFFFFFFFL /* 44 bit */) {
                writeNumberHelper(7, 0xF4, num);
            } else if (num <= 0x7FFFFFFFFFFFFL /* 51 bit */) {
                writeNumberHelper(8, 0xF8, num);
            } else {
                writeNumberHelper(9, 0xFC, num);
            }
        }

        // B = 1011 1111

        // 0x1FFF xx11 1111 x111 1111

        private void writeStringHelper(final String s, final int offset, final int delimiter)
                throws IOException {
            final int len = s.length();
            for (int i = offset; i < len; ++i) {
                int c = s.charAt(i);
                if (c <= 0x07) {
                    c += 0xFFFF;
                }
                if (c <= 0x7F) {
                    write(c);
                } else if (c <= 0x1FFF) {
                    write(0x80 | c >> 7);
                    write(0x80 | c & 0x7F);
                } else {
                    write(0xC0 | c >> 14);
                    write(0x80 | c >> 7 & 0x7F);
                    write(0x80 | c & 0x7F);
                }
            }
            write(delimiter);
        }

        private void writeNumberHelper(final int len, final int mask, final long num)
                throws IOException {
            write(mask | (int) (num >>> (len - 1) * 7));
            for (int i = len - 2; i >= 0; --i) {
                write(0x80 | (int) (num >>> i * 7 & 0x7F));
            }
        }

        private void write(final int b) throws IOException {
            assert (b & 0xFF) != 0;
            this.out.write(b);
        }

    }

    public static final class Input {

        private final InputStream in;

        private final Dictionary dictionary;

        private final StringBuilder builder;

        private int c;

        Input(final InputStream in, final Dictionary dictionary) {
            this.in = in;
            this.dictionary = dictionary;
            this.builder = new StringBuilder();
            this.c = 0;
        }

        boolean nextRecord() throws IOException {
            while (this.c != 0) {
                LOGGER.warn("Skipping " + this.c);
                this.c = this.in.read();
                if (this.c < 0) {
                    throw new EOFException("EOF found before completing read of record");
                }
            }
            this.c = this.in.read();
            if (this.c < 0) {
                return false; // EOF reached, no more records
            }
            if (this.c == 0) {
                throw new Error("Empty record!");
            }
            return true;
        }

        void close() {
            IO.closeQuietly(this.in);
        }

        public final boolean isEOF() {
            return this.c <= 0;
        }

        @Nullable
        public final Statement readStatement() throws IOException {

            final Resource s = (Resource) readValue();
            if (s == null) {
                return null;
            }

            final URI p = (URI) readValue();
            final Value o = readValue();
            final Resource c = (Resource) readValue();

            final ValueFactory vf = Statements.VALUE_FACTORY;
            return c == null ? vf.createStatement(s, p, o) : vf.createStatement(s, p, o, c);
        }

        @Nullable
        public final Value readValue() throws IOException {
            final int delim = readStringHelper();
            if (delim == 1 && this.builder.length() == 0) {
                return null;
            }
            final String s = this.builder.toString();
            final ValueFactory vf = Statements.VALUE_FACTORY;
            if (delim == 1) {
                return vf.createBNode(this.builder.toString());
            } else if (delim == 2) {
                return vf.createLiteral(s);
            } else if (delim == 3) {
                final int delim2 = readStringHelper();
                final String s2 = this.builder.toString();
                if (delim2 == 1) {
                    return vf.createLiteral(s, vf.createURI(s2));
                } else {
                    return vf.createLiteral(s, s2);
                }
            } else if (delim == 4) {
                final int key = (int) readNumber();
                final URI dt = this.dictionary.decodeDatatype(key);
                return vf.createLiteral(s, dt);
            } else if (delim == 5) {
                final int key = (int) readNumber();
                final String lang = this.dictionary.decodeLanguage(key);
                return vf.createLiteral(s, lang);
            } else if (delim == 6) {
                return vf.createURI(s);
            } else if (delim == 7) {
                final int key = (int) readNumber();
                return this.dictionary.decodeURI(key, s.isEmpty() ? null : s);
            }
            throw new IllegalArgumentException("Invalid value delimiter: " + delim);
        }

        @Nullable
        public final String readString() throws IOException {
            final int delimiter = readStringHelper();
            if (delimiter == 0x02) {
                return null;
            } else if (delimiter != 0x01) {
                throw new IOException("Found invalid string delimiter: " + delimiter);
            }
            return this.builder.toString();
        }

        public final long readNumber() throws IOException {
            final int b = read();
            if (b <= 0x40 + 0x3F) {
                return readNumberHelper(1, b & 0x3F);
            } else if (b <= 0x80 + 0x3F) {
                return readNumberHelper(2, b & 0x3F);
            } else if (b <= 0xC0 + 0x0F) {
                return readNumberHelper(3, b & 0x0F);
            } else if (b <= 0xD0 + 0x0F) {
                return readNumberHelper(4, b & 0x0F);
            } else if (b <= 0xE0 + 0x0F) {
                return readNumberHelper(5, b & 0x0F);
            } else if (b <= 0xF0 + 0x03) {
                return readNumberHelper(6, b & 0x03);
            } else if (b <= 0xF4 + 0x03) {
                return readNumberHelper(7, b & 0x03);
            } else if (b <= 0xF8 + 0x03) {
                return readNumberHelper(8, b & 0x03);
            } else if (b <= 0xFC + 0x01) {
                return readNumberHelper(9, b & 0x01);
            } else {
                return readNumberHelper(10, b & 0x01);
            }
        }

        private int readStringHelper() throws IOException {
            this.builder.setLength(0);
            while (true) {
                final int c = read();
                if (c <= 0x07) {
                    return c;
                } else if (c <= 0x7F) {
                    this.builder.append((char) c);
                } else if (c <= 0xBF) {
                    final int c1 = read();
                    final int n = (c & 0x3F) << 7 | c1 & 0x7F;
                    this.builder.append((char) n);
                } else {
                    final int c1 = read();
                    final int c2 = read();
                    int n = (c & 0x3F) << 14 | (c1 & 0x7F) << 7 | c2 & 0x7F;
                    if (n > 0xFFFF) {
                        n = n - 0xFFFF;
                    }
                    this.builder.append((char) n);
                }
            }
        }

        private long readNumberHelper(final int len, final int start) throws IOException {
            long num = start;
            for (int i = 1; i < len; ++i) {
                final int c = read();
                num = num << 7 | c & 0x7F;
            }
            return num;
        }

        private int read() throws IOException {
            final int result = this.c;
            if (result <= 0) {
                throw new EOFException("Byte is " + result);
            }
            this.c = this.in.read();
            return result;
        }

    }

    private static final class StatementSorter extends Sorter<Statement> {

        private final boolean compress;

        StatementSorter(final boolean compress) {
            this.compress = compress;
        }

        @Override
        protected void encode(final Output output, final Statement record) throws IOException {
            output.writeStatement(record, this.compress);
        }

        @Override
        protected Statement decode(final Input input) throws IOException {
            return input.readStatement();
        }

    }

    private static final class TupleSorter extends Sorter<Object[]> {

        private static final int TYPE_STATEMENT = 0;

        private static final int TYPE_VALUE = 1;

        private static final int TYPE_STRING = 2;

        private static final int TYPE_NUMBER = 3;

        private final boolean compress;

        private final int[] schema;

        TupleSorter(final boolean compress, final Class<?>... schema) {
            this.compress = compress;
            this.schema = new int[schema.length];
            for (int i = 0; i < schema.length; ++i) {
                final Class<?> clazz = schema[i];
                if (Statement.class.equals(clazz)) {
                    this.schema[i] = TYPE_STATEMENT;
                } else if (Value.class.equals(clazz)) {
                    this.schema[i] = TYPE_VALUE;
                } else if (String.class.equals(clazz)) {
                    this.schema[i] = TYPE_STRING;
                } else if (Long.class.equals(clazz)) {
                    this.schema[i] = TYPE_NUMBER;
                } else {
                    throw new IllegalArgumentException("Unsupported tuple field: " + clazz);
                }
            }
        }

        @Override
        protected void encode(final Output output, final Object[] record) throws IOException {
            for (int i = 0; i < this.schema.length; ++i) {
                final Object field = record[i];
                final int type = this.schema[i];
                switch (type) {
                case TYPE_STATEMENT:
                    output.writeStatement((Statement) field, this.compress);
                    break;
                case TYPE_VALUE:
                    output.writeValue((Value) field, this.compress);
                    break;
                case TYPE_STRING:
                    output.writeString((String) field);
                    break;
                case TYPE_NUMBER:
                    output.writeNumber(((Number) field).longValue());
                    break;
                default:
                    throw new Error("Unexpected type " + type);
                }
            }
        }

        @Override
        protected Object[] decode(final Input input) throws IOException {
            final Object[] record = new Object[this.schema.length];
            for (int i = 0; i < this.schema.length; ++i) {
                final int type = this.schema[i];
                switch (type) {
                case TYPE_STATEMENT:
                    record[i] = input.readStatement();
                    break;
                case TYPE_VALUE:
                    record[i] = input.readValue();
                    break;
                case TYPE_STRING:
                    record[i] = input.readString();
                    break;
                case TYPE_NUMBER:
                    record[i] = input.readNumber();
                    break;
                default:
                    throw new Error("Unexpected type " + type);
                }
            }
            return record;
        }

    }

    private static final class Dictionary {

        private static final int LANGUAGE_INDEX_SIZE = 1024;

        private static final int DATATYPE_INDEX_SIZE = 1024;

        private static final int NAMESPACE_INDEX_SIZE = 256 * 1024;

        private static final int VOCAB_INDEX_SIZE = 64 * 1024;

        private static final int OTHER_INDEX_SIZE = 4 * 1024;

        private static final int URI_CACHE_SIZE = 8191;

        private final GenericIndex<String> languageIndex;

        private final GenericIndex<URI> datatypeIndex;

        private final StringIndex namespaceIndex;

        private final StringIndex vocabNameIndex;

        private final StringIndex otherNameIndex;

        private final int vocabNamespaces;

        private final int[] uriCacheCodes;

        private final URI[] uriCacheURIs;

        private final Object[] uriCacheLocks;

        public Dictionary() {
            this.languageIndex = new GenericIndex<String>(LANGUAGE_INDEX_SIZE);
            this.datatypeIndex = new GenericIndex<URI>(DATATYPE_INDEX_SIZE);
            this.namespaceIndex = new StringIndex(NAMESPACE_INDEX_SIZE);
            this.vocabNameIndex = new StringIndex(VOCAB_INDEX_SIZE);
            this.otherNameIndex = new StringIndex(OTHER_INDEX_SIZE);

            for (final String ns : Namespaces.DEFAULT.uris()) {
                int nsHash = 0;
                for (int i = ns.length() - 1; i >= 0; --i) {
                    final int c = ns.charAt(i);
                    nsHash = nsHash * 31 + c;
                }
                this.namespaceIndex.put(ns, 0, ns.length(), nsHash, false);
            }
            this.vocabNamespaces = Namespaces.DEFAULT.uris().size();

            this.vocabNameIndex.put("", 0, 0, 0, false);
            this.otherNameIndex.put("", 0, 0, 0, false);

            this.uriCacheCodes = new int[URI_CACHE_SIZE];
            this.uriCacheURIs = new URI[URI_CACHE_SIZE];
            this.uriCacheLocks = new Object[32];
            for (int i = 0; i < 32; ++i) {
                this.uriCacheLocks[i] = new Object();
            }
        }

        public int encodeLanguage(final String language) {
            return this.languageIndex.put(language);
        }

        public String decodeLanguage(final int code) {
            return this.languageIndex.get(code);
        }

        public int encodeDatatype(final URI datatype) {
            return this.datatypeIndex.put(datatype);
        }

        public URI decodeDatatype(final int code) {
            return this.datatypeIndex.get(code);
        }

        public int encodeURI(final URI uri, final int[] remaining) {

            final String s = uri.stringValue();
            final int len = s.length();

            int nameHash = 0;
            int nsLen = len;
            while (--nsLen >= 0) {
                final int c = s.charAt(nsLen);
                if (c == '#' || c == '/' || c == ':') {
                    break;
                }
                nameHash = nameHash * 31 + c;
            }

            int nsHash = 0;
            for (int i = nsLen; i >= 0; --i) {
                final int c = s.charAt(i);
                nsHash = nsHash * 31 + c;
            }
            ++nsLen;

            final int nsKey = this.namespaceIndex.put(s, 0, nsLen, nsHash, false);
            if (nsKey < 0) {
                remaining[0] = -1;
                return -1;
            }

            if (nsKey < this.vocabNamespaces) {
                final int nameKey = this.vocabNameIndex.put(s, nsLen, len, nameHash, true);
                if (nameKey < 0) {
                    remaining[0] = nsLen;
                    return nsKey;
                } else {
                    final int code = nameKey * this.vocabNamespaces + nsKey << 1;
                    remaining[0] = -1;
                    return code;
                }
            }

            final int nameKey = this.otherNameIndex.put(s, nsLen, len, nameHash, false);
            if (nameKey < 0) {
                remaining[0] = nsLen;
                return nsKey;
            } else {
                final int code = nameKey * NAMESPACE_INDEX_SIZE + nsKey << 1 | 0x01;
                remaining[0] = -1;
                return code;
            }
        }

        public URI decodeURI(final int code, final String remaining) {

            if (remaining != null && !remaining.isEmpty()) {
                final String ns = this.namespaceIndex.get(code);
                return Statements.VALUE_FACTORY.createURI(ns, remaining);
            }

            final int offset = code % this.uriCacheURIs.length;
            final Object lock = this.uriCacheLocks[offset % this.uriCacheLocks.length];
            synchronized (lock) {
                final int cachedCode = this.uriCacheCodes[offset];
                if (cachedCode == code) {
                    return this.uriCacheURIs[offset];
                }
            }

            final boolean isVocab = (code & 0x01) == 0;
            final int c = code >>> 1;

            URI uri;
            if (isVocab) {
                final int nsKey = c % this.vocabNamespaces;
                final int nameKey = c / this.vocabNamespaces;
                final String ns = this.namespaceIndex.get(nsKey);
                final String name = this.vocabNameIndex.get(nameKey);
                uri = Statements.VALUE_FACTORY.createURI(ns, name);
            } else {
                final int nsKey = c % NAMESPACE_INDEX_SIZE;
                final int nameKey = c / NAMESPACE_INDEX_SIZE;
                final String ns = this.namespaceIndex.get(nsKey);
                final String name = this.otherNameIndex.get(nameKey);
                uri = Statements.VALUE_FACTORY.createURI(ns, name);
            }

            synchronized (lock) {
                this.uriCacheURIs[offset] = uri;
                this.uriCacheCodes[offset] = code;
            }

            return uri;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("language index:  ").append(this.languageIndex).append("\n");
            builder.append("datatype index:  ").append(this.datatypeIndex).append("\n");
            builder.append("namespace index: ").append(this.namespaceIndex).append("\n");
            builder.append("vocab index:     ").append(this.vocabNameIndex).append("\n");
            builder.append("other index:     ").append(this.otherNameIndex);
            return builder.toString();
        }

        private static final class StringIndex {

            private static final int NUM_LOCKS = 32;

            private final int capacity;

            private final AtomicInteger size;

            private final List<String> list;

            private final int[] table;

            private final Object[] locks;

            StringIndex(final int capacity) {
                this.capacity = capacity;
                this.size = new AtomicInteger(0);
                this.list = new ArrayList<String>();
                this.table = new int[2 * (this.capacity * 4 - 1)];
                this.locks = new Object[NUM_LOCKS];
                for (int i = 0; i < NUM_LOCKS; ++i) {
                    this.locks[i] = new Object();
                }
            }

            @Nullable
            public int put(final String string, final int begin, final int end, final int hash,
                    final boolean likelyExist) {

                final int tableSize = this.table.length / 2;
                final int segmentSize = (tableSize + NUM_LOCKS - 1) / NUM_LOCKS;

                int index = Math.abs(hash) % tableSize;

                final int segment = index / segmentSize;
                final int segmentStart = segment * segmentSize;
                final int segmentEnd = Math.min(tableSize, segmentStart + segmentSize);

                final boolean full = this.size.get() >= this.capacity;

                // first we operate read-only with no synchronization if possible
                if (full || likelyExist) {
                    for (int i = 0; i < segmentSize; ++i) {
                        final int offset = index * 2;
                        final int key = this.table[offset] - 1;
                        if (key < 0) {
                            if (full) {
                                return -1;
                            }
                            break;
                        } else if (hash == this.table[offset + 1]
                                && equals(this.list.get(key), string, begin, end)) {
                            return key;
                        }
                        ++index;
                        if (index >= segmentEnd) {
                            index = segmentStart;
                        }
                    }
                }

                // then operate read-write with synchronization using lock striping
                // NOTE: index may have changed
                synchronized (this.locks[segment]) {
                    for (int i = 0; i < segmentSize; ++i) {
                        final int offset = index * 2;
                        final int key = this.table[offset] - 1;
                        if (key < 0) {
                            synchronized (this.list) {
                                final int newKey = this.size.get();
                                if (newKey >= this.capacity) {
                                    return -1;
                                }
                                this.list.add(string.substring(begin, end));
                                this.table[offset] = newKey + 1;
                                this.table[offset + 1] = hash;
                                this.size.incrementAndGet(); // should flush memory changes
                                return newKey;
                            }
                        } else if (hash == this.table[offset + 1]
                                && equals(this.list.get(key), string, begin, end)) {
                            return key;
                        }
                        ++index;
                        if (index >= segmentEnd) {
                            index = segmentStart;
                        }
                    }
                }

                return -1; // segment full (unlikely, thus we handle but don't optimize this case)
            }

            @Nullable
            public String get(final int key) {
                final String result = this.list.get(key);
                if (result == null) {
                    throw new IllegalArgumentException("No element for key " + key);
                }
                return result;
            }

            @Override
            public String toString() {
                return this.size.get() + "/" + this.capacity;
            }

            private boolean equals(final String reference, final String string, final int begin,
                    final int end) {
                final int len = reference.length();
                if (len == end - begin) {
                    int i = len;
                    int j = end;
                    while (--i >= 0) {
                        --j;
                        if (reference.charAt(i) != string.charAt(j)) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }

        }

        private static final class GenericIndex<T> {

            private final int capacity;

            private final List<T> list;

            private final int[] table;

            GenericIndex(final int capacity) {
                this.capacity = capacity;
                this.list = new ArrayList<T>();
                this.table = new int[2 * (this.capacity * 4 - 1)];
            }

            @Nullable
            public int put(final T element) {

                final int tableSize = this.table.length / 2;
                final int hash = element.hashCode();
                int index = Math.abs(hash) % tableSize;

                // first we operate read-only with no synchronization, as it is likely the entry
                // is already there (for datatypes and langagues, at least)
                while (true) {
                    final int offset = index * 2;
                    final int key = this.table[offset] - 1;
                    if (key < 0) {
                        break;
                    } else if (hash == this.table[offset + 1]
                            && element.equals(this.list.get(key))) {
                        return key;
                    }
                    index = (index + 1) % tableSize;
                }

                // then we operate read-write with a simple global lock (few insertions expected)
                synchronized (this.list) {
                    while (true) {
                        final int offset = index * 2;
                        final int key = this.table[offset] - 1;
                        if (key < 0) {
                            final int newKey = this.list.size();
                            if (newKey >= this.capacity) {
                                return -1;
                            }
                            this.list.add(element);
                            this.table[offset] = newKey + 1;
                            this.table[offset + 1] = hash;
                            return newKey;
                        } else if (hash == this.table[offset + 1]
                                && element.equals(this.list.get(key))) {
                            return key;
                        }
                        index = (index + 1) % tableSize;
                    }
                }
            }

            @Nullable
            public T get(final int key) {
                final T result = this.list.get(key);
                if (result == null) {
                    throw new IllegalArgumentException("No element for key " + key);
                }
                return result;
            }

            @Override
            public String toString() {
                return this.list.size() + "/" + this.capacity;
            }

        }

    }

}
