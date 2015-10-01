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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.ProcessBuilder.Redirect;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// note on buffered stream thread safety: they are not thread safe and are expected to be used by
// a single thread, with the exception of method close() which can be called concurrently by other
// threads (so to guarantee e.g. asynchronous write/read termination) and it is synchronized so
// that it is run exactly once

public final class IO {

    private static final Logger LOGGER = LoggerFactory.getLogger(IO.class);

    // sequential read performances varying buffer size on test TQL file
    // 8k - 380781 tr/s
    // 16k - 383115 tr/s
    // 32k - 386237 tr/s
    // 64k - 394210 tr/s
    // 128k - 398547 tr/s
    // 256k - 396883 tr/s
    // 512k - 389184 tr/s
    // 1M - 383701 tr/s
    // note: pipe buffer on linux is 64k

    private static final int BUFFER_SIZE = Integer.parseInt(Environment.getProperty(
            "rdfpro.buffer.size", "" + 64 * 1024));

    // parallel read performances varying queue size on test TQL file, buffer = 64K
    // 16 * 64k (1M) - 603k tr/s
    // 64 * 64k (4M) - 605k-616k tr/s
    // 128 * 64k (8M) - 601k-618k tr/s
    // 256 * 64k (16M) - 624k-631k tr/s
    // 1024 * 64k (64M) - 625k tr/s

    private static final int BUFFER_NUM_READ = Integer.parseInt(Environment.getProperty(
            "rdfpro.buffer.numr", "256"));

    private static final int BUFFER_NUM_WRITE = Integer.parseInt(Environment.getProperty(
            "rdfpro.buffer.numw", "16"));

    @Nullable
    public static <T> T closeQuietly(@Nullable final T object) {
        if (object instanceof AutoCloseable) {
            try {
                ((AutoCloseable) object).close();
            } catch (final Throwable ex) {
                LOGGER.error("Error closing " + object.getClass().getSimpleName(), ex);
            }
        }
        return object;
    }

    public static URL extractURL(final String location) {
        Objects.requireNonNull(location);
        try {
            final int index = location.indexOf(':');
            if (index < 0) {
                return new File(location).toURI().toURL();
            }
            String s = location.charAt(0) != '.' ? location : location.substring(index + 1);
            if (s.startsWith("classpath:")) {
                s = s.substring("classpath:".length());
                return Objects.requireNonNull(IO.class.getResource(s));
            } else {
                try {
                    return new URL(s);
                } catch (final MalformedURLException ex) {
                    return new File(s).toURI().toURL();
                }
            }
        } catch (final Throwable ex) {
            throw new IllegalArgumentException("Cannot extract URL from '" + location + "'", ex);
        }
    }

    public static String extractExtension(final String location) {
        Objects.requireNonNull(location);
        final int index = location.indexOf(':');
        int extEnd = location.length();
        if (index >= 0) {
            if (location.charAt(0) == '.') {
                return location.substring(0, index);
            }
            int index2 = location.lastIndexOf('#');
            index2 = index2 >= 0 ? index2 : location.length();
            extEnd = location.lastIndexOf('?', index2);
            extEnd = extEnd >= 0 ? extEnd : index2;
        }
        final int nameStart = Math.max(-1, location.lastIndexOf('/', extEnd)) + 1;
        final int extStart = location.indexOf('.', nameStart);
        return extStart < 0 ? "" : location.substring(extStart, extEnd);
    }

    public static InputStream read(final String location) throws IOException {

        final String ext = extractExtension(location);
        final URL url = extractURL(location);

        String cmd = null;
        if (ext.endsWith(".bz2")) {
            cmd = Environment.getProperty("rdfpro.cmd.bzip2", "bzip2") + " -dck";
        } else if (ext.endsWith(".gz")) {
            cmd = Environment.getProperty("rdfpro.cmd.gzip", "gzip") + " -dc";
        } else if (ext.endsWith(".xz")) {
            cmd = Environment.getProperty("rdfpro.cmd.xz", "xz") + " -dc";
        } else if (ext.endsWith(".7z")) {
            cmd = Environment.getProperty("rdfpro.cmd.7za", "7za") + " -so e";
        } else if (ext.endsWith(".lz4")) {
            cmd = Environment.getProperty("rdfpro.cmd.lz4", "lz4") + " -dc";
        }

        if ("file".equals(url.getProtocol())) {
            final File file;
            try {
                file = new File(url.toURI());
            } catch (final URISyntaxException ex) {
                throw new IllegalArgumentException("Invalid file:// URL: " + location);
            }

            if (cmd == null) {
                LOGGER.debug("Reading file {}", file);
                return new FileInputStream(file);

            } else {
                LOGGER.debug("Reading file {} using {}", file, cmd);
                cmd += " " + file.getAbsolutePath();
                final Process process = new ProcessBuilder(cmd.split("\\s+")) //
                        .redirectError(Redirect.INHERIT).start();
                return process.getInputStream();
            }

        } else {
            final InputStream stream = url.openStream();
            if (cmd == null) {
                LOGGER.debug("Downloading file {}", url);
                return stream;

            } else {
                LOGGER.debug("Downloading file {} using {}", url, cmd);
                final Process process = new ProcessBuilder(cmd.split("\\s+")) //
                        .redirectError(Redirect.INHERIT).start();
                Environment.getPool().execute(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            final byte[] buffer = new byte[8 * 1024];
                            while (true) {
                                final int count = stream.read(buffer);
                                if (count < 0) {
                                    break;
                                }
                                process.getOutputStream().write(buffer, 0, count);
                            }
                            process.getOutputStream().close();

                        } catch (final Throwable ex) {
                            LOGGER.error("Error reading from " + url, ex);
                            process.destroy();
                        } finally {
                            closeQuietly(stream);
                        }
                    }
                });
                return process.getInputStream();
            }
        }
    }

    public static OutputStream write(final String location) throws IOException {

        final String ext = extractExtension(location);
        final URL url = extractURL(location);

        if (!"file".equals(url.getProtocol())) {
            throw new IllegalArgumentException("Cannot write to non-file URL " + location);
        }

        final File file;
        try {
            file = new File(url.toURI());
        } catch (final URISyntaxException ex) {
            throw new IllegalArgumentException("Invalid file:// URL: " + location);
        }

        String cmd = null;
        if (ext.endsWith(".bz2")) {
            cmd = Environment.getProperty("rdfpro.cmd.bzip2", "bzip2") + " -c -9";
        } else if (ext.endsWith(".gz")) {
            cmd = Environment.getProperty("rdfpro.cmd.gzip", "gzip") + " -c -9";
        } else if (ext.endsWith(".xz")) {
            cmd = Environment.getProperty("rdfpro.cmd.xz", "xz") + " -c -9";
        } else if (ext.endsWith(".lz4")) {
            cmd = Environment.getProperty("rdfpro.cmd.lz4", "lz4") + " -c -9";
        }

        if (cmd == null) {
            LOGGER.debug("Writing file {}", file);
            return new FileOutputStream(file);

        } else {
            // FIXME: here we should wrap the returned output stream, making sure that an
            // invocation to close waits for the process to exit; otherwise, if we immediately try
            // opening the written file, we may find it unfinished as the process is still
            // flushing data to it.
            LOGGER.debug("Writing file {} using {}", file, cmd);
            final Process process = new ProcessBuilder(cmd.split("\\s+")) //
                    .redirectOutput(file).redirectError(Redirect.INHERIT).start();
            return process.getOutputStream();
        }
    }

    public static InputStream buffer(final InputStream stream) {
        return new SimpleBufferedInputStream(stream);
    }

    public static OutputStream buffer(final OutputStream stream) {
        return new SimpleBufferedOutputStream(stream);
    }

    public static Reader buffer(final Reader reader) {
        return new SimpleBufferedReader(reader);
    }

    public static Writer buffer(final Writer writer) {
        return new SimpleBufferedWriter(writer);
    }

    public static InputStream parallelBuffer(final InputStream stream, final byte delimiter) {
        return new ParallelBufferedInputStream(stream, delimiter);
    }

    public static OutputStream parallelBuffer(final OutputStream stream, final byte delimiter) {
        return new ParallelBufferedOutputStream(stream, delimiter);
    }

    public static Reader parallelBuffer(final Reader reader, final char delimiter) {
        return new ParallelBufferedReader(reader, delimiter);
    }

    public static Writer parallelBuffer(final Writer writer, final char delimiter) {
        return new ParallelBufferedWriter(writer, delimiter);
    }

    public static Reader utf8Reader(final InputStream stream) {
        return new UTF8Reader(stream);
    }

    public static Writer utf8Writer(final OutputStream stream) {
        return new UTF8Writer(stream);
    }

    private static void propagate(final Throwable ex) throws IOException {
        if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        } else if (ex instanceof Error) {
            throw (Error) ex;
        } else if (ex instanceof IOException) {
            throw (IOException) ex;
        }
        throw new IOException(ex);
    }

    private IO() {
    }

    private static final class SimpleBufferedInputStream extends InputStream {

        private final InputStream stream;

        private final byte buffer[];

        private int count;

        private int pos;

        private boolean closed;

        public SimpleBufferedInputStream(final InputStream stream) {
            this.stream = Objects.requireNonNull(stream);
            this.buffer = new byte[BUFFER_SIZE];
            this.count = 0;
            this.pos = 0;
            this.closed = false;
        }

        @Override
        public int read() throws IOException {
            if (this.pos >= this.count) {
                fill();
                if (this.pos >= this.count) {
                    return -1;
                }
            }
            return this.buffer[this.pos++] & 0xFF;
        }

        @Override
        public int read(final byte buf[], int off, int len) throws IOException {
            if ((off | len | off + len | buf.length - (off + len)) < 0) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                checkNotClosed();
                return 0;
            }
            int result = 0;
            while (true) {
                final int available = this.count - this.pos;
                if (available > 0) {
                    final int n = available > len ? len : available;
                    System.arraycopy(this.buffer, this.pos, buf, off, n);
                    this.pos += n;
                    off += n;
                    len -= n;
                    result += n;
                    if (len == 0 || this.stream.available() == 0) {
                        return result;
                    }
                }
                if (len >= BUFFER_SIZE) {
                    final int n = this.stream.read(buf, off, len);
                    result += n < 0 ? 0 : n;
                    return result == 0 ? -1 : result;
                } else if (len > 0) {
                    fill();
                    if (this.count == 0) {
                        return result == 0 ? -1 : result;
                    }
                }
            }
        }

        @Override
        public long skip(final long n) throws IOException {
            if (n <= 0) {
                checkNotClosed();
                return 0;
            }
            final int available = this.count - this.pos;
            if (available <= 0) {
                return this.stream.skip(n);
            }
            final long skipped = available < n ? available : n;
            this.pos += skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            final int n = this.count - this.pos;
            final int available = this.stream.available();
            return n > Integer.MAX_VALUE - available ? Integer.MAX_VALUE : n + available;
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }

        @Override
        public void mark(final int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffer) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            this.count = this.pos; // fail soon in case a new write request is received
            this.stream.close();
        }

        private void fill() throws IOException {
            checkNotClosed();
            final int n = this.stream.read(this.buffer);
            this.count = n < 0 ? 0 : n;
            this.pos = 0;
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Stream has been closed");
            }
        }

    }

    private static final class SimpleBufferedOutputStream extends OutputStream {

        private final OutputStream stream;

        private final byte[] buffer;

        private int count; // num of bytes in the buffer

        private boolean closed;

        SimpleBufferedOutputStream(final OutputStream stream) {
            this.stream = Objects.requireNonNull(stream);
            this.buffer = new byte[BUFFER_SIZE];
            this.count = 0;
            this.closed = false;
        }

        @Override
        public void write(final int b) throws IOException {
            if (this.count >= BUFFER_SIZE) {
                flushBuffer();
            }
            this.buffer[this.count++] = (byte) b;
        }

        @Override
        public void write(final byte buf[], int off, int len) throws IOException {
            if (len >= BUFFER_SIZE) {
                flushBuffer();
                this.stream.write(buf, off, len);
                return;
            }
            final int available = BUFFER_SIZE - this.count;
            if (available < len) {
                System.arraycopy(buf, off, this.buffer, this.count, available);
                this.count += available;
                off += available;
                len -= available;
                flushBuffer();
            }
            System.arraycopy(buf, off, this.buffer, this.count, len);
            this.count += len;
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
            this.stream.flush();
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffer) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            flushBuffer();
            this.stream.close();
            // this.count = BUFFER_SIZE; // fail soon in case a new write request is received
        }

        private void flushBuffer() throws IOException {
            if (this.count > 0) {
                this.stream.write(this.buffer, 0, this.count);
                this.count = 0;
            }
        }

    }

    private static final class SimpleBufferedReader extends Reader {

        private final Reader reader;

        private final char[] buffer;

        private int count;

        private int pos;

        private boolean closed;

        public SimpleBufferedReader(final Reader reader) {
            this.reader = reader;
            this.buffer = new char[BUFFER_SIZE];
            this.count = 0;
            this.pos = 0;
            this.closed = false;
        }

        @Override
        public int read() throws IOException {
            if (this.pos >= this.count) {
                fill();
                if (this.pos >= this.count) {
                    return -1;
                }
            }
            return this.buffer[this.pos++] & 0xFFFF;
        }

        @Override
        public int read(final char[] cbuf, final int off, final int len) throws IOException {
            if ((off | len | off + len | cbuf.length - (off + len)) < 0) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                checkNotClosed();
                return 0;
            }
            int available = this.count - this.pos;
            if (available == 0) {
                if (len >= BUFFER_SIZE) {
                    return this.reader.read(cbuf, off, len);
                } else {
                    fill();
                    available = this.count - this.pos;
                    if (available == 0) {
                        return -1;
                    }
                }
            }
            final int n = available > len ? len : available;
            System.arraycopy(this.buffer, this.pos, cbuf, off, n);
            this.pos += n;
            return n;
        }

        @Override
        public long skip(final long n) throws IOException {
            if (n <= 0) {
                checkNotClosed();
                return 0;
            }
            final int available = this.count - this.pos;
            if (available == 0) {
                return this.reader.skip(n);
            }
            final long skipped = available < n ? available : n;
            this.pos += skipped;
            return skipped;
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }

        @Override
        public void mark(final int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffer) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            this.closed = true;
            this.count = this.pos; // fail soon in case a new write request is received
            this.reader.close();
        }

        private void fill() throws IOException {
            checkNotClosed();
            final int n = this.reader.read(this.buffer);
            this.count = n < 0 ? 0 : n;
            this.pos = 0;
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Reader has been closed");
            }
        }

    }

    private static final class SimpleBufferedWriter extends Writer {

        private final Writer writer;

        private final char[] buffer;

        private int count; // num of chars in the buffer

        private boolean closed;

        SimpleBufferedWriter(final Writer writer) {
            this.writer = Objects.requireNonNull(writer);
            this.buffer = new char[BUFFER_SIZE];
            this.count = 0;
            this.closed = false;
        }

        @Override
        public void write(final int c) throws IOException {
            if (this.count >= BUFFER_SIZE) {
                flushBuffer();
            }
            this.buffer[this.count++] = (char) c;
        }

        @Override
        public void write(final char[] cbuf, int off, int len) throws IOException {
            if (len >= BUFFER_SIZE) {
                flushBuffer();
                this.writer.write(cbuf, off, len);
                return;
            }
            final int available = BUFFER_SIZE - this.count;
            if (available < len) {
                System.arraycopy(cbuf, off, this.buffer, this.count, available);
                this.count += available;
                off += available;
                len -= available;
                flushBuffer();
            }
            System.arraycopy(cbuf, off, this.buffer, this.count, len);
            this.count += len;
        }

        @Override
        public void write(final String str, int off, int len) throws IOException {
            if (len >= BUFFER_SIZE) {
                flushBuffer();
                this.writer.write(str, off, len);
                return;
            }
            final int available = BUFFER_SIZE - this.count;
            if (available < len) {
                str.getChars(off, off + available, this.buffer, this.count);
                this.count += available;
                off += available;
                len -= available;
                flushBuffer();
            }
            str.getChars(off, off + len, this.buffer, this.count);
            this.count += len;
        }

        @Override
        public void flush() throws IOException {
            flushBuffer();
            this.writer.flush();
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffer) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            flushBuffer();
            this.writer.close();
            // this.count = BUFFER_SIZE; // fail soon in case a new write request is received
        }

        private void flushBuffer() throws IOException {
            if (this.count > 0) {
                this.writer.write(this.buffer, 0, this.count);
                this.count = 0;
            }
        }

    }

    private static final class ParallelBufferedReader extends Reader {

        private Fetcher fetcher;

        private final List<CharBuffer> buffers;

        private int index;

        private char[] buffer;

        private int count;

        private int pos;

        private boolean closed;

        ParallelBufferedReader(final Reader reader, final char delimiter) {
            this.fetcher = Fetcher.forReader(reader, delimiter);
            this.buffers = new ArrayList<CharBuffer>();
            this.index = 0;
            this.buffer = null;
            this.count = 0;
            this.pos = 0;
            this.closed = false;
            this.fetcher.open();
        }

        @Override
        public int read() throws IOException {
            if (this.pos >= this.count) {
                fill();
                if (this.count == 0) {
                    return -1;
                }
            }
            return this.buffer[this.pos++] & 0xFFFF;
        }

        @Override
        public int read(final char[] cbuf, final int off, final int len) throws IOException {
            if ((off | len | off + len | cbuf.length - (off + len)) < 0) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                checkNotClosed();
                return 0;
            }
            final int available = this.count - this.pos;
            if (available == 0) {
                fill();
                if (this.count == 0) {
                    return -1;
                }
            }
            final int n = available > len ? len : available;
            System.arraycopy(this.buffer, this.pos, cbuf, off, n);
            this.pos += n;
            return n;
        }

        @Override
        public long skip(final long n) throws IOException {
            if (n <= 0) {
                checkNotClosed();
                return 0;
            }
            int available = this.count - this.pos;
            if (available == 0) {
                fill();
                available = this.count;
            }
            final long skipped = available < n ? available : n;
            this.pos += skipped;
            return skipped;
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }

        @Override
        public void mark(final int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffers) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            this.count = this.pos;
            this.buffers.clear();
            this.fetcher.close();
            this.fetcher = null;
        }

        private void fill() throws IOException {
            checkNotClosed();
            if (this.buffer != null) {
                this.buffer = null;
                this.pos = 0;
                this.count = 0;
            }
            if (this.index == this.buffers.size()) {
                this.fetcher.fetch(this.buffers);
                this.index = 0;
            }
            if (this.index < this.buffers.size()) {
                final CharBuffer cb = this.buffers.get(this.index++);
                this.buffer = cb.array();
                this.count = cb.limit();
            }
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Reader has been closed");
            }
        }

        private static final class Fetcher implements Runnable {

            private static final Map<Reader, Fetcher> FETCHERS = new WeakHashMap<Reader, Fetcher>();

            private static final Object EOF = new Object();

            private final BlockingQueue<Object> queue;

            private Reader reader;

            private final char delimiter;

            private final List<CharBuffer> buffers;

            private int references;

            private Throwable exception;

            private final CountDownLatch latch;

            private Fetcher(final Reader reader, final char delimiter) {
                this.queue = new ArrayBlockingQueue<Object>(BUFFER_NUM_READ, false);
                this.reader = reader;
                this.delimiter = delimiter;
                this.buffers = new ArrayList<CharBuffer>();
                this.references = 0;
                this.exception = null;
                this.latch = new CountDownLatch(1);

                Environment.getPool().submit(this);
            }

            private void release(final CharBuffer buffer) {
                synchronized (this.buffers) {
                    if (this.buffers.size() < BUFFER_NUM_READ + Environment.getCores() + 1) {
                        buffer.clear();
                        this.buffers.add(buffer);
                    }
                }
            }

            private CharBuffer allocate() {
                synchronized (this.buffers) {
                    if (!this.buffers.isEmpty()) {
                        return this.buffers.remove(this.buffers.size() - 1);
                    }
                }
                return CharBuffer.allocate(BUFFER_SIZE);
            }

            public void open() {
                synchronized (this) {
                    if (this.references < 0) {
                        throw new IllegalStateException("Reader has been closed");
                    }
                    ++this.references;
                }
            }

            public void close() throws IOException {
                synchronized (this) {
                    --this.references;
                    if (this.references != 0) {
                        return;
                    }
                    this.references = -1; // prevent further open() to occur
                }
                this.queue.clear(); // nobody will use queued buffers
                while (true) {
                    try {
                        this.latch.await();
                        break;
                    } catch (final InterruptedException ex) {
                        // ignore
                    }
                }
                synchronized (FETCHERS) {
                    FETCHERS.remove(this.reader);
                }
                this.queue.clear();
                this.buffers.clear();
                this.reader = null; // may be heavyweight, better to release immediately
                synchronized (this) {
                    if (this.exception != null) {
                        propagate(this.exception);
                    }
                }
            }

            @SuppressWarnings("unchecked")
            public void fetch(final List<CharBuffer> buffers) throws IOException {
                try {
                    synchronized (this) {
                        if (this.exception != null) {
                            throw this.exception;
                        }
                    }
                    for (final CharBuffer buffer : buffers) {
                        release(buffer);
                    }
                    buffers.clear();
                    final Object object = this.queue.take();
                    if (object == EOF) {
                        this.queue.add(EOF);
                        return;
                    }
                    buffers.addAll((List<CharBuffer>) object);
                } catch (IOException | RuntimeException | Error ex) {
                    throw ex;
                } catch (final Throwable ex) {
                    throw new IOException(ex);
                }
            }

            @Override
            public void run() {

                try {
                    CharBuffer restBuffer = allocate();
                    List<CharBuffer> buffers = new ArrayList<CharBuffer>();

                    boolean eof = false;
                    while (!eof) {

                        synchronized (this) {
                            if (this.references < 0) {
                                break;
                            }
                        }

                        final CharBuffer curBuffer = restBuffer;
                        while (!eof && curBuffer.hasRemaining()) {
                            final int n = this.reader.read(curBuffer);
                            eof = n < 0;
                        }
                        curBuffer.flip();
                        buffers.add(curBuffer);

                        restBuffer = allocate();
                        if (!eof) {
                            final char[] curChars = curBuffer.array();
                            final int curLastIndex = curBuffer.limit() - 1;
                            for (int i = curLastIndex; i >= 0; --i) {
                                if (curChars[i] == this.delimiter) {
                                    restBuffer.position(curLastIndex - i);
                                    System.arraycopy(curChars, i + 1, restBuffer.array(), 0,
                                            restBuffer.position());
                                    curBuffer.limit(i + 1);
                                    this.queue.put(buffers);
                                    buffers = new ArrayList<CharBuffer>();
                                    break;
                                }
                            }
                        }
                    }

                    this.queue.put(buffers);

                } catch (final Throwable ex) {
                    synchronized (this) {
                        this.exception = ex;
                    }
                }

                try {
                    closeQuietly(this.reader);

                    while (true) {
                        try {
                            this.queue.put(EOF);
                            break;
                        } catch (final InterruptedException ex) {
                            // ignore
                        }
                    }
                } finally {
                    this.latch.countDown();
                }
            }

            public static Fetcher forReader(final Reader reader, final char delimiter) {
                synchronized (FETCHERS) {
                    Fetcher fetcher = FETCHERS.get(reader);
                    if (fetcher == null) {
                        fetcher = new Fetcher(reader, delimiter);
                        FETCHERS.put(reader, fetcher);
                    } else if (fetcher.delimiter != delimiter) {
                        throw new IllegalStateException("Already reading from reader " + reader
                                + " using delimiter " + delimiter);
                    }
                    return fetcher;
                }
            }

        }

    }

    private static final class ParallelBufferedWriter extends Writer {

        private Emitter emitter;

        private final char delimiter;

        private final List<CharBuffer> buffers;

        private char[] buffer;

        private int count; // from 0 to BUFFER_SIZE

        private int threshold;

        private boolean closed;

        ParallelBufferedWriter(final Writer writer, final char delimiter) {
            this.emitter = Emitter.forWriter(writer);
            this.delimiter = delimiter;
            this.buffers = new ArrayList<CharBuffer>();
            this.buffer = new char[2 * BUFFER_SIZE];
            this.count = 0;
            this.threshold = BUFFER_SIZE;
            this.closed = false;
            this.emitter.open();
        }

        @Override
        public void write(final int c) throws IOException {
            if (this.count < this.threshold) {
                this.buffer[this.count++] = (char) c;
            } else {
                writeAndTryFlush((char) c);
            }
        }

        @Override
        public void write(final char[] cbuf, int off, int len) throws IOException {
            final int available = this.threshold - this.count;
            if (available >= len) {
                System.arraycopy(cbuf, off, this.buffer, this.count, len);
                this.count += len;
                return;
            }
            if (available > 0) {
                System.arraycopy(cbuf, off, this.buffer, this.count, available);
                this.count += available;
                off += available;
                len -= available;
            }
            final int end = off + len;
            while (off < end) {
                writeAndTryFlush(cbuf[off++]);
            }
        }

        @Override
        public void write(final String str, int off, int len) throws IOException {
            final int available = this.threshold - this.count;
            final int end = off + len;
            if (available >= len) {
                str.getChars(off, end, this.buffer, this.count);
                this.count += len;
                return;
            }
            if (available > 0) {
                str.getChars(off, off + available, this.buffer, this.count);
                this.count += available;
                off += available;
                len -= available;
            }
            while (off < end) {
                writeAndTryFlush(str.charAt(off++));
            }
        }

        @Override
        public void flush() throws IOException {
            flushBuffers();
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffers) {
                if (this.closed) {
                    return;
                }
                flushBuffers();
                this.closed = true;
            }
            this.buffers.clear();
            this.buffer = null;
            this.emitter.close();
            this.emitter = null;
        }

        private void writeAndTryFlush(final char c) throws IOException {
            this.buffer[this.count++] = c;
            if (c == this.delimiter) {
                flushBuffers();
            } else if (this.count == this.buffer.length) {
                checkNotClosed();
                this.buffers.add(CharBuffer.wrap(this.buffer));
                this.buffer = new char[BUFFER_SIZE];
                this.count = 0;
                this.threshold = 0;
            }
        }

        private void flushBuffers() throws IOException {
            checkNotClosed();
            if (this.count > 0) {
                final CharBuffer cb = CharBuffer.wrap(this.buffer);
                cb.limit(this.count);
                this.buffers.add(cb);
            }
            this.emitter.emit(this.buffers);
            if (!this.buffers.isEmpty()) {
                this.buffer = this.buffers.get(0).array();
                this.buffers.clear();
            }
            this.count = 0;
            this.threshold = BUFFER_SIZE;
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Writer has been closed");
            }
        }

        private static class Emitter implements Runnable {

            private static final Map<Writer, Emitter> EMITTERS = new WeakHashMap<Writer, Emitter>();

            private static final Object EOF = new Object();

            private final BlockingQueue<Object> queue;

            private final List<CharBuffer> buffers;

            private Writer writer;

            private int references;

            private Throwable exception;

            private final CountDownLatch latch;

            private Emitter(final Writer writer) {
                this.queue = new ArrayBlockingQueue<Object>(BUFFER_NUM_WRITE, false);
                this.writer = writer;
                this.buffers = new ArrayList<CharBuffer>();
                this.references = 0;
                this.exception = null;
                this.latch = new CountDownLatch(1);
                Environment.getPool().submit(this);
            }

            private void release(final CharBuffer buffer) {
                synchronized (this.buffers) {
                    if (this.buffers.size() < BUFFER_NUM_WRITE + Environment.getCores() + 1) {
                        buffer.clear();
                        this.buffers.add(buffer);
                    }
                }
            }

            private CharBuffer allocate() {
                synchronized (this.buffers) {
                    if (!this.buffers.isEmpty()) {
                        return this.buffers.remove(this.buffers.size() - 1);
                    }
                }
                return CharBuffer.allocate(2 * BUFFER_SIZE);
            }

            public void open() {
                synchronized (this) {
                    if (this.references < 0) {
                        throw new IllegalStateException("Stream has been closed");
                    }
                    ++this.references;
                }
            }

            public void close() throws IOException {
                synchronized (this) {
                    --this.references;
                    if (this.references != 0) {
                        return;
                    }
                    this.references = -1; // prevent further open() to occur
                }
                while (true) {
                    try {
                        this.queue.put(EOF);
                        break;
                    } catch (final InterruptedException ex) {
                        // ignore
                    }
                }
                while (true) {
                    try {
                        this.latch.await();
                        break;
                    } catch (final InterruptedException ex) {
                        // ignore
                    }
                }
                synchronized (EMITTERS) {
                    EMITTERS.remove(this.writer);
                }
                this.queue.clear();
                this.buffers.clear();
                this.writer = null; // may be heavyweight, better to release immediately
                synchronized (this) {
                    if (this.exception != null) {
                        propagate(this.exception);
                    }
                }
            }

            public void emit(final List<CharBuffer> buffers) throws IOException {
                try {
                    synchronized (this) {
                        if (this.exception != null) {
                            throw this.exception;
                        }
                    }
                    this.queue.put(new ArrayList<CharBuffer>(buffers));
                    buffers.clear();
                    buffers.add(allocate());
                } catch (IOException | RuntimeException | Error ex) {
                    throw ex;
                } catch (final Throwable ex) {
                    throw new IOException(ex);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                try {
                    while (true) {
                        final Object object = this.queue.take();
                        if (object == EOF) {
                            break;
                        }
                        final List<CharBuffer> buffers = (List<CharBuffer>) object;
                        for (final CharBuffer buffer : buffers) {
                            this.writer.write(buffer.array(), buffer.position(), buffer.limit());
                        }
                        if (!buffers.isEmpty()) {
                            release(buffers.get(0));
                        }
                    }
                } catch (final Throwable ex) {
                    synchronized (this) {
                        this.exception = ex;
                    }
                    this.queue.clear();
                } finally {
                    closeQuietly(this.writer);
                    this.latch.countDown();
                }
            }

            public static Emitter forWriter(final Writer writer) {
                synchronized (EMITTERS) {
                    Emitter manager = EMITTERS.get(writer);
                    if (manager == null) {
                        manager = new Emitter(writer);
                        EMITTERS.put(writer, manager);
                    }
                    return manager;
                }
            }

        }

    }

    private static final class ParallelBufferedInputStream extends InputStream {

        private Fetcher fetcher;

        private final List<ByteBuffer> buffers;

        private int index;

        private byte[] buffer;

        private int count;

        private int pos;

        private boolean closed;

        ParallelBufferedInputStream(final InputStream stream, final byte delimiter) {
            this.fetcher = Fetcher.forStream(stream, delimiter);
            this.buffers = new ArrayList<ByteBuffer>();
            this.index = 0;
            this.buffer = null;
            this.count = 0;
            this.pos = 0;
            this.closed = false;
            this.fetcher.open();
        }

        @Override
        public int read() throws IOException {
            if (this.pos >= this.count) {
                fill();
                if (this.count == 0) {
                    return -1;
                }
            }
            return this.buffer[this.pos++] & 0xFF;
        }

        @Override
        public int read(final byte[] buf, final int off, final int len) throws IOException {
            if ((off | len | off + len | buf.length - (off + len)) < 0) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                checkNotClosed();
                return 0;
            }
            final int available = this.count - this.pos;
            if (available == 0) {
                fill();
                if (this.count == 0) {
                    return -1;
                }
            }
            final int n = available > len ? len : available;
            System.arraycopy(this.buffer, this.pos, buf, off, n);
            this.pos += n;
            return n;
        }

        @Override
        public long skip(final long n) throws IOException {
            if (n <= 0) {
                checkNotClosed();
                return 0;
            }
            int available = this.count - this.pos;
            if (available == 0) {
                fill();
                available = this.count;
            }
            final long skipped = available < n ? available : n;
            this.pos += skipped;
            return skipped;
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }

        @Override
        public void mark(final int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffers) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            this.count = this.pos;
            this.buffers.clear();
            this.fetcher.close();
            this.fetcher = null;
        }

        private void fill() throws IOException {
            checkNotClosed();
            if (this.buffer != null) {
                this.buffer = null;
                this.pos = 0;
                this.count = 0;
            }
            if (this.index == this.buffers.size()) {
                this.fetcher.fetch(this.buffers);
                this.index = 0;
            }
            if (this.index < this.buffers.size()) {
                final ByteBuffer buffer = this.buffers.get(this.index++);
                this.buffer = buffer.array();
                this.count = buffer.limit();
            }
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Stream has been closed");
            }
        }

        private static final class Fetcher implements Runnable {

            private static final Map<InputStream, Fetcher> FETCHERS = new WeakHashMap<InputStream, Fetcher>();

            private static final Object EOF = new Object();

            private final BlockingQueue<Object> queue;

            private InputStream stream;

            private final byte delimiter;

            private final List<ByteBuffer> buffers;

            private int references;

            private Throwable exception;

            private final CountDownLatch latch;

            private Fetcher(final InputStream stream, final byte delimiter) {
                this.queue = new ArrayBlockingQueue<Object>(BUFFER_NUM_READ, false);
                this.stream = stream;
                this.delimiter = delimiter;
                this.buffers = new ArrayList<ByteBuffer>();
                this.references = 0;
                this.exception = null;
                this.latch = new CountDownLatch(1);

                Environment.getPool().submit(this);
            }

            private void release(final ByteBuffer buffer) {
                synchronized (this.buffers) {
                    if (this.buffers.size() < BUFFER_NUM_READ + Environment.getCores() + 1) {
                        buffer.clear();
                        this.buffers.add(buffer);
                    }
                }
            }

            private ByteBuffer allocate() {
                synchronized (this.buffers) {
                    if (!this.buffers.isEmpty()) {
                        return this.buffers.remove(this.buffers.size() - 1);
                    }
                }
                return ByteBuffer.allocate(2 * BUFFER_SIZE);
            }

            public void open() {
                synchronized (this) {
                    if (this.references < 0) {
                        throw new IllegalStateException("Reader has been closed");
                    }
                    ++this.references;
                }
            }

            public void close() throws IOException {
                synchronized (this) {
                    --this.references;
                    if (this.references != 0) {
                        return;
                    }
                    this.references = -1; // prevent further open() to occur
                }
                this.queue.clear(); // nobody will use queued buffers
                while (true) {
                    try {
                        this.latch.await();
                        break;
                    } catch (final InterruptedException ex) {
                        // ignore
                    }
                }
                synchronized (FETCHERS) {
                    FETCHERS.remove(this.stream);
                }
                this.queue.clear();
                this.buffers.clear();
                this.stream = null; // may be heavyweight, better to release immediately
                synchronized (this) {
                    if (this.exception != null) {
                        propagate(this.exception);
                    }
                }
            }

            @SuppressWarnings("unchecked")
            public void fetch(final List<ByteBuffer> buffers) throws IOException {
                try {
                    synchronized (this) {
                        if (this.exception != null) {
                            throw this.exception;
                        }
                    }
                    for (final ByteBuffer buffer : buffers) {
                        release(buffer);
                    }
                    buffers.clear();
                    final Object object = this.queue.take();
                    if (object == EOF) {
                        this.queue.add(EOF);
                        return;
                    }
                    buffers.addAll((List<ByteBuffer>) object);
                } catch (IOException | RuntimeException | Error ex) {
                    throw ex;
                } catch (final Throwable ex) {
                    throw new IOException(ex);
                }
            }

            @Override
            public void run() {

                try {
                    ByteBuffer restBuffer = allocate();
                    List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

                    boolean eof = false;
                    while (!eof) {

                        synchronized (this) {
                            if (this.references < 0) {
                                break;
                            }
                        }

                        final ByteBuffer curBuffer = restBuffer;
                        final byte[] array = curBuffer.array();

                        while (!eof && curBuffer.hasRemaining()) {
                            final int offset = curBuffer.position();
                            final int len = curBuffer.remaining();
                            final int n = this.stream.read(array, offset, len);
                            eof = n < 0;
                            if (!eof) {
                                curBuffer.position(offset + n);
                            }
                        }

                        curBuffer.flip();
                        buffers.add(curBuffer);

                        restBuffer = allocate();
                        if (!eof) {
                            final int curLastIndex = curBuffer.limit() - 1;
                            for (int i = curLastIndex; i >= 0; --i) {
                                if (array[i] == this.delimiter) {
                                    restBuffer.position(curLastIndex - i);
                                    System.arraycopy(array, i + 1, restBuffer.array(), 0,
                                            restBuffer.position());
                                    curBuffer.limit(i + 1);
                                    this.queue.put(buffers);
                                    buffers = new ArrayList<ByteBuffer>();
                                    break;
                                }
                            }
                        }
                    }

                    this.queue.put(buffers);

                } catch (final Throwable ex) {
                    synchronized (this) {
                        this.exception = ex;
                    }
                }

                try {
                    closeQuietly(this.stream);

                    while (true) {
                        try {
                            this.queue.put(EOF);
                            break;
                        } catch (final InterruptedException ex) {
                            // ignore
                        }
                    }
                } finally {
                    this.latch.countDown();
                }
            }

            public static Fetcher forStream(final InputStream stream, final byte delimiter) {
                synchronized (FETCHERS) {
                    Fetcher fetcher = FETCHERS.get(stream);
                    if (fetcher == null) {
                        fetcher = new Fetcher(stream, delimiter);
                        FETCHERS.put(stream, fetcher);
                    } else if (fetcher.delimiter != delimiter) {
                        throw new IllegalStateException("Already reading from stream " + stream
                                + " using delimiter " + delimiter);
                    }
                    return fetcher;
                }
            }

        }

    }

    private static final class ParallelBufferedOutputStream extends OutputStream {

        private Emitter emitter;

        private final byte delimiter;

        private final List<ByteBuffer> buffers;

        private byte[] buffer;

        private int count; // from 0 to BUFFER_SIZE

        private int threshold;

        private boolean closed;

        ParallelBufferedOutputStream(final OutputStream stream, final byte delimiter) {
            this.emitter = Emitter.forStream(stream);
            this.delimiter = delimiter;
            this.buffers = new ArrayList<ByteBuffer>();
            this.buffer = new byte[2 * BUFFER_SIZE];
            this.count = 0;
            this.threshold = BUFFER_SIZE;
            this.closed = false;
            this.emitter.open();
        }

        @Override
        public void write(final int c) throws IOException {
            if (this.count < this.threshold) {
                this.buffer[this.count++] = (byte) c;
            } else {
                writeAndTryFlush((byte) c);
            }
        }

        @Override
        public void write(final byte[] buf, int off, int len) throws IOException {
            final int available = this.threshold - this.count;
            if (available >= len) {
                System.arraycopy(buf, off, this.buffer, this.count, len);
                this.count += len;
                return;
            }
            if (available > 0) {
                System.arraycopy(buf, off, this.buffer, this.count, available);
                this.count += available;
                off += available;
                len -= available;
            }
            final int end = off + len;
            while (off < end) {
                writeAndTryFlush(buf[off++]);
            }
        }

        @Override
        public void flush() throws IOException {
            flushBuffers();
        }

        @Override
        public void close() throws IOException {
            synchronized (this.buffers) {
                if (this.closed) {
                    return;
                }
                flushBuffers();
                this.closed = true;
            }
            this.buffers.clear();
            this.buffer = null;
            this.emitter.close();
            this.emitter = null;
        }

        private void writeAndTryFlush(final byte c) throws IOException {
            this.buffer[this.count++] = c;
            if (c == this.delimiter) {
                flushBuffers();
            } else if (this.count == this.buffer.length) {
                checkNotClosed();
                this.buffers.add(ByteBuffer.wrap(this.buffer));
                this.buffer = new byte[BUFFER_SIZE];
                this.count = 0;
                this.threshold = 0;
            }
        }

        private void flushBuffers() throws IOException {
            checkNotClosed();
            if (this.count > 0) {
                final ByteBuffer buffer = ByteBuffer.wrap(this.buffer);
                buffer.limit(this.count);
                this.buffers.add(buffer);
            }
            this.emitter.emit(this.buffers);
            if (!this.buffers.isEmpty()) {
                this.buffer = this.buffers.get(0).array();
                this.buffers.clear();
            }
            this.count = 0;
            this.threshold = BUFFER_SIZE;
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Writer has been closed");
            }
        }

        private static class Emitter implements Runnable {

            private static final Map<OutputStream, Emitter> EMITTERS = new WeakHashMap<OutputStream, Emitter>();

            private static final Object EOF = new Object();

            private final BlockingQueue<Object> queue;

            private final List<ByteBuffer> buffers;

            private OutputStream stream;

            private int references;

            private Throwable exception;

            private final CountDownLatch latch;

            private Emitter(final OutputStream stream) {
                this.queue = new ArrayBlockingQueue<Object>(BUFFER_NUM_WRITE, false);
                this.stream = stream;
                this.buffers = new ArrayList<ByteBuffer>();
                this.references = 0;
                this.exception = null;
                this.latch = new CountDownLatch(1);
                Environment.getPool().submit(this);
            }

            private void release(final ByteBuffer buffer) {
                synchronized (this.buffers) {
                    if (this.buffers.size() < BUFFER_NUM_WRITE + Environment.getCores() + 1) {
                        buffer.clear();
                        this.buffers.add(buffer);
                    }
                }
            }

            private ByteBuffer allocate() {
                synchronized (this.buffers) {
                    if (!this.buffers.isEmpty()) {
                        return this.buffers.remove(this.buffers.size() - 1);
                    }
                }
                return ByteBuffer.allocate(2 * BUFFER_SIZE);
            }

            public void open() {
                synchronized (this) {
                    if (this.references < 0) {
                        throw new IllegalStateException("Stream has been closed");
                    }
                    ++this.references;
                }
            }

            public void close() throws IOException {
                synchronized (this) {
                    --this.references;
                    if (this.references != 0) {
                        return;
                    }
                    this.references = -1; // prevent further open() to occur
                }
                while (true) {
                    try {
                        this.queue.put(EOF);
                        break;
                    } catch (final InterruptedException ex) {
                        // ignore
                    }
                }
                while (true) {
                    try {
                        this.latch.await();
                        break;
                    } catch (final InterruptedException ex) {
                        // ignore
                    }
                }
                synchronized (EMITTERS) {
                    EMITTERS.remove(this.stream);
                }
                this.queue.clear();
                this.buffers.clear();
                this.stream = null; // may be heavyweight, better to release immediately
                synchronized (this) {
                    if (this.exception != null) {
                        propagate(this.exception);
                    }
                }
            }

            public void emit(final List<ByteBuffer> buffers) throws IOException {
                try {
                    synchronized (this) {
                        if (this.exception != null) {
                            throw this.exception;
                        }
                    }
                    this.queue.put(new ArrayList<ByteBuffer>(buffers));
                    buffers.clear();
                    buffers.add(allocate());
                } catch (final Throwable ex) {
                    propagate(ex);
                }
            }

            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                try {
                    while (true) {
                        final Object object = this.queue.take();
                        if (object == EOF) {
                            break;
                        }
                        final List<ByteBuffer> buffers = (List<ByteBuffer>) object;
                        for (final ByteBuffer buffer : buffers) {
                            this.stream.write(buffer.array(), buffer.position(), buffer.limit());
                        }
                        if (!buffers.isEmpty()) {
                            release(buffers.get(0));
                        }
                    }
                } catch (final Throwable ex) {
                    synchronized (this) {
                        this.exception = ex;
                    }
                    this.queue.clear();
                } finally {
                    closeQuietly(this.stream);
                    this.latch.countDown();
                }
            }

            public static Emitter forStream(final OutputStream stream) {
                synchronized (EMITTERS) {
                    Emitter emitter = EMITTERS.get(stream);
                    if (emitter == null) {
                        emitter = new Emitter(stream);
                        EMITTERS.put(stream, emitter);
                    }
                    return emitter;
                }
            }

        }

    }

    private static final class UTF8Reader extends Reader {

        private final InputStream stream;

        private boolean closed;

        public UTF8Reader(final InputStream stream) {
            this.stream = stream;
            this.closed = false;
        }

        @Override
        public int read() throws IOException {
            final int b0 = this.stream.read();
            return (b0 & 0xFFFFFF80) == 0 ? b0 : readHelper(b0);
        }

        private int readHelper(final int b0) throws IOException {

            if (b0 < 0) { // EOF
                return -1;

            } else if (b0 <= 0b11011111) { // 110xxxxx 10xxxxxx
                final int b1 = this.stream.read();
                if ((b1 & 0b11000000) == 0b10000000) {
                    return (b0 & 0b00011111) << 6 | b1 & 0b00111111;
                }

            } else if (b0 <= 0b11101111) { // 1110xxxx 10xxxxxx 10xxxxxx
                final int b1 = this.stream.read();
                final int b2 = this.stream.read();
                if ((b1 & 0b11000000) == 0b10000000 && (b2 & 0b11000000) == 0b10000000) {
                    return (b0 & 0b00001111) << 12 | (b1 & 0b00111111) << 6 | b2 & 0b00111111;
                }

            } else if (b0 <= 0b11110111) { // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                final int b1 = this.stream.read();
                final int b2 = this.stream.read();
                final int b3 = this.stream.read();
                if ((b1 & 0b11000000) == 0b10000000 && (b2 & 0b11000000) == 0b10000000
                        && (b3 & 0b11000000) == 0b10000000) {
                    return (b0 & 0b00000111) << 18 | (b1 & 0b00111111) << 12
                            | (b2 & 0b00111111) << 6 | b3 & 0b00111111;
                }
            }

            throw new IOException("Invalid/truncated UTF8 code");
        }

        @Override
        public int read(final char[] buf, final int off, final int len) throws IOException {
            if ((off | len | off + len | buf.length - (off + len)) < 0) {
                throw new IndexOutOfBoundsException();
            }
            if (len == 0) {
                checkNotClosed();
                return 0;
            }
            int index = off;
            int c = read();
            if (c < 0) {
                return -1;
            }
            buf[index++] = (char) c;
            final int end = off + Math.min(len, this.stream.available() / 2);
            while (index < end) {
                c = read();
                if (c < 0) {
                    break;
                }
                buf[index++] = (char) c;
            }
            return index - off;
        }

        @Override
        public long skip(final long n) throws IOException {
            if (n == 0L) {
                checkNotClosed();
                return 0L;
            }
            final int skippable = this.stream.available() / 2;
            int toSkip = skippable;
            do {
                final int c = read();
                if (c < 0) {
                    break;
                }
                --toSkip;
            } while (toSkip > 0);
            return skippable - toSkip;
        }

        @Override
        public boolean ready() throws IOException {
            return this.stream.available() >= 4;
        }

        @Override
        public void reset() throws IOException {
            throw new IOException("Mark not supported");
        }

        @Override
        public void mark(final int readlimit) {
        }

        @Override
        public boolean markSupported() {
            return false;
        }

        @Override
        public void close() throws IOException {
            synchronized (this) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            this.stream.close();
        }

        private void checkNotClosed() throws IOException {
            if (this.closed) {
                throw new IOException("Reader has been closed");
            }
        }

    }

    private static final class UTF8Writer extends Writer {

        private final OutputStream stream;

        private boolean closed;

        UTF8Writer(final OutputStream stream) {
            this.stream = stream;
            this.closed = false;
        }

        @Override
        public void write(final int c) throws IOException {
            if (c <= 0b1111111) { // 0xxxxxxx
                this.stream.write(c);
            } else {
                writeHelper(c);
            }
        }

        private void writeHelper(final int c) throws IOException {

            if (c <= 0b11111_111111) { // 110xxxxx 10xxxxxx
                this.stream.write(0b11000000 | c >>> 6);
                this.stream.write(0b10000000 | c & 0b00111111);

            } else if (c <= 0b1111_111111_111111) { // 1110xxxx 10xxxxxx 10xxxxxx
                this.stream.write(0b11100000 | c >>> 12);
                this.stream.write(0b10000000 | c >>> 6 & 0b00111111);
                this.stream.write(0b10000000 | c & 0b00111111);

            } else if (c <= 0b111_111111_111111_111111) { // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                this.stream.write(0b11110000 | c >>> 18);
                this.stream.write(0b10000000 | c >>> 12 & 0b00111111);
                this.stream.write(0b10000000 | c >>> 6 & 0b00111111);
                this.stream.write(0b10000000 | c & 0b00111111);

            } else {
                throw new IOException("Invalid code point " + c);
            }
        }

        @Override
        public void write(final char[] cbuf, final int off, final int len) throws IOException {
            final int end = off + len;
            for (int index = off; index < end; ++index) {
                write(cbuf[index]);
            }
        }

        @Override
        public void write(final String str, final int off, final int len) throws IOException {
            final int end = off + len;
            for (int index = off; index < end; ++index) {
                write(str.charAt(index));
            }
        }

        @Override
        public void flush() throws IOException {
            this.stream.flush();
        }

        @Override
        public void close() throws IOException {
            synchronized (this) {
                if (this.closed) {
                    return;
                }
                this.closed = true;
            }
            this.stream.close();
        }

    }

}
