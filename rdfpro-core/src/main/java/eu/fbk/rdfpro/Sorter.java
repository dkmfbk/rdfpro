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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class Sorter implements Closeable {

    public static char DELIMITER = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(Sorter.class);

    private boolean parallelDecode;

    @Nullable
    private Process sortProcess;

    @Nullable
    private Writer sortWriter;

    @Nullable
    private Reader sortReader;

    @Nullable
    private final List<Writer> encodeWriters;

    @Nullable
    private final ThreadLocal<Writer> encodeWriter;

    private List<Reader> decodeReaders;

    private CountDownLatch decodeLatch;

    private Throwable exception;

    public Sorter(final boolean parallelDecode) {
        this.parallelDecode = parallelDecode;
        this.encodeWriters = new ArrayList<Writer>();
        this.encodeWriter = new ThreadLocal<Writer>() {

            @Override
            protected Writer initialValue() {
                final Writer writer = Streams.buffer(Sorter.this.sortWriter, DELIMITER);
                synchronized (Sorter.this.encodeWriters) {
                    Sorter.this.encodeWriters.add(writer);
                }
                return writer;
            }

        };
    }

    public void start() throws IOException {

        // Invoke sort
        final List<String> command = new ArrayList<String>(Arrays.asList(Util.settingFor(
                "rdfp.cmd.sort", "sort").split("\\s+")));
        command.add("-z"); // zero-terminated lines
        command.add("-u"); // remove duplicates
        final ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("LC_ALL", "C"); // case sensitive sort
        this.sortProcess = builder.start();

        // Retrieve input and output streams
        this.sortWriter = new OutputStreamWriter(this.sortProcess.getOutputStream(),
                Charset.forName("UTF-8"));
        this.sortReader = new InputStreamReader(this.sortProcess.getInputStream(),
                Charset.forName("UTF-8"));

        // Launch a task to log STDERR at ERROR level
        Threads.getMiscPool().submit(new Runnable() {

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
                    Util.closeQuietly(in);
                }
            }

        });
    }

    public Writer getWriter() {
        return this.encodeWriter.get();
    }

    public void end() throws Throwable {

        // Complete data sending to sort
        for (final Writer threadWriter : this.encodeWriters) {
            threadWriter.close();
        }
        this.encodeWriters.clear();
        this.sortWriter.close();

        // Callback
        startDecode();

        // Consume sort output, possibly using multiple decode threads
        final int decoders = this.parallelDecode ? Threads.CORES : 1;
        this.decodeLatch = new CountDownLatch(decoders);
        this.decodeReaders = new ArrayList<Reader>();
        try {
            for (int i = 0; i < decoders; ++i) {
                this.decodeReaders.add(Streams.buffer(this.sortReader, DELIMITER));
            }
            for (int i = 1; i < decoders; ++i) {
                final Reader reader = this.decodeReaders.get(i);
                Threads.getMainPool().execute(new Runnable() {

                    @Override
                    public void run() {
                        tryDecode(reader);
                    }

                });

            }
            tryDecode(this.decodeReaders.get(0));
            this.decodeLatch.await();
        } finally {
            this.sortReader.close();
            if (this.exception != null) {
                throw this.exception;
            }
        }

        // Callback
        endDecode();
    }

    @Override
    public void close() {
        try {
            this.sortProcess.destroy();
        } catch (final Throwable ex) {
            LOGGER.error("Exception caught while killing sort process", ex);
        }
    }

    private void tryDecode(final Reader reader) {
        try {
            decode(reader);
            Util.closeQuietly(reader);
        } catch (final Throwable ex) {
            if (this.exception == null) {
                this.exception = ex;
                for (final Reader decodeReader : this.decodeReaders) {
                    Util.closeQuietly(decodeReader);
                }
            }
        } finally {
            this.decodeLatch.countDown();
        }
    }

    protected void startDecode() throws Throwable {
        // callback that may be overridden by subclasses
    }

    protected abstract void decode(Reader reader) throws Throwable;

    protected void endDecode() throws Throwable {
        // callback that may be overridden by subclasses
    }

}
