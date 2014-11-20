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

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Util {

    public static final int CORES = Integer.parseInt(System.getProperty("rdfp.cores", ""
            + Runtime.getRuntime().availableProcessors()));

    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

    private static final Map<String, String> SETTINGS = new HashMap<String, String>();

    private static final ExecutorService POOL = Executors
            .newCachedThreadPool(new ThreadFactoryImpl("rdfpro-%d", Thread.NORM_PRIORITY, true));

    public static ExecutorService getPool() {
        return POOL;
    }

    // OK

    public static String settingFor(final String key, @Nullable final String defaultValue) {
        synchronized (SETTINGS) {
            String setting = SETTINGS.get(key);
            if (setting == null) {
                setting = System.getenv(key.toUpperCase().replace('.', '_'));
                if (setting == null) {
                    setting = System.getProperty(key.toLowerCase());
                }
                if (setting != null) {
                    LOGGER.info("using '{}' for '{}'", setting, key);
                } else {
                    setting = defaultValue;
                }
                SETTINGS.put(key, setting);
            }
            return setting;
        }
    }

    public static String[] tokenize(final String spec) {

        final List<String> tokens = new ArrayList<String>();

        final StringBuilder builder = new StringBuilder();
        boolean quoted = false;
        boolean escaped = false;
        int start = -1;

        for (int i = 0; i < spec.length(); ++i) {
            final char ch = spec.charAt(i);
            final boolean ws = Character.isWhitespace(ch);
            if (ch == '\\' && !escaped) {
                escaped = true;
            } else {
                if (start < 0) {
                    if (!ws) {
                        start = i;
                        quoted = ch == '\'' || ch == '\"';
                        builder.setLength(0);
                        if (!quoted) {
                            builder.append(ch);
                        }
                    }
                } else {
                    final boolean tokenChar = escaped || quoted && ch != spec.charAt(start)
                            || !quoted && !ws;
                    if (tokenChar) {
                        builder.append(ch);
                    }
                    if (!tokenChar || i == spec.length() - 1) {
                        tokens.add(builder.toString());
                        start = -1;
                        quoted = false;
                    }
                }
                escaped = false;
            }
        }

        return tokens.toArray(new String[tokens.size()]);
    }

    // OK - hashing

    public static long[] murmur3(final String... args) {

        long h1 = 0;
        long h2 = 0;
        int length = 0;

        long l1 = 0;
        long l2 = 0;
        int index = 0;

        long cur = 0;
        for (int i = 0; i < args.length; ++i) {
            final boolean lastArg = i == args.length - 1;
            final String arg = args[i];
            for (int j = 0; j < arg.length(); ++j) {
                final long c = arg.charAt(j) & 0xFFFFL;
                cur = cur | c << index % 4 * 16;
                boolean process = false;
                if (lastArg && j == arg.length() - 1) {
                    l1 = index <= 3 ? cur : l1;
                    l2 = index > 3 ? cur : l2;
                    cur = 0;
                    process = true;
                } else if (index == 3) {
                    l1 = cur;
                    cur = 0;
                } else if (index == 7) {
                    l2 = cur;
                    cur = 0;
                    process = true;
                }
                if (process) {
                    l1 *= 0x87c37b91114253d5L;
                    l1 = Long.rotateLeft(l1, 31);
                    l1 *= 0x4cf5ad432745937fL;
                    h1 ^= l1;
                    h1 = Long.rotateLeft(h1, 27);
                    h1 += h2;
                    h1 = h1 * 5 + 0x52dce729;
                    l2 *= 0x4cf5ad432745937fL;
                    l2 = Long.rotateLeft(l2, 33);
                    l2 *= 0x87c37b91114253d5L;
                    h2 ^= l2;
                    h2 = Long.rotateLeft(h2, 31);
                    h2 += h1;
                    h2 = h2 * 5 + 0x38495ab5;
                    length += 16;
                    l1 = 0;
                    l2 = 0;
                    index = 0;
                    process = false;
                } else {
                    ++index;
                }
            }
        }

        h1 ^= length;
        h2 ^= length;
        h1 += h2;
        h2 += h1;

        h1 ^= h1 >>> 33;
        h1 *= 0xff51afd7ed558ccdL;
        h1 ^= h1 >>> 33;
        h1 *= 0xc4ceb9fe1a85ec53L;
        h1 ^= h1 >>> 33;

        h2 ^= h2 >>> 33;
        h2 *= 0xff51afd7ed558ccdL;
        h2 ^= h2 >>> 33;
        h2 *= 0xc4ceb9fe1a85ec53L;
        h2 ^= h2 >>> 33;

        h1 += h2;
        h2 += h1;

        return new long[] { h1, h2 };
    }

    public static String murmur3str(final String... args) {
        final long[] longs = murmur3(args);
        final StringBuilder builder = new StringBuilder(8 * args.length);
        int max = 52;
        for (int i = 0; i < longs.length; ++i) {
            long l = longs[i] & 0x7FFFFFFFFFFFFFFFL;
            for (int j = 0; j < 8; ++j) {
                final int n = (int) (l % max);
                l = l / max;
                if (n < 26) {
                    builder.append((char) (65 + n));
                } else if (n < 52) {
                    builder.append((char) (71 + n));
                } else {
                    builder.append((char) (n - 4));
                }
                max = 62;
            }
        }
        return builder.toString();
    }

    // To be checked again

    @Nullable
    public static <T> T closeQuietly(@Nullable final T object) {
        if (object instanceof Closeable) {
            try {
                ((Closeable) object).close();
            } catch (final Throwable ex) {
                LOGGER.error("Error closing " + object.getClass().getSimpleName(), ex);
            }
        }
        return object;
    }

    // OK - guava throwables

    public static RuntimeException propagate(final Throwable ex) {
        if (ex instanceof Error) {
            throw (Error) ex;
        } else if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        } else {
            throw new RuntimeException(ex);
        }
    }

    public static <E extends Exception> void propagateIfPossible(final Throwable ex,
            final Class<E> clazz) throws E {
        if (ex instanceof Error) {
            throw (Error) ex;
        } else if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        } else if (clazz.isInstance(ex)) {
            throw clazz.cast(ex);
        } else {
            throw new RuntimeException(ex);
        }
    }

    // OK - guava checks

    public static <T> T checkNotNull(final T object) {
        if (object == null) {
            throw new NullPointerException();
        }
        return object;
    }

    public static <T> T[] checkNotNull(final T[] objects) {
        if (objects == null) {
            throw new NullPointerException("Array is null");
        }
        for (int i = 0; i < objects.length; ++i) {
            if (objects[i] == null) {
                throw new NullPointerException("Element " + i + " is null");
            }
        }
        return objects;
    }

    private Util() {
    }

    private static final class ThreadFactoryImpl implements ThreadFactory {

        private final String nameFormat;

        private final int priority;

        private final boolean daemon;

        private final AtomicInteger counter;

        ThreadFactoryImpl(final String nameFormat, final int priority, final boolean daemon) {
            this.nameFormat = nameFormat;
            this.priority = priority;
            this.daemon = daemon;
            this.counter = new AtomicInteger(0);
        }

        @Override
        public Thread newThread(final Runnable runnable) {
            final int index = this.counter.getAndIncrement();
            final Thread thread = new Thread(runnable);
            thread.setName(String.format(this.nameFormat, index));
            thread.setPriority(this.priority);
            thread.setDaemon(this.daemon);
            return thread;
        }

    }

}
