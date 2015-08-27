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

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Tracker {

    private static final Logger STATUS_LOGGER = LoggerFactory.getLogger("status."
            + Tracker.class.getName());

    private static final Map<Integer, String> STATUS_DATA = new TreeMap<Integer, String>();

    private static final AtomicInteger STATUS_KEY_COUNTER = new AtomicInteger(0);

    private final Logger logger;

    @Nullable
    private final String startMessage;

    @Nullable
    private final String endMessage;

    @Nullable
    private final String statusMessage;

    @Nullable
    private Integer statusKey;

    private final AtomicLong counter;

    private long counterAtTs = 0;

    private long ts0;

    private long ts1;

    private long ts;

    private long chunkSize;

    public Tracker(final Logger logger, @Nullable final String startMessage,
            @Nullable final String endMessage, @Nullable final String statusMessage) {
        this.logger = Objects.requireNonNull(logger);
        this.startMessage = startMessage;
        this.endMessage = endMessage;
        this.statusMessage = statusMessage;
        this.statusKey = null;
        this.counter = new AtomicLong(0L);
    }

    public void start() {
        this.counter.set(0L);
        this.counterAtTs = 0;
        this.ts0 = 0;
        this.ts1 = 0;
        this.ts = 0;
        this.chunkSize = 1L;
        if (this.startMessage != null) {
            this.logger.info(this.startMessage);
        }
    }

    public void increment() {
        long counter = this.counter.getAndIncrement();
        if (counter % this.chunkSize == 0 && this.statusMessage != null) {
            updateStatus(counter + 1);
        }
    }

    public void add(final long delta) {
        final long counter = this.counter.addAndGet(delta);
        if (this.statusMessage != null
                && (counter - delta) / this.chunkSize < counter / this.chunkSize) {
            updateStatus(counter);
        }
    }

    public void end() {
        if (this.statusMessage != null && this.statusKey != null) {
            registerStatus(this.statusKey, null);
        }
        if (this.endMessage != null) {
            final long ts = this.ts1;
            final long avgThroughput = this.counter.get() * 1000 / (ts - this.ts0 + 1);
            if (this.logger.isInfoEnabled()) {
                this.logger
                        .info(String.format(this.endMessage, this.counter.get(), avgThroughput));
            }
        }
    }

    private synchronized void updateStatus(long counter) {
        synchronized (this) {
            final long ts = System.currentTimeMillis();
            this.ts1 = ts;
            if (this.ts0 == 0) {
                this.ts0 = ts;
                this.ts = ts;
                this.statusKey = STATUS_KEY_COUNTER.getAndIncrement();
            }
            final long delta = ts - this.ts0;
            if (delta > 0) {
                final long avgThroughput = counter * 1000 / delta;
                this.chunkSize = avgThroughput < 10 ? 1
                        : avgThroughput < 10000 ? avgThroughput / 10 : 1000;
                if (ts / 1000 - this.ts / 1000 >= 1) {
                    final long throughput = (counter - this.counterAtTs) * 1000 / (ts - this.ts);
                    this.ts = ts;
                    this.counterAtTs = counter;
                    registerStatus(this.statusKey, String.format(this.statusMessage, counter - 1,
                            throughput, avgThroughput));
                }
            }
        }
    }

    private static void registerStatus(final Integer key, @Nullable final String message) {
        synchronized (STATUS_DATA) {
            if (message == null) {
                STATUS_DATA.remove(key);
            } else {
                STATUS_DATA.put(key, message);
            }
            if (STATUS_LOGGER.isInfoEnabled()) {
                final StringBuilder builder = new StringBuilder();
                int count = 0;
                for (final String value : STATUS_DATA.values()) {
                    if (count == 4) {
                        builder.append(" ..."); // max 4 elements printed
                        break;
                    } else if (count > 0) {
                        builder.append(" | ");
                    }
                    builder.append(value);
                    ++count;
                }
                builder.append((char) 0);
                STATUS_LOGGER.info(builder.toString());
            }
        }
    }

}
