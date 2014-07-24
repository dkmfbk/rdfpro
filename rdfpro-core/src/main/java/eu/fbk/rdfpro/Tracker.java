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

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.slf4j.Logger;

final class Tracker {

    private final Logger logger;

    @Nullable
    private final String startMessage;

    @Nullable
    private final String endMessage;

    @Nullable
    private final String statusKey;

    @Nullable
    private final String statusMessage;

    private final AtomicLong counter;

    private long counterAtTs = 0;

    private long ts0;

    private long ts1;

    private long ts;

    private long chunkSize;

    public Tracker(@Nullable final Logger logger, @Nullable final String startMessage,
            @Nullable final String endMessage, @Nullable final String statusKey,
            @Nullable final String statusMessage) {
        this.logger = Util.checkNotNull(logger);
        this.startMessage = startMessage;
        this.endMessage = endMessage;
        this.statusKey = statusKey;
        this.statusMessage = statusMessage;
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
            synchronized (this) {
                ++counter;
                final long ts = System.currentTimeMillis();
                this.ts1 = ts;
                if (counter == 1) {
                    this.ts0 = ts;
                    this.ts = ts;
                }
                final long delta = ts - this.ts0;
                if (delta > 0) {
                    final long avgThroughput = counter * 1000 / delta;
                    this.chunkSize = avgThroughput < 10 ? 1
                            : avgThroughput < 10000 ? avgThroughput / 10 : 1000;
                    if (ts / 1000 - this.ts / 1000 >= 1) {
                        final long throughput = (counter - this.counterAtTs) * 1000
                                / (ts - this.ts);
                        this.ts = ts;
                        this.counterAtTs = counter;
                        Util.registerStatus(this.statusKey, String.format(this.statusMessage,
                                counter - 1, throughput, avgThroughput));
                    }
                }
            }
        }
    }

    public void end() {
        if (this.statusMessage != null) {
            Util.registerStatus(this.statusKey, null);
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

}
