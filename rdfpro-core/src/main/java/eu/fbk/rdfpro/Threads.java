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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

final class Threads {

    public static final int CORES = Integer.parseInt(System.getProperty("rdfp.cores", ""
            + Runtime.getRuntime().availableProcessors()));

    private static ExecutorService mainPool;

    private static ExecutorService backgroundPool;

    private static ExecutorService miscPool;

    public synchronized static ExecutorService getMainPool() {
        if (mainPool == null) {
            mainPool = Executors.newFixedThreadPool(CORES, new ThreadFactoryImpl("rdfp-main-%d",
                    Thread.NORM_PRIORITY, true));
        }
        return mainPool;
    }

    public synchronized static ExecutorService getBackgroundPool() {
        if (backgroundPool == null) {
            backgroundPool = Executors.newFixedThreadPool(CORES, new ThreadFactoryImpl(
                    "rdfp-background-%d", Thread.MIN_PRIORITY, true));
        }
        return backgroundPool;
    }

    public synchronized static ExecutorService getMiscPool() {
        if (miscPool == null) {
            miscPool = Executors.newCachedThreadPool(new ThreadFactoryImpl("rdfp-misc-%d",
                    Thread.NORM_PRIORITY, true));
        }
        return miscPool;
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
