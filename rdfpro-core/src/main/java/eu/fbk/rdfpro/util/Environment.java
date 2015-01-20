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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Environment {

    private static final Logger LOGGER = LoggerFactory.getLogger(Environment.class);

    private static Map<String, String> configuredProperties = new HashMap<>();

    private static Map<String, String> loadedProperties = new HashMap<>();

    private static Map<String, Optional<String>> frozenProperties = new ConcurrentHashMap<>();

    private static ExecutorService configuredPool = null;

    private static ExecutorService frozenPool = null;

    private static List<Plugin> frozenPlugins = null;

    private static int frozenCores = 0;

    static {
        final Properties properties = new Properties();
        properties.setProperty("rdfpro.cores", "" + Runtime.getRuntime().availableProcessors());
        try {
            final List<URL> urls = new ArrayList<>();
            final ClassLoader cl = Environment.class.getClassLoader();
            for (final String p : new String[] { "META-INF/rdfpro.properties", "rdfpro.properties" }) {
                for (final Enumeration<URL> e = cl.getResources(p); e.hasMoreElements();) {
                    urls.add(e.nextElement());
                }
            }
            for (final URL url : urls) {
                final Reader in = new InputStreamReader(url.openStream(), Charset.forName("UTF-8"));
                try {
                    properties.load(in);
                    LOGGER.debug("Loaded configuration from '" + url + "'");
                } catch (final Throwable ex) {
                    LOGGER.warn("Could not load configuration from '" + url + "' - ignoring", ex);
                } finally {
                    in.close();
                }
            }
        } catch (final IOException ex) {
            LOGGER.warn("Could not complete loading of configuration from classpath resources", ex);
        }
        for (final Map.Entry<?, ?> entry : properties.entrySet()) {
            loadedProperties.put((String) entry.getKey(), (String) entry.getValue());
        }
        for (final Map.Entry<?, ?> entry : System.getProperties().entrySet()) {
            loadedProperties.put((String) entry.getKey(), (String) entry.getValue());
        }
        for (final Map.Entry<String, String> entry : System.getenv().entrySet()) {
            final String key = entry.getKey().toString().toLowerCase().replace('_', '.');
            loadedProperties.put(key, entry.getValue());
        }
    }

    public static void configurePool(@Nullable final ExecutorService pool) {
        synchronized (Environment.class) {
            if (frozenPool != null) {
                throw new IllegalStateException("Thread pool already in use");
            }
            configuredPool = pool; // to be frozen later
        }
    }

    public static void configureProperty(final String name, @Nullable final String value) {
        Objects.requireNonNull(name);
        synchronized (Environment.class) {
            if (frozenPlugins != null && name.startsWith("plugin,")) {
                throw new IllegalStateException("Plugin configuration already loaded");
            }
            if (frozenProperties.containsKey(name)) {
                throw new IllegalStateException("Property " + name + " already in use (value "
                        + frozenProperties.get(name) + ")");
            }
            if (value == null) {
                configuredProperties.remove(name);
            } else {
                configuredProperties.put(name, value);
            }
        }
    }

    public static int getCores() {
        if (frozenCores <= 0) {
            frozenCores = Integer.parseInt(getProperty("rdfpro.cores"));
        }
        return frozenCores;
    }

    public static ExecutorService getPool() {
        if (frozenPool == null) {
            synchronized (Environment.class) {
                if (frozenPool == null) {
                    frozenPool = configuredPool;
                    if (frozenPool == null) {
                        final ThreadFactory factory = new ThreadFactory() {

                            private final AtomicInteger counter = new AtomicInteger(0);

                            @Override
                            public Thread newThread(final Runnable runnable) {
                                final int index = this.counter.getAndIncrement();
                                final Thread thread = new Thread(runnable);
                                thread.setName(String.format("rdfpro-%03d", index));
                                thread.setPriority(Thread.NORM_PRIORITY);
                                thread.setDaemon(true);
                                return thread;
                            }

                        };
                        frozenPool = Executors.newCachedThreadPool(factory);
                    }
                    LOGGER.debug("Using pool {}", frozenPool);
                }
            }
        }
        return frozenPool;
    }

    @Nullable
    public static String getProperty(final String name) {
        Objects.requireNonNull(name);
        Optional<String> holder = frozenProperties.get(name);
        if (holder == null) {
            synchronized (Environment.class) {
                holder = frozenProperties.get(name);
                if (holder == null) {
                    String value;
                    if (configuredProperties.containsKey(name)) {
                        value = configuredProperties.get(name);
                    } else {
                        value = loadedProperties.get(name);
                    }
                    holder = Optional.ofNullable(value);
                    frozenProperties.put(name, holder);
                    if (value != null) {
                        LOGGER.debug("Using {} = {}", name, value);
                    }
                }
            }
        }
        return holder.orElse(null);
    }

    @Nullable
    public static String getProperty(final String name, @Nullable final String valueIfNull) {
        final String value = getProperty(name);
        return value != null ? value : valueIfNull;
    }

    public static Map<String, String> getPlugins(final Class<?> baseClass) {

        Objects.requireNonNull(baseClass);

        if (frozenPlugins == null) {
            loadPlugins();
        }

        final Map<String, String> map = new HashMap<>();
        for (final Plugin plugin : frozenPlugins) {
            if (baseClass.isAssignableFrom(plugin.factory.getReturnType())) {
                map.put(plugin.names.get(0), plugin.description);
            }
        }
        return map;
    }

    public static <T> T newPlugin(final Class<T> baseClass, final String name,
            final String... args) {

        Objects.requireNonNull(baseClass);
        Objects.requireNonNull(name);
        if (Arrays.asList(args).contains(null)) {
            throw new NullPointerException();
        }

        if (frozenPlugins == null) {
            loadPlugins();
        }

        for (final Plugin plugin : frozenPlugins) {
            if (baseClass.isAssignableFrom(plugin.factory.getReturnType())
                    && plugin.names.contains(name)) {
                try {
                    return baseClass.cast(plugin.factory.invoke(null, name, args));
                } catch (final IllegalAccessException ex) {
                    throw new Error("Unexpected error (!)", ex); // checked when loading plugins
                } catch (final InvocationTargetException ex) {
                    final Throwable cause = ex.getCause();
                    throw cause instanceof RuntimeException ? (RuntimeException) cause
                            : new RuntimeException(ex);
                }
            }
        }

        throw new IllegalArgumentException("Unknown plugin " + name + " for class "
                + baseClass.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    private static void loadPlugins() {
        synchronized (Environment.class) {
            if (frozenPlugins != null) {
                return;
            }
            final List<Plugin> plugins = new ArrayList<>();
            for (final Map<String, String> map : new Map[] { loadedProperties,
                    configuredProperties }) {
                for (final Map.Entry<String, String> entry : map.entrySet()) {
                    final String name = entry.getKey();
                    final String value = entry.getValue();
                    if (name.startsWith("plugin,")) {
                        try {
                            final String[] tokens = name.toString().split(",");
                            final String className = tokens[1];
                            final String methodName = tokens[2];
                            final List<String> pluginNames = Arrays.asList(Arrays.copyOfRange(
                                    tokens, 3, tokens.length));
                            final Class<?> clazz = Class.forName(className);
                            final Method method = clazz.getDeclaredMethod(methodName,
                                    String.class, String[].class);
                            method.setAccessible(true);
                            plugins.add(new Plugin(pluginNames, value, method));
                        } catch (final Throwable ex) {
                            LOGGER.warn("Invalid plugin definition " + name
                                    + " in file - ignoring", ex);
                        }
                    }
                }
            }
            frozenPlugins = plugins;
        }
    }

    private static final class Plugin {

        public final List<String> names;

        public final String description;

        public final Method factory;

        Plugin(final List<String> names, final String description, final Method factory) {
            this.names = names;
            this.description = description;
            this.factory = factory;
        }

    }

}
