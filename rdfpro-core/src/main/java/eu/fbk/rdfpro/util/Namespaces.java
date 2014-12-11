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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;

/**
 * A specialized immutable {@code Namespace} set.
 * <p>
 * A {@code Namespaces} object is an immutable container of {@code prefix -> namespace URI}
 * bindings, where multiple prefixes can map to the same namespace URI but there is at most a
 * binding for any given prefix.
 * </p>
 * <p>
 * Data in this class can be primarily accessed using lookup methods {@link #uriFor(String)},
 * {@link #prefixFor(String)} and {@link #prefixesFor(String)}. In addition, the following views
 * are offered:
 * </p>
 * <ul>
 * <li>{@code set<Namespace object>} view, directly implemented by class instances;</li>
 * <li>{@code prefix -> namespace URI} map view, produced by {@link #uriMap};</li>
 * <li>{@code namespace URI -> prefix} map view, produced by {@link #prefixMap()};</li>
 * <li>{@code set<prefix string>} view, produced by {@link #prefixes()};</li>
 * <li>{@code set<URI string>} view, produced by {@link #uris()};</li>
 * </ul>
 * <p>
 * Instances of this class can be created from:
 * </p>
 * <ul>
 * <li>a {@code Namespace} iterable, by {@link #forIterable(Iterable)};</li>
 * <li>a {@code prefix -> namespace URI} map, by {@link #forURIMap(Map)};</li>
 * <li>a {@code namespace URI -> prefix} map, by {@link #forPrefixMap(Map)};</li>
 * <li>a file identified by a {@code URL}, by {@link #load(URL)};</li>
 * <li>a char stream given by a {@code Reader} object, by {@link #load(Reader)};</li>
 * </ul>
 * <p>
 * This class is immutable and thus thread-safe.
 * </p>
 */
public final class Namespaces extends AbstractSet<Namespace> {

    public static final Namespaces DEFAULT;

    static {
        try {
            DEFAULT = Namespaces.load(Namespaces.class.getResource("prefixes"));
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    private final EntryMap uriMap;

    private final EntryMap prefixMap;

    private final int[] uriTable;

    private final int[] prefixTable;

    private final String[] data; // uri, prefix pairs, sorted by uri, with uris interned

    private final int uriCount;

    private final int neighborhood;

    private Namespaces(final List<URIPrefixPair> pairs) {

        final int size = pairs.size();
        final int tableSize = size + size / 2; // target fill factor = 0.66
        int neighborhood = 4; // first attempt

        this.uriMap = new EntryMap(true);
        this.prefixMap = new EntryMap(false);
        this.prefixTable = new int[2 * tableSize];
        this.uriTable = new int[2 * tableSize];
        this.data = new String[size * 2];

        Collections.sort(pairs);

        int pointer = 0;
        String uri = null;
        int uriCount = 0;
        for (int i = 0; i < size; ++i) {
            final URIPrefixPair pair = pairs.get(i);
            final String prefix = pair.prefix;
            if (!pair.uri.equals(uri)) {
                uri = pair.uri;
                ++uriCount;
            }
            this.data[pointer++] = uri;
            this.data[pointer] = prefix;
            neighborhood = insert(this.uriTable, uri.hashCode(), pointer, neighborhood, null);
            neighborhood = insert(this.prefixTable, prefix.hashCode(), pointer, neighborhood,
                    prefix);
            ++pointer;
        }

        this.uriCount = uriCount;
        this.neighborhood = neighborhood;
    }

    private int insert(final int[] table, final int hash, final int pointer,
            final int neighborhood, final String prefixToCheck) {

        // see http://en.wikipedia.org/wiki/Hopscotch_hashing

        final int buckets = table.length / 2;
        int index = Math.abs(hash) % buckets;
        int offset = index * 2;
        int distance = 0;
        while (table[offset] != 0) {
            if (prefixToCheck != null && table[offset + 1] == hash
                    && prefixToCheck.equals(this.data[table[offset]])) {
                throw new IllegalArgumentException("Duplicate prefix " + prefixToCheck);
            }
            index = (index + 1) % buckets;
            offset = index * 2;
            ++distance;
        }
        if (distance < neighborhood) {
            table[offset] = pointer;
            table[offset + 1] = hash;
            return neighborhood;
        }
        for (int i = 1; i < neighborhood; ++i) {
            int oldIndex = index - i;
            oldIndex = oldIndex >= 0 ? oldIndex : oldIndex + buckets;
            final int oldOffset = oldIndex * 2;
            int oldDistance = index - Math.abs(table[oldOffset + 1]) % buckets;
            oldDistance = oldDistance >= 0 ? oldDistance : oldDistance + buckets;
            if (oldDistance < neighborhood) {
                table[offset] = table[oldOffset];
                table[offset + 1] = table[oldOffset + 1];
                table[oldOffset] = 0;
                table[oldOffset + 1] = 0;
                return insert(table, hash, pointer, neighborhood, prefixToCheck);
            }
        }
        return insert(table, hash, pointer, neighborhood + 1, prefixToCheck);
    }

    private int lookup(final String string, final int[] table, final int field) {
        final int hash = string.hashCode();
        int offset = 2 * (Math.abs(hash) % (table.length / 2));
        for (int i = 0; i < this.neighborhood; ++i) {
            final int prefixIndex = table[offset];
            if (prefixIndex == 0) {
                return -1;
            }
            final int storedHash = table[offset + 1];
            if (storedHash == hash) {
                final int stringIndex = prefixIndex - 1 + field;
                final String storedString = this.data[stringIndex];
                if (storedString.equals(string)) {
                    return prefixIndex - 1;
                }
            }
            offset = (offset + 2) % table.length;
        }
        return -1;
    }

    /**
     * Creates a {@code Namespaces} set by loading the namespace declarations in the file at the
     * URL specified. The file must be a text file encoded in UTF-8. Each line has the format
     * {@code namespace_URI prefix_1 ... prefix_N}, where components are separated by whitespaces.
     *
     * @param url
     *            the URL identifying the file to load, not null
     * @return the created {@code Namespaces} object
     * @throws IOException
     *             on IO error
     * @throws IllegalArgumentException
     *             in case a prefix is associated to different namespace URIs
     */
    public static Namespaces load(final URL url) throws IOException, IllegalArgumentException {
        try (final Reader reader = new InputStreamReader(url.openStream(),
                Charset.forName("UTF-8"))) {
            return load(reader);
        }
    }

    /**
     * Creates a {@code Namespaces} set by loading the namespace declarations from the char
     * stream. Each line in the stream must have the format
     * {@code namespace_URI prefix_1 ... prefix_N}, where components are separated by whitespaces.
     *
     * @param reader
     *            the {@code Reader} where to read from, not null
     * @return the created {@code Namespaces} object
     * @throws IOException
     *             on IO error
     * @throws IllegalArgumentException
     *             in case a prefix is associated to different namespace URIs
     */
    public static Namespaces load(final Reader reader) throws IOException,
            IllegalArgumentException {

        final List<URIPrefixPair> pairs = new ArrayList<>();
        final BufferedReader in = reader instanceof BufferedReader ? (BufferedReader) reader
                : new BufferedReader(reader);

        String line;
        while ((line = in.readLine()) != null) {
            final String[] tokens = line.split("\\s+");
            for (int i = 1; i < tokens.length; ++i) {
                pairs.add(new URIPrefixPair(tokens[0], tokens[i]));
            }
        }

        return new Namespaces(pairs);
    }

    /**
     * Returns a {@code Namespaces} set for the {@code Namespace} {@code Iterable} supplied. In
     * case the {@code Iterable} is itself a {@code Namespaces} object, it will be directly
     * returned.
     *
     * @param iterable
     *            the {@code Iterable} where to load namespaces from, not null
     * @return the corresponding {@code Namespaces} object
     * @throws IllegalArgumentException
     *             in case the {@code Iterable} associates the same prefix to different namespace
     *             URIs
     */
    public static Namespaces forIterable(final Iterable<? extends Namespace> iterable)
            throws IllegalArgumentException {
        if (iterable instanceof Namespaces) {
            return (Namespaces) iterable;
        }
        final int size = iterable instanceof Collection ? ((Collection<?>) iterable).size() : 1024;
        final List<URIPrefixPair> pairs = new ArrayList<URIPrefixPair>(size);
        for (final Namespace namespace : iterable) {
            pairs.add(new URIPrefixPair(namespace.getName(), namespace.getPrefix()));
        }
        return new Namespaces(pairs);
    }

    /**
     * Returns a {@code Namespaces} set for the {@code prefix -> namespace URI} map supplied. In
     * case the map is the {@link #uriMap()} of another {@code Namespaces} object, that object
     * will be directly returned.
     *
     * @param map
     *            the {@code prefix -> namespace URI} map, not null
     * @return the corresponding {@code Namespaces} object
     */
    public static Namespaces forURIMap(final Map<String, String> map) {

        if (map instanceof EntryMap) {
            final EntryMap entryMap = (EntryMap) map;
            if (entryMap.keyIsPrefix) {
                return entryMap.getNamespaces();
            }
        }

        final List<URIPrefixPair> pairs = new ArrayList<URIPrefixPair>(map.size());
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            pairs.add(new URIPrefixPair(entry.getValue(), entry.getKey()));
        }
        return new Namespaces(pairs);
    }

    /**
     * Returns a {@code Namespaces} set for the {@code namespace URI -> URI} map supplied. In case
     * the map is the {@link #prefixMap()} of another {@code Namespaces} object, that object will
     * be directly returned.
     *
     * @param map
     *            the {@code namespace URI -> prefix} map, not null
     * @return the corresponding {@code Namespaces} object
     * @throws IllegalArgumentException
     *             in case the map associates the same prefix to multiple namespace URIs
     */
    public static Namespaces forPrefixMap(final Map<String, String> map)
            throws IllegalArgumentException {

        if (map instanceof EntryMap) {
            final EntryMap entryMap = (EntryMap) map;
            if (!entryMap.keyIsPrefix) {
                return entryMap.getNamespaces();
            }
        }

        final List<URIPrefixPair> pairs = new ArrayList<URIPrefixPair>(map.size());
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            pairs.add(new URIPrefixPair(entry.getKey(), entry.getValue()));
        }
        return new Namespaces(pairs);
    }

    @Override
    public int size() {
        return this.data.length / 2;
    }

    @Override
    public boolean isEmpty() {
        return this.data.length > 0;
    }

    @Override
    public boolean contains(@Nullable final Object object) {
        if (object instanceof Namespace) {
            final Namespace ns = (Namespace) object;
            return Objects.equals(ns.getName(), uriFor(ns.getPrefix()));
        }
        return false;
    }

    /**
     * Returns a {@code Set} view of the prefixes in this {@code Namespace} set.
     *
     * @return a {@code Set} view of prefixes
     */
    public Set<String> prefixes() {
        return this.uriMap.keySet();
    }

    /**
     * Returns all the prefixes bound to the namespace URI specified.
     *
     * @param uri
     *            the namespace URI, not null
     * @return a non-null list with the prefixes bound to the give URI
     */
    public List<String> prefixesFor(final String uri) {
        int index = lookup(uri, this.uriTable, 0);
        if (index < 0) {
            return null;
        }
        final String storedURI = this.data[index];
        final List<String> result = new ArrayList<>();
        while (this.data[index] == storedURI && index < this.data.length) {
            result.add(this.data[index + 1]);
            index += 2;
        }
        return result;
    }

    /**
     * Returns the first prefix bound to the namespace URI specified, or null if it does not
     * exist.
     *
     * @param uri
     *            the namespace URI, not null
     * @return the first prefix bound to the namespace URI specified, if it exists, otherwise null
     */
    @Nullable
    public String prefixFor(final String uri) {
        final int index = lookup(uri, this.uriTable, 0);
        return index < 0 ? null : this.data[index + 1];
    }

    /**
     * Returns a {@code namespace URI -> prefix} map view. Note that the map will report only the
     * first prefix bound to a namespace URI, in case multiple prefixes are defined for that URI.
     *
     * @return a {@code namespace URI -> prefix} map view
     */
    public Map<String, String> prefixMap() {
        return this.prefixMap;
    }

    /**
     * Returns a {@code Set} view of the namespace URIs in this {@code Namespace} set.
     *
     * @return a {@code Set} view of namespace URIs
     */
    public Set<String> uris() {
        return this.prefixMap.keySet();
    }

    /**
     * Returns the namespace URI for the prefix specified, or null if it does not exist.
     *
     * @param prefix
     *            the prefix, not null
     * @return the namespace URI for the prefix specified, if it exists, otherwise null
     */
    @Nullable
    public String uriFor(final String prefix) {
        final int index = lookup(prefix, this.prefixTable, 1);
        return index < 0 ? null : this.data[index];
    }

    /**
     * Returns a {@code prefix -> namespace URI} map view.
     *
     * @return a {@code prefix -> namespace URI} map view
     */
    public Map<String, String> uriMap() {
        return this.uriMap;
    }

    @Override
    public Iterator<Namespace> iterator() {
        return new NamespaceIterator();
    }

    @Override
    public Namespace[] toArray() {
        return super.toArray(new Namespace[size()]);
    }

    private class EntryMap extends AbstractMap<String, String> {

        final boolean keyIsPrefix;

        final EntrySet entries;

        EntryMap(final boolean keyIsPrefix) {
            this.keyIsPrefix = keyIsPrefix;
            this.entries = new EntrySet(keyIsPrefix);
        }

        Namespaces getNamespaces() {
            return Namespaces.this;
        }

        @Override
        public Set<Map.Entry<String, String>> entrySet() {
            return this.entries;
        }

        @Override
        public String get(final Object key) {
            if (key instanceof String) {
                final String s = (String) key;
                return this.keyIsPrefix ? uriFor(s) : prefixFor(s);
            }
            return null;
        }

        @Override
        public boolean containsKey(final Object key) {
            if (key instanceof String) {
                final String s = (String) key;
                return this.keyIsPrefix ? uriFor(s) != null : prefixFor(s) != null;
            }
            return false;
        }

        @Override
        public boolean containsValue(final Object value) {
            if (value instanceof String) {
                final String s = (String) value;
                return this.keyIsPrefix ? prefixFor(s) != null : uriFor(s) != null;
            }
            return false;
        }

    }

    private class EntrySet extends AbstractSet<Map.Entry<String, String>> {

        final boolean keyIsPrefix;

        EntrySet(final boolean keyIsPrefix) {
            this.keyIsPrefix = keyIsPrefix;
        }

        @Override
        public Iterator<Map.Entry<String, String>> iterator() {
            return new EntryIterator(this.keyIsPrefix);
        }

        @Override
        public int size() {
            return this.keyIsPrefix ? Namespaces.this.size() : Namespaces.this.uriCount;
        }

        @Override
        public boolean contains(final Object object) {
            if (object instanceof Map.Entry<?, ?>) {
                final Map.Entry<?, ?> entry = (Map.Entry<?, ?>) object;
                if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                    final String key = (String) entry.getKey();
                    final String value = (String) entry.getValue();
                    return this.keyIsPrefix ? Objects.equals(uriFor(key), value) : Objects.equals(
                            prefixFor(key), value);
                }
            }
            return false;
        }

    }

    private class EntryIterator implements Iterator<Map.Entry<String, String>> {

        final boolean prefixToUri;

        int offset;

        EntryIterator(final boolean prefixToUri) {
            this.prefixToUri = prefixToUri;
            this.offset = 0;
        }

        @Override
        public boolean hasNext() {
            return this.offset < Namespaces.this.data.length;
        }

        @Override
        public Entry<String, String> next() {
            final String uri = Namespaces.this.data[this.offset];
            final String prefix = Namespaces.this.data[this.offset + 1];
            this.offset += 2;
            if (this.prefixToUri) {
                return new AbstractMap.SimpleImmutableEntry<String, String>(prefix, uri);
            } else {
                while (this.offset < Namespaces.this.data.length
                        && Namespaces.this.data[this.offset] == Namespaces.this.data[this.offset - 2]) {
                    this.offset += 2;
                }
                return new AbstractMap.SimpleImmutableEntry<String, String>(uri, prefix);
            }
        }

    }

    private class NamespaceIterator implements Iterator<Namespace> {

        int offset;

        @Override
        public boolean hasNext() {
            return this.offset < Namespaces.this.data.length;
        }

        @Override
        public Namespace next() {
            final String uri = Namespaces.this.data[this.offset];
            final String prefix = Namespaces.this.data[this.offset + 1];
            this.offset += 2;
            return new NamespaceImpl(prefix, uri);
        }

    }

    private static class URIPrefixPair implements Comparable<URIPrefixPair> {

        public final String uri;

        public final String prefix;

        public URIPrefixPair(final String uri, final String prefix) {
            this.uri = Objects.requireNonNull(uri);
            this.prefix = Objects.requireNonNull(prefix);
        }

        @Override
        public int compareTo(final URIPrefixPair other) {
            int result = this.uri.compareTo(other.uri);
            if (result == 0) {
                result = this.prefix.compareTo(other.prefix);
            }
            return result;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            }
            if (!(object instanceof URIPrefixPair)) {
                return false;
            }
            final URIPrefixPair other = (URIPrefixPair) object;
            return this.uri.equals(other.uri) && this.prefix.equals(other.prefix);
        }

        @Override
        public int hashCode() {
            return this.uri.hashCode() ^ this.prefix.hashCode();
        }

    }

}
