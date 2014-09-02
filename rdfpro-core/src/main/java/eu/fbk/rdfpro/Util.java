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

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.WriterConfig;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.BasicWriterSettings;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.RDFJSONParserSettings;
import org.openrdf.rio.helpers.TriXParserSettings;
import org.openrdf.rio.helpers.XMLParserSettings;
import org.openrdf.rio.trig.TriGWriter;
import org.openrdf.rio.turtle.TurtleUtil;
import org.openrdf.rio.turtle.TurtleWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

final class Util {

    public static final ValueFactory FACTORY = ValueFactoryImpl.getInstance();

    public static final Map<String, String> NS_TO_PREFIX_MAP;

    public static final Map<String, String> PREFIX_TO_NS_MAP;

    public static final Set<URI> TBOX_CLASSES = Collections.unmodifiableSet(new HashSet<URI>(
            Arrays.asList(RDFS.CLASS, RDFS.DATATYPE, RDF.PROPERTY,
                    FACTORY.createURI(OWL.NAMESPACE, "AllDisjointClasses"),
                    FACTORY.createURI(OWL.NAMESPACE, "AllDisjointProperties"),
                    OWL.ANNOTATIONPROPERTY,
                    FACTORY.createURI(OWL.NAMESPACE, "AsymmetricProperty"), OWL.CLASS,
                    OWL.DATATYPEPROPERTY, OWL.FUNCTIONALPROPERTY, OWL.INVERSEFUNCTIONALPROPERTY,
                    FACTORY.createURI(OWL.NAMESPACE, "IrreflexiveProperty"), OWL.OBJECTPROPERTY,
                    OWL.ONTOLOGY, FACTORY.createURI(OWL.NAMESPACE, "ReflexiveProperty"),
                    OWL.RESTRICTION, OWL.SYMMETRICPROPERTY, OWL.TRANSITIVEPROPERTY)));

    // NOTE: rdf:first and rdf:rest considered as TBox statements as used (essentially) for
    // encoding OWL axioms

    public static final Set<URI> TBOX_PROPERTIES = Collections.unmodifiableSet(new HashSet<URI>(
            Arrays.asList(RDF.FIRST, RDF.REST, RDFS.DOMAIN, RDFS.RANGE, RDFS.SUBCLASSOF,
                    RDFS.SUBPROPERTYOF, OWL.ALLVALUESFROM, OWL.CARDINALITY, OWL.COMPLEMENTOF,
                    FACTORY.createURI(OWL.NAMESPACE, "datatypeComplementOf"),
                    FACTORY.createURI(OWL.NAMESPACE, "disjointUnionOf"), OWL.DISJOINTWITH,
                    OWL.EQUIVALENTCLASS, OWL.EQUIVALENTPROPERTY,
                    FACTORY.createURI(OWL.NAMESPACE, "hasKey"),
                    FACTORY.createURI(OWL.NAMESPACE, "hasSelf"), OWL.HASVALUE, OWL.IMPORTS,
                    OWL.INTERSECTIONOF, OWL.INVERSEOF, OWL.MAXCARDINALITY,
                    FACTORY.createURI(OWL.NAMESPACE, "maxQualifiedCardinality"),
                    FACTORY.createURI(OWL.NAMESPACE, "members"), OWL.MINCARDINALITY,
                    FACTORY.createURI(OWL.NAMESPACE, "minQualifiedCardinality"),
                    FACTORY.createURI(OWL.NAMESPACE, "onClass"),
                    FACTORY.createURI(OWL.NAMESPACE, "onDataRange"),
                    FACTORY.createURI(OWL.NAMESPACE, "onDataType"),
                    FACTORY.createURI(OWL.NAMESPACE, "onProperties"), OWL.ONPROPERTY, OWL.ONEOF,
                    FACTORY.createURI(OWL.NAMESPACE, "propertyChainAxiom"),
                    FACTORY.createURI(OWL.NAMESPACE, "propertyDisjointWith"),
                    FACTORY.createURI(OWL.NAMESPACE, "qualifiedCardinality"), OWL.SOMEVALUESFROM,
                    OWL.UNIONOF, OWL.VERSIONIRI,
                    FACTORY.createURI(OWL.NAMESPACE, "withRestrictions"))));

    private static final Logger LOGGER = LoggerFactory.getLogger(Util.class);

    private static final Map<String, String> SETTINGS = new HashMap<String, String>();

    private static final Logger STATUS_LOGGER = LoggerFactory.getLogger("status."
            + Util.class.getName());

    private static final Map<String, String> STATUS_DATA = new TreeMap<String, String>();

    static {
        try {
            final Map<String, String> nsToPrefixMap = new HashMap<String, String>();
            final Map<String, String> prefixToNsMap = new HashMap<String, String>();
            parseNamespaces(Util.class.getResource("prefixes"), nsToPrefixMap, prefixToNsMap);
            NS_TO_PREFIX_MAP = Collections.unmodifiableMap(nsToPrefixMap);
            PREFIX_TO_NS_MAP = Collections.unmodifiableMap(prefixToNsMap);
        } catch (final IOException ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    public static void registerStatus(final String key, @Nullable final String message) {
        synchronized (STATUS_DATA) {
            if (message == null) {
                STATUS_DATA.remove(key);
            } else {
                STATUS_DATA.put(key, message);
            }
            STATUS_LOGGER.info(Util.join(" | ", STATUS_DATA.values()) + (char) 0);
        }
    }

    public static void parseNamespaces(final URL resource,
            @Nullable final Map<String, String> nsToPrefixMap,
            @Nullable final Map<String, String> prefixToNsMap) throws IOException {

        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                resource.openStream(), Charset.forName("UTF-8")));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                final String[] tokens = line.split("\\s+");
                if (tokens.length >= 2) {
                    if (nsToPrefixMap != null) {
                        nsToPrefixMap.put(tokens[0], tokens[1]);
                    }
                    if (prefixToNsMap != null) {
                        for (int i = 1; i < tokens.length; ++i) {
                            prefixToNsMap.put(tokens[i], tokens[0]);
                        }
                    }
                }
            }
        } finally {
            reader.close();
        }
    }

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

    @Nullable
    static File toRDFFile(final String fileSpec) {
        final int index = fileSpec.indexOf(':');
        if (index > 0 && RDFFormat.forFileName("test." + fileSpec.substring(0, index)) != null) {
            return new File(fileSpec.substring(index + 1));
        }
        return new File(fileSpec);
    }

    static RDFFormat toRDFFormat(final String fileSpec) {
        final int index = fileSpec.indexOf(':');
        if (index > 0) {
            final RDFFormat format = RDFFormat.forFileName("test." + fileSpec.substring(0, index));
            if (format != null) {
                return format;
            }
        }
        final RDFFormat format = RDFFormat.forFileName(fileSpec);
        if (format == null) {
            throw new IllegalArgumentException("Unknown RDF format for " + fileSpec);
        }
        return format;
    }

    static boolean isRDFFormatTextBased(final RDFFormat format) {
        for (final String ext : format.getFileExtensions()) {
            if (ext.equalsIgnoreCase("rdf") || ext.equalsIgnoreCase("rj")
                    || ext.equalsIgnoreCase("jsonld") || ext.equalsIgnoreCase("nt")
                    || ext.equalsIgnoreCase("nq") || ext.equalsIgnoreCase("trix")
                    || ext.equalsIgnoreCase("trig") || ext.equalsIgnoreCase("tql")
                    || ext.equalsIgnoreCase("ttl") || ext.equalsIgnoreCase("n3")) {
                return true;
            }
        }
        return false;
    }

    static boolean isRDFFormatLineBased(final RDFFormat format) {
        for (final String ext : format.getFileExtensions()) {
            if (ext.equalsIgnoreCase("nt") || ext.equalsIgnoreCase("nq")
                    || ext.equalsIgnoreCase("tql")) {
                return true;
            }
        }
        return false;
    }

    public static boolean isTBoxStatement(final Statement statement) {
        final URI p = statement.getPredicate();
        final Value o = statement.getObject();
        return TBOX_PROPERTIES.contains(p) || p.equals(RDF.TYPE) && TBOX_CLASSES.contains(o);
    }

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

    public static RDFParser newRDFParser(final RDFFormat format) {

        final RDFParser parser = Rio.createParser(format);
        parser.setValueFactory(FACTORY);
        final ParserConfig config = parser.getParserConfig();

        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);
        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES, false);
        config.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
        config.set(BasicParserSettings.VERIFY_LANGUAGE_TAGS, false);
        config.set(BasicParserSettings.VERIFY_RELATIVE_URIS, false);
        config.set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, true);
        config.set(BasicParserSettings.NORMALIZE_LANGUAGE_TAGS, true);
        config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);

        if (format.equals(RDFFormat.NTRIPLES)) {
            config.set(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES, false);

        } else if (format.equals(RDFFormat.RDFJSON)) {
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_DATATYPES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_LANGUAGES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_TYPES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_VALUES, false);
            config.set(RDFJSONParserSettings.FAIL_ON_UNKNOWN_PROPERTY, false);
            config.set(RDFJSONParserSettings.SUPPORT_GRAPHS_EXTENSION, true);

        } else if (format.equals(RDFFormat.TRIX)) {
            config.set(TriXParserSettings.FAIL_ON_TRIX_INVALID_STATEMENT, false);
            config.set(TriXParserSettings.FAIL_ON_TRIX_MISSING_DATATYPE, false);

        } else if (format.equals(RDFFormat.RDFXML)) {
            config.set(XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID, false);
            config.set(XMLParserSettings.FAIL_ON_INVALID_NCNAME, false);
            config.set(XMLParserSettings.FAIL_ON_INVALID_QNAME, false);
            config.set(XMLParserSettings.FAIL_ON_MISMATCHED_TAGS, false);
            config.set(XMLParserSettings.FAIL_ON_NON_STANDARD_ATTRIBUTES, false);
            config.set(XMLParserSettings.FAIL_ON_SAX_NON_FATAL_ERRORS, false);
        }

        return parser;
    }

    public static RDFWriter newRDFWriter(final RDFFormat format, final Closeable streamOrWriter) {

        RDFWriter writer;
        if (streamOrWriter instanceof OutputStream) {
            writer = Rio.createWriter(format, (OutputStream) streamOrWriter);
        } else if (format.equals(RDFFormat.TURTLE)) {
            writer = patchedTurtleWriter((Writer) streamOrWriter);
        } else if (format.equals(RDFFormat.TRIG)) {
            writer = patchedTriGWriter((Writer) streamOrWriter);
        } else {
            writer = Rio.createWriter(format, (Writer) streamOrWriter);
        }

        final WriterConfig config = writer.getWriterConfig();
        config.set(BasicWriterSettings.PRETTY_PRINT, true);
        config.set(BasicWriterSettings.RDF_LANGSTRING_TO_LANG_LITERAL, true);
        config.set(BasicWriterSettings.XSD_STRING_TO_PLAIN_LITERAL, true);
        return writer;
    }

    private static RDFWriter patchedTurtleWriter(final Writer writer) {
        return new TurtleWriter(writer) {

            @Override
            protected void writeURI(final URI uri) throws IOException {
                patchedWriteURI(uri, this.namespaceTable, this.writer);
            }

        };
    }

    private static RDFWriter patchedTriGWriter(final Writer writer) {
        return new TriGWriter(writer) {

            @Override
            protected void writeURI(final URI uri) throws IOException {
                patchedWriteURI(uri, this.namespaceTable, this.writer);
            }

        };
    }

    private static void patchedWriteURI(final URI uri, final Map<String, String> namespaceTable,
            final Writer writer) throws IOException {
        final String uriString = uri.toString();

        // Try to find a prefix for the URI's namespace
        String prefix = null;

        final int splitIdx = patchedFindURISplitIndex(uriString);
        if (splitIdx > 0) {
            final String namespace = uriString.substring(0, splitIdx);
            prefix = namespaceTable.get(namespace);
        }

        if (prefix != null) {
            // Namespace is mapped to a prefix; write abbreviated URI
            writer.write(prefix);
            writer.write(":");
            writer.write(uriString.substring(splitIdx));
        } else {
            // Write full URI
            writer.write("<");
            writer.write(TurtleUtil.encodeURIString(uriString));
            writer.write(">");
        }
    }

    private static int patchedFindURISplitIndex(final String uri) {
        final int uriLength = uri.length();

        int idx = uriLength - 1;

        // BEGIN FIX
        final char ch = uri.charAt(idx);
        if (ch == '.' || ch == '\\') {
            return -1;
        }
        // END FIX

        // Search last character that is not a name character
        for (; idx >= 0; idx--) {
            if (!TurtleUtil.isNameChar(uri.charAt(idx))) {
                // Found a non-name character
                break;
            }
        }

        idx++;

        // Local names need to start with a 'nameStartChar', skip characters
        // that are not nameStartChar's.
        for (; idx < uriLength; idx++) {
            if (TurtleUtil.isNameStartChar(uri.charAt(idx))) {
                break;
            }
        }

        if (idx > 0 && idx < uriLength) {
            // A valid split index has been found
            return idx;
        }

        // No valid local name has been found
        return -1;
    }

    public static Value shortenValue(final Value value, final int threshold) {
        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            final URI datatype = literal.getDatatype();
            final String language = literal.getLanguage();
            final String label = ((Literal) value).getLabel();
            if (label.length() > threshold
                    && (datatype == null || datatype.equals(XMLSchema.STRING))) {
                int offset = threshold;
                for (int i = threshold; i >= 0; --i) {
                    if (Character.isWhitespace(label.charAt(i))) {
                        offset = i;
                        break;
                    }
                }
                final String newLabel = label.substring(0, offset) + "...";
                if (datatype != null) {
                    return FACTORY.createLiteral(newLabel, datatype);
                } else if (language != null) {
                    return FACTORY.createLiteral(newLabel, language);
                } else {
                    return FACTORY.createLiteral(newLabel);
                }
            }
        }
        return value;
    }

    public static String formatValue(final Value value) {
        final StringBuilder builder = new StringBuilder();
        formatValue(value, builder);
        return builder.toString();
    }

    public static void formatValue(final Value value, final StringBuilder builder) {

        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            builder.append('\"').append(literal.getLabel()).append('\"');
            final String language = literal.getLanguage();
            if (language != null) {
                builder.append('@').append(language);
            }
            final URI datatype = literal.getDatatype();
            if (datatype != null) {
                builder.append('^').append('^');
                formatValue(datatype, builder);
            }

        } else if (value instanceof BNode) {
            final BNode bnode = (BNode) value;
            builder.append('_').append(':').append(bnode.getID());

        } else if (value instanceof URI) {
            final URI uri = (URI) value;
            final String prefix = NS_TO_PREFIX_MAP.get(uri.getNamespace());
            if (prefix != null) {
                builder.append(prefix).append(":").append(uri.getLocalName());
            } else {
                builder.append('<').append(uri.stringValue()).append('>');
            }
        } else {
            throw new Error("Unknown value type (!): " + value);
        }
    }

    public static Value parseValue(final String string) {

        final int length = string.length();
        if (string.startsWith("\"") || string.startsWith("'")) {
            if (string.charAt(length - 1) == '"' || string.charAt(length - 1) == '\'') {
                return FACTORY.createLiteral(string.substring(1, length - 1));
            }
            int index = string.lastIndexOf("\"@");
            if (index == length - 4) {
                final String language = string.substring(index + 2);
                if (Character.isLetter(language.charAt(0))
                        && Character.isLetter(language.charAt(1))) {
                    return FACTORY.createLiteral(string.substring(1, index), language);
                }
            }
            index = string.lastIndexOf("\"^^");
            if (index > 0) {
                final String datatype = string.substring(index + 3);
                try {
                    final URI datatypeURI = (URI) parseValue(datatype);
                    return FACTORY.createLiteral(string.substring(1, index), datatypeURI);
                } catch (final Throwable ex) {
                    // ignore
                }
            }
            throw new IllegalArgumentException("Invalid literal: " + string);

        } else if (string.startsWith("_:")) {
            return FACTORY.createBNode(string.substring(2));

        } else if (string.startsWith("<")) {
            return FACTORY.createURI(string.substring(1, length - 1));

        } else {
            final int index = string.indexOf(':');
            final String prefix = string.substring(0, index);
            final String localName = string.substring(index + 1);
            final String namespace = PREFIX_TO_NS_MAP.get(prefix);
            if (namespace != null) {
                return FACTORY.createURI(namespace, localName);
            }
            throw new IllegalArgumentException("Unknown prefix for URI: " + string);
        }
    }

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

    public static Set<String> createHashSet(String pattern, File file) {
        final boolean matchSub = pattern.contains("s");
        final boolean matchPre = pattern.contains("p");
        final boolean matchObj = pattern.contains("o");
        final boolean matchCtx = pattern.contains("c");
        final Set<String> hashes = new TreeSet<>();
        final RDFParser parser = Rio.createParser(Rio.getParserFormatForFileName(file.getName()));
        parser.setRDFHandler(new RDFHandler() {
            @Override
            public void startRDF() throws RDFHandlerException {
            }

            @Override
            public void endRDF() throws RDFHandlerException {
            }

            @Override
            public void handleNamespace(String s, String s2) throws RDFHandlerException {
            }

            @Override
            public void handleStatement(Statement statement) throws RDFHandlerException {
                if(matchSub) hashes.add(murmur3Str(statement.getSubject().stringValue()));
                if(matchPre) hashes.add(murmur3Str(statement.getPredicate().stringValue()));
                if(matchObj) hashes.add(murmur3Str(statement.getObject().stringValue()));
                if(matchCtx) hashes.add(murmur3Str(statement.getContext().stringValue()));
            }

            @Override
            public void handleComment(String s) throws RDFHandlerException {
            }
        });

        try {
            parser.parse(new FileReader(file), "");
        } catch (Exception e) {
            throw new IllegalArgumentException("Error while parsing pattern file.", e);
        }
        return hashes;
    }

    public static Set<String> parseFileFilterRule(String rule) {
        final int lastSeparator = rule.lastIndexOf(']');
        final String filename = rule.substring(2, lastSeparator);
        final String pattern = rule.substring(lastSeparator + 1);
        final File file = new File(filename);
        if(!file.exists()) throw new IllegalArgumentException("Cannot find file " + file.getAbsolutePath());
        return createHashSet(pattern, file);
    }

    public static String join(final String delimiter, final Iterable<?> objects) {
        final StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (final Object object : objects) {
            if (!first) {
                builder.append(delimiter);
            }
            builder.append(object);
            first = false;
        }
        return builder.toString();
    }

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

    public static String toString(final long... longs) {
        final StringBuilder builder = new StringBuilder();
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

    public static String murmur3Str(String...in) {
        return toString(murmur3(in));
    }

    private Util() {
    }

}
