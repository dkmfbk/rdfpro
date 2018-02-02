/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
 *
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 *
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.RioConfig;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;
import org.eclipse.rdf4j.rio.helpers.RDFJSONParserSettings;
import org.eclipse.rdf4j.rio.helpers.TriXParserSettings;
import org.eclipse.rdf4j.rio.helpers.XMLParserSettings;

public final class Statements {

    public static final ValueFactory VALUE_FACTORY;

    static {
        final boolean hashfactory = Boolean
                .parseBoolean(Environment.getProperty("rdfpro.hashfactory", "true"));
        if (hashfactory) {
            VALUE_FACTORY = HashValueFactory.INSTANCE;
        } else {
            VALUE_FACTORY = SimpleValueFactory.getInstance();
        }
    }

    public static final Function<Value, Value> VALUE_NORMALIZER = new Function<Value, Value>() {

        @Override
        public Value apply(final Value value) {
            return Statements.normalize(value);
        }

    };

    public static final DatatypeFactory DATATYPE_FACTORY;

    public static final Set<IRI> TBOX_CLASSES = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList(RDFS.CLASS, RDFS.DATATYPE, RDF.PROPERTY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "AllDisjointClasses"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "AllDisjointProperties"),
                    OWL.ANNOTATIONPROPERTY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "AsymmetricProperty"),
                    OWL.CLASS, OWL.DATATYPEPROPERTY, OWL.FUNCTIONALPROPERTY,
                    OWL.INVERSEFUNCTIONALPROPERTY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "IrreflexiveProperty"),
                    OWL.OBJECTPROPERTY, OWL.ONTOLOGY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "ReflexiveProperty"),
                    OWL.RESTRICTION, OWL.SYMMETRICPROPERTY, OWL.TRANSITIVEPROPERTY)));

    // NOTE: rdf:first and rdf:rest considered as TBox statements as used (essentially) for
    // encoding OWL axioms

    public static final Set<IRI> TBOX_PROPERTIES = Collections.unmodifiableSet(new HashSet<>(
            Arrays.asList(RDF.FIRST, RDF.REST, RDFS.DOMAIN, RDFS.RANGE, RDFS.SUBCLASSOF,
                    RDFS.SUBPROPERTYOF, OWL.ALLVALUESFROM, OWL.CARDINALITY, OWL.COMPLEMENTOF,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "datatypeComplementOf"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "disjointUnionOf"),
                    OWL.DISJOINTWITH, OWL.EQUIVALENTCLASS, OWL.EQUIVALENTPROPERTY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "hasKey"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "hasSelf"), OWL.HASVALUE,
                    OWL.IMPORTS, OWL.INTERSECTIONOF, OWL.INVERSEOF, OWL.MAXCARDINALITY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "maxQualifiedCardinality"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "members"),
                    OWL.MINCARDINALITY,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "minQualifiedCardinality"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "onClass"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "onDataRange"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "onDataType"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "onProperties"),
                    OWL.ONPROPERTY, OWL.ONEOF,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "propertyChainAxiom"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "propertyDisjointWith"),
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "qualifiedCardinality"),
                    OWL.SOMEVALUESFROM, OWL.UNIONOF, OWL.VERSIONIRI,
                    Statements.VALUE_FACTORY.createIRI(OWL.NAMESPACE, "withRestrictions"))));

    private static final Comparator<Value> DEFAULT_VALUE_ORDERING = new ValueComparator(false);

    private static final Comparator<Statement> DEFAULT_STATEMENT_ORDERING = new StatementComparator(
            "spoc", new ValueComparator(false, RDF.NAMESPACE));

    static {
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!): " + ex.getMessage(), ex);
        }
    }

    private static final Hash NIL_HASH = Hash.murmur3("\u0001");

    public static Comparator<Value> valueComparator(final String... rankedNamespaces) {
        return valueComparator(false, rankedNamespaces);
    }

    public static Comparator<Value> valueComparator(final boolean shortestFirst,
            final String... rankedNamespaces) {
        return !shortestFirst && (rankedNamespaces == null || rankedNamespaces.length == 0)
                ? Statements.DEFAULT_VALUE_ORDERING
                : new ValueComparator(shortestFirst, rankedNamespaces);
    }

    public static Comparator<Statement> statementComparator(@Nullable final String components,
            @Nullable final Comparator<? super Value> valueComparator) {
        if (components == null) {
            return valueComparator == null ? Statements.DEFAULT_STATEMENT_ORDERING //
                    : new StatementComparator("spoc", valueComparator);
        } else {
            return new StatementComparator(components,
                    valueComparator == null ? Statements.DEFAULT_VALUE_ORDERING : valueComparator);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static Predicate<Statement> statementMatcher(@Nullable final String spec) {
        if (spec == null) {
            return null;
        } else if (Scripting.isScript(spec)) {
            return Scripting.compile(Predicate.class, spec, "q");
        } else {
            return new StatementMatcher(spec);
        }
    }

    public static Statement normalize(final Statement statement) {
        final Resource subj = Statements.normalize(statement.getSubject());
        final IRI pred = Statements.normalize(statement.getPredicate());
        final Value obj = Statements.normalize(statement.getObject());
        final Resource ctx = Statements.normalize(statement.getContext());
        if (subj == statement.getSubject() && pred == statement.getPredicate()
                && obj == statement.getObject() && ctx == statement.getContext()) {
            return statement;
        } else if (ctx != null) {
            return Statements.VALUE_FACTORY.createStatement(subj, pred, obj, ctx);
        } else {
            return Statements.VALUE_FACTORY.createStatement(subj, pred, obj);
        }
    }

    @Nullable
    public static <T extends Value> T normalize(@Nullable final T value) {
        if (Statements.VALUE_FACTORY instanceof HashValueFactory) {
            return HashValueFactory.normalize(value);
        }
        return value;
    }

    public static Hash getHash(final Statement statement) {
        return statement instanceof Hashable ? ((Hashable) statement).getHash()
                : Statements.computeHash(statement);
    }

    public static Hash getHash(@Nullable final Value value) {
        return value instanceof Hashable ? ((Hashable) value).getHash()
                : Statements.computeHash(value);
    }

    public static Hash computeHash(final Statement statement) {
        final Hash subjHash = Statements.getHash(statement.getSubject());
        final Hash predHash = Statements.getHash(statement.getPredicate());
        final Hash objHash = Statements.getHash(statement.getObject());
        final Hash ctxHash = Statements.getHash(statement.getContext());
        return Hash.combine(subjHash, predHash, objHash, ctxHash);
    }

    public static Hash computeHash(@Nullable final Value value) {
        if (value == null) {
            return Statements.NIL_HASH;
        }
        Hash hash;
        if (value instanceof IRI) {
            hash = Hash.murmur3("\u0001", value.stringValue());
        } else if (value instanceof BNode) {
            hash = Hash.murmur3("\u0002", ((BNode) value).getID());
        } else {
            final Literal l = (Literal) value;
            if (l.getLanguage().isPresent()) {
                hash = Hash.murmur3("\u0003", l.getLanguage().get(), l.getLabel());
            } else if (!l.getDatatype().equals(XMLSchema.STRING)) {
                hash = Hash.murmur3("\u0004", l.getDatatype().stringValue(), l.getLabel());
            } else {
                hash = Hash.murmur3("\u0005", l.getLabel());
            }
        }
        if (hash.getLow() == 0) {
            hash = Hash.fromLongs(hash.getHigh(), 1L);
        }
        return hash;
    }

    @Nullable
    public static File toRDFFile(final String fileSpec) {
        final int index = fileSpec.indexOf(':');
        if (index > 0) {
            final String name = "test." + fileSpec.substring(0, index);
            if (Rio.getParserFormatForFileName(name) != null
                    || Rio.getWriterFormatForFileName(name) != null) {
                return new File(fileSpec.substring(index + 1));
            }
        }
        return new File(fileSpec);
    }

    public static RDFFormat toRDFFormat(final String fileSpec) {
        final int index = fileSpec.indexOf(':');
        RDFFormat format = null;
        if (index > 0) {
            final String name = "test." + fileSpec.substring(0, index);
            format = Rio.getParserFormatForFileName(name).orElse(null);
            if (format == null) {
                format = Rio.getWriterFormatForFileName(name).orElse(null);
            }
        }
        if (format == null) {
            format = Rio.getParserFormatForFileName(fileSpec).orElse(null);
            if (format == null) {
                format = Rio.getWriterFormatForFileName(fileSpec).orElse(null);
            }
        }
        if (format == null) {
            throw new IllegalArgumentException("Unknown RDF format for " + fileSpec);
        }
        return format;
    }

    public static boolean isRDFFormatTextBased(final RDFFormat format) {
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

    public static boolean isRDFFormatLineBased(final RDFFormat format) {
        for (final String ext : format.getFileExtensions()) {
            if (ext.equalsIgnoreCase("nt") || ext.equalsIgnoreCase("nq")
                    || ext.equalsIgnoreCase("tql")) {
                return true;
            }
        }
        return false;
    }

    public static ParserConfig newParserConfig(final boolean lenient) {

        final ParserConfig config = new ParserConfig();

        config.set(BasicParserSettings.NORMALIZE_DATATYPE_VALUES, false);
        config.set(BasicParserSettings.NORMALIZE_LANGUAGE_TAGS, true);
        config.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);
        config.set(RDFJSONParserSettings.SUPPORT_GRAPHS_EXTENSION, true);

        config.set(BasicParserSettings.FAIL_ON_UNKNOWN_DATATYPES, false);

        for (final RioSetting<Boolean> setting : Arrays.asList(
                BasicParserSettings.VERIFY_DATATYPE_VALUES,
                BasicParserSettings.VERIFY_LANGUAGE_TAGS, //
                BasicParserSettings.VERIFY_RELATIVE_URIS, //
                BasicParserSettings.VERIFY_URI_SYNTAX,
                BasicParserSettings.FAIL_ON_UNKNOWN_LANGUAGES,
                NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES,
                RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_DATATYPES,
                RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_LANGUAGES,
                RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_TYPES,
                RDFJSONParserSettings.FAIL_ON_MULTIPLE_OBJECT_VALUES,
                RDFJSONParserSettings.FAIL_ON_UNKNOWN_PROPERTY,
                TriXParserSettings.FAIL_ON_TRIX_INVALID_STATEMENT,
                TriXParserSettings.FAIL_ON_TRIX_MISSING_DATATYPE,
                XMLParserSettings.FAIL_ON_DUPLICATE_RDF_ID,
                XMLParserSettings.FAIL_ON_INVALID_NCNAME, //
                XMLParserSettings.FAIL_ON_INVALID_QNAME, //
                XMLParserSettings.FAIL_ON_MISMATCHED_TAGS,
                XMLParserSettings.FAIL_ON_NON_STANDARD_ATTRIBUTES,
                XMLParserSettings.FAIL_ON_SAX_NON_FATAL_ERRORS)) {
            config.set(setting, !lenient); // report error and (possibly) skip data if not lenient
            config.addNonFatalError(setting); // always try to proceed further
        }

        return config;
    }

    public static ParserConfig newParserConfig(final ParserConfig source) {
        try {
            final Field field = RioConfig.class.getDeclaredField("settings");
            field.setAccessible(true);
            @SuppressWarnings({ "unchecked", "rawtypes" })
            final Map<RioSetting<Object>, Object> settings = (Map) field.get(source);
            final ParserConfig config = new ParserConfig();
            for (final Entry<RioSetting<Object>, Object> entry : settings.entrySet()) {
                config.set(entry.getKey(), entry.getValue());
            }
            config.setNonFatalErrors(source.getNonFatalErrors());
            return config;
        } catch (final Throwable ex) {
            throw new Error(ex);
        }
    }

    public static Value shortenValue(final Value value, final int threshold) {
        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            final IRI datatype = literal.getDatatype();
            final String language = literal.getLanguage().orElse(null);
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
                if (language != null) {
                    return Statements.VALUE_FACTORY.createLiteral(newLabel, language);
                } else if (datatype != null) {
                    return Statements.VALUE_FACTORY.createLiteral(newLabel, datatype);
                } else {
                    return Statements.VALUE_FACTORY.createLiteral(newLabel);
                }
            }
        }
        return value;
    }

    @Nullable
    public static String formatValue(@Nullable final Value value) {
        return Statements.formatValue(value, null);
    }

    public static String formatValue(@Nullable final Value value,
            @Nullable final Namespaces namespaces) {
        if (value == null) {
            return null;
        }
        try {
            final StringBuilder builder = new StringBuilder(value.stringValue().length() * 2);
            Statements.formatValue(value, namespaces, builder);
            return builder.toString();
        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!)", ex);
        }
    }

    public static void formatValue(final Value value, @Nullable final Namespaces namespaces,
            final Appendable out) throws IOException {
        if (value instanceof IRI) {
            Statements.formatIRI((IRI) value, out, namespaces);
        } else if (value instanceof BNode) {
            Statements.formatBNode((BNode) value, out);
        } else if (value instanceof Literal) {
            Statements.formatLiteral((Literal) value, out, namespaces);
        } else {
            throw new Error("Unexpected value class (!): " + value.getClass().getName());
        }
    }

    private static boolean isGoodQName(final String prefix, final String name) { // good to emit

        final int prefixLen = prefix.length();
        if (prefixLen > 0) {
            if (!Statements.isPN_CHARS_BASE(prefix.charAt(0))
                    || !Statements.isPN_CHARS(prefix.charAt(prefixLen - 1))) {
                return false;
            }
            for (int i = 1; i < prefixLen - 1; ++i) {
                final char c = prefix.charAt(i);
                if (!Statements.isPN_CHARS(c) && c != '.') {
                    return false;
                }
            }
        }

        final int nameLen = name.length();
        if (nameLen > 0) {
            int i = 0;
            while (i < nameLen) {
                final char c = name.charAt(i++);
                if (!Statements.isPN_CHARS_BASE(c) && c != ':'
                        && (i != 1 || c != '_' && !Statements.isNumber(c))
                        && (i == 1 || !Statements.isPN_CHARS(c) && (i == nameLen || c != '.'))) {
                    return false;
                }
            }
        }

        return true;
    }

    private static void formatIRI(final IRI iri, final Appendable out,
            @Nullable final Namespaces namespaces) throws IOException {

        if (namespaces != null) {
            final String prefix = namespaces.prefixFor(iri.getNamespace());
            if (prefix != null) {
                final String name = iri.getLocalName();
                if (Statements.isGoodQName(prefix, name)) {
                    out.append(prefix);
                    out.append(":");
                    out.append(name);
                    return;
                }
            }
        }

        final String string = iri.stringValue();
        final int len = string.length();
        out.append('<');
        for (int i = 0; i < len; ++i) {
            final char ch = string.charAt(i);
            switch (ch) {
            case 0x22: // "
                out.append("\\u0022");
                break;
            case 0x3C: // <
                out.append("\\u003C");
                break;
            case 0x3E: // >
                out.append("\\u003E");
                break;
            case 0x5C: // \
                out.append("\\u005C");
                break;
            case 0x5E: // ^
                out.append("\\u005E");
                break;
            case 0x60: // `
                out.append("\\u0060");
                break;
            case 0x7B: // {
                out.append("\\u007B");
                break;
            case 0x7C: // |
                out.append("\\u007C");
                break;
            case 0x7D: // }
                out.append("\\u007D");
                break;
            case 0x7F: // delete control char (not strictly necessary)
                out.append("\\u007F");
                break;
            default:
                if (ch <= 32) { // control char and ' '
                    out.append("\\u00").append(Character.forDigit(ch / 16, 16))
                            .append(Character.forDigit(ch % 16, 16));
                } else {
                    out.append(ch);
                }
            }
        }
        out.append('>');
    }

    private static void formatBNode(final BNode bnode, final Appendable out) throws IOException {
        final String id = bnode.getID();
        final int last = id.length() - 1;
        out.append('_').append(':');
        if (last < 0) {
            out.append("genid-hash-").append(Integer.toHexString(System.identityHashCode(bnode)));
        } else {
            char ch = id.charAt(0);
            if (Statements.isPN_CHARS_U(ch) || Statements.isNumber(ch)) {
                out.append(ch);
            } else {
                out.append("genid-start-").append(ch);
            }
            if (last > 0) {
                for (int i = 1; i < last; ++i) {
                    ch = id.charAt(i);
                    if (Statements.isPN_CHARS(ch) || ch == '.') {
                        out.append(ch);
                    } else {
                        out.append(Integer.toHexString(ch));
                    }
                }
                ch = id.charAt(last);
                if (Statements.isPN_CHARS(ch)) {
                    out.append(ch);
                } else {
                    out.append(Integer.toHexString(ch));
                }
            }
        }
    }

    private static void formatLiteral(final Literal literal, final Appendable out,
            @Nullable final Namespaces namespaces) throws IOException {
        final String label = literal.getLabel();
        final int length = label.length();
        out.append('"');
        for (int i = 0; i < length; ++i) {
            final char ch = label.charAt(i);
            switch (ch) {
            case 0x08: // \b
                out.append('\\');
                out.append('b');
                break;
            case 0x09: // \t
                out.append('\\');
                out.append('t');
                break;
            case 0x0A: // \n
                out.append('\\');
                out.append('n');
                break;
            case 0x0C: // \f
                out.append('\\');
                out.append('f');
                break;
            case 0x0D: // \r
                out.append('\\');
                out.append('r');
                break;
            case 0x22: // "
                out.append('\\');
                out.append('"');
                break;
            case 0x5C: // \
                out.append('\\');
                out.append('\\');
                break;
            case 0x7F: // delete control char
                out.append("\\u007F");
                break;
            default:
                if (ch < 32) { // other control char (not strictly necessary)
                    out.append("\\u00");
                    out.append(Character.forDigit(ch / 16, 16));
                    out.append(Character.forDigit(ch % 16, 16));
                } else {
                    out.append(ch);
                }
            }
        }
        out.append('"');
        final String language = literal.getLanguage().orElse(null);
        if (language != null) {
            out.append('@');
            final int len = language.length();
            boolean minusFound = false;
            boolean valid = true;
            for (int i = 0; i < len; ++i) {
                final char ch = language.charAt(i);
                if (ch == '-') {
                    minusFound = true;
                    if (i == 0) {
                        valid = false;
                    } else {
                        final char prev = language.charAt(i - 1);
                        valid &= Statements.isLetter(prev) || Statements.isNumber(prev);
                    }
                } else if (Statements.isNumber(ch)) {
                    valid &= minusFound;
                } else {
                    valid &= Statements.isLetter(ch);
                }
                out.append(ch);
            }
            if (!valid || language.charAt(len - 1) == '-') {
                throw new IllegalArgumentException(
                        "Invalid language tag '" + language + "' in '" + literal + "'");
            }
        } else {
            final IRI datatype = literal.getDatatype();
            if (datatype != null && !XMLSchema.STRING.equals(datatype)) {
                out.append('^');
                out.append('^');
                Statements.formatIRI(datatype, out, namespaces);
            }
        }
    }

    @Nullable
    public static Value parseValue(@Nullable final CharSequence sequence) {
        return Statements.parseValue(sequence, null);
    }

    public static Value parseValue(@Nullable final CharSequence sequence,
            @Nullable final Namespaces namespaces) {
        if (sequence == null) {
            return null;
        }
        final int c = sequence.charAt(0);
        if (c == '_') {
            return Statements.parseBNode(sequence);
        } else if (c == '"' || c == '\'') {
            return Statements.parseLiteral(sequence, namespaces);
        } else {
            return Statements.parseIRI(sequence, namespaces);
        }
    }

    private static IRI parseIRI(final CharSequence sequence,
            @Nullable final Namespaces namespaces) {

        if (sequence.charAt(0) == '<') {
            final int last = sequence.length() - 1;
            final StringBuilder builder = new StringBuilder(last - 1);
            if (sequence.charAt(last) != '>') {
                throw new IllegalArgumentException("Invalid IRI: " + sequence);
            }
            int i = 1;
            while (i < last) {
                char c = sequence.charAt(i++);
                if (c < 32) { // discard control chars but accept other chars forbidden by W3C
                    throw new IllegalArgumentException(
                            "Invalid char '" + c + "' in IRI: " + sequence);
                } else if (c != '\\') {
                    builder.append(c);
                } else {
                    if (i == last) {
                        throw new IllegalArgumentException("Invalid IRI: " + sequence);
                    }
                    c = sequence.charAt(i++);
                    if (c == 'u') {
                        builder.append(Statements.parseHex(sequence, i, 4));
                        i += 4;
                    } else if (c == 'U') {
                        builder.append(Statements.parseHex(sequence, i, 8));
                        i += 8;
                    } else {
                        builder.append(c); // accept \> and \\ plus others
                    }
                }
            }
            return Statements.VALUE_FACTORY.createIRI(builder.toString());

        } else if (namespaces != null) {
            final int len = sequence.length();
            final StringBuilder builder = new StringBuilder(len);

            int i = 0;
            while (i < len) {
                final char c = sequence.charAt(i++);
                if (c == ':') {
                    if (i > 2 && !Statements.isPN_CHARS(sequence.charAt(i - 2))) {
                        throw new IllegalArgumentException("Invalid qname " + sequence);
                    }
                    break;
                } else if (i == 1 && !Statements.isPN_CHARS_BASE(c)
                        || i > 1 && !Statements.isPN_CHARS(c) && c != '.') {
                    throw new IllegalArgumentException("Invalid qname " + sequence);
                } else {
                    builder.append(c);
                }
            }
            final String prefix = builder.toString();
            final String namespace = namespaces.uriFor(prefix);
            if (namespace == null) {
                throw new IllegalArgumentException("Unknown prefix " + prefix);
            }
            builder.setLength(0);

            while (i < len) {
                final char c = sequence.charAt(i++);
                if (c == '%') {
                    builder.append(Statements.parseHex(sequence, i, 2));
                    i += 2;
                } else if (c == '\\') {
                    final char d = sequence.charAt(i++);
                    if (d == '_' || d == '~' || d == '.' || d == '-' || d == '!' || d == '$'
                            || d == '&' || d == '\'' || d == '(' || d == ')' || d == '*'
                            || d == '+' || d == ',' || d == ';' || d == '=' || d == '/' || d == '?'
                            || d == '#' || d == '@' || d == '%') {
                        builder.append(d);
                    } else {
                        throw new IllegalArgumentException("Invalid qname " + sequence);
                    }
                } else if (Statements.isPN_CHARS_BASE(c) || c == ':'
                        || i == 1 && (c == '_' || Statements.isNumber(c))
                        || i > 1 && (Statements.isPN_CHARS(c) || i < len && c == '.')) {
                    builder.append(c);
                } else {
                    throw new IllegalArgumentException("Invalid qname " + sequence);
                }
            }
            final String name = builder.toString();

            return Statements.VALUE_FACTORY.createIRI(namespace, name);
        }

        throw new IllegalArgumentException("Unsupported qname " + sequence);
    }

    private static BNode parseBNode(final CharSequence sequence) {
        final int len = sequence.length();
        if (len > 2 && sequence.charAt(1) == ':') {
            final StringBuilder builder = new StringBuilder(len - 2);
            boolean ok = true;
            char c = sequence.charAt(2);
            builder.append(c);
            ok &= Statements.isPN_CHARS_U(c) || Statements.isNumber(c);
            for (int i = 2; i < len - 1; ++i) {
                c = sequence.charAt(i);
                builder.append(c);
                ok &= Statements.isPN_CHARS(c) || c == '.';
            }
            c = sequence.charAt(len - 1);
            builder.append(c);
            ok &= Statements.isPN_CHARS(c);
            if (ok) {
                return Statements.VALUE_FACTORY.createBNode(builder.toString());
            }
        }
        throw new IllegalArgumentException("Invalid BNode '" + sequence + "'");
    }

    private static Literal parseLiteral(final CharSequence sequence,
            @Nullable final Namespaces namespaces) {

        final StringBuilder builder = new StringBuilder(sequence.length());
        final int len = sequence.length();

        final char delim = sequence.charAt(0);
        char c = 0;
        int i = 1;
        while (i < len && (c = sequence.charAt(i++)) != delim) {
            if (c == '\\' && i < len) {
                c = sequence.charAt(i++);
                switch (c) {
                case 'b':
                    builder.append('\b');
                    break;
                case 'f':
                    builder.append('\f');
                    break;
                case 'n':
                    builder.append('\n');
                    break;
                case 'r':
                    builder.append('\r');
                    break;
                case 't':
                    builder.append('\t');
                    break;
                case 'u':
                    builder.append(Statements.parseHex(sequence, i, 4));
                    i += 4;
                    break;
                case 'U':
                    builder.append(Statements.parseHex(sequence, i, 8));
                    i += 8;
                    break;
                default:
                    builder.append(c); // handles ' " \
                    break;
                }
            } else {
                builder.append(c);
            }
        }
        final String label = builder.toString();

        if (i == len && c == delim) {
            return Statements.VALUE_FACTORY.createLiteral(label);

        } else if (i < len - 2 && sequence.charAt(i) == '^' && sequence.charAt(i + 1) == '^') {
            final IRI datatype = Statements.parseIRI(sequence.subSequence(i + 2, len), namespaces);
            return Statements.VALUE_FACTORY.createLiteral(label, datatype);

        } else if (i < len - 1 && sequence.charAt(i) == '@') {
            builder.setLength(0);
            boolean minusFound = false;
            for (int j = i + 1; j < len; ++j) {
                c = sequence.charAt(j);
                if (!Statements.isLetter(c) && (c != '-' || j == i + 1 || j == len - 1)
                        && (!Statements.isNumber(c) || !minusFound)) {
                    throw new IllegalArgumentException("Invalid lang in '" + sequence + "'");
                }
                minusFound |= c == '-';
                builder.append(c);
            }
            return Statements.VALUE_FACTORY.createLiteral(label, builder.toString());
        }

        throw new IllegalArgumentException("Invalid literal '" + sequence + "'");
    }

    private static char parseHex(final CharSequence sequence, final int index, final int count) {
        int code = 0;
        final int len = sequence.length();
        if (index + count >= len) {
            throw new IllegalArgumentException("Incomplete hex code '"
                    + sequence.subSequence(index, len) + "' in RDF value '" + sequence + "'");
        }
        for (int i = 0; i < count; ++i) {
            final char c = sequence.charAt(index + i);
            final int digit = Character.digit(c, 16);
            if (digit < 0) {
                throw new IllegalArgumentException(
                        "Invalid hex digit '" + c + "' in RDF value '" + sequence + "'");
            }
            code = code * 16 + digit;
        }
        return (char) code;
    }

    private static boolean isPN_CHARS(final int c) { // ok
        return Statements.isPN_CHARS_U(c) || Statements.isNumber(c) || c == '-' || c == 0x00B7
                || c >= 0x0300 && c <= 0x036F || c >= 0x203F && c <= 0x2040;
    }

    private static boolean isPN_CHARS_U(final int c) { // ok
        return Statements.isPN_CHARS_BASE(c) || c == '_';
    }

    private static boolean isPN_CHARS_BASE(final int c) { // ok
        return Statements.isLetter(c) || c >= 0x00C0 && c <= 0x00D6 || c >= 0x00D8 && c <= 0x00F6
                || c >= 0x00F8 && c <= 0x02FF || c >= 0x0370 && c <= 0x037D
                || c >= 0x037F && c <= 0x1FFF || c >= 0x200C && c <= 0x200D
                || c >= 0x2070 && c <= 0x218F || c >= 0x2C00 && c <= 0x2FEF
                || c >= 0x3001 && c <= 0xD7FF || c >= 0xF900 && c <= 0xFDCF
                || c >= 0xFDF0 && c <= 0xFFFD || c >= 0x10000 && c <= 0xEFFFF;
    }

    private static boolean isLetter(final int c) {
        return c >= 65 && c <= 90 || c >= 97 && c <= 122;
    }

    private static boolean isNumber(final int c) {
        return c >= 48 && c <= 57;
    }

    /**
     * General conversion facility. This method attempts to convert a supplied {@code object} to
     * an instance of the class specified. If the input is null, null is returned. If conversion
     * is unsupported or fails, an exception is thrown. The following table lists the supported
     * conversions: <blockquote>
     * <table border="1" summary="supported conversions">
     * <thead>
     * <tr>
     * <th>From classes (and sub-classes)</th>
     * <th>To classes (and super-classes)</th>
     * </tr>
     * </thead><tbody>
     * <tr>
     * <td>{@link Boolean}, {@link Literal} ({@code xsd:boolean})</td>
     * <td>{@link Boolean}, {@link Literal} ({@code xsd:boolean}), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link String}, {@link Literal} (plain, {@code xsd:string})</td>
     * <td>{@link String}, {@link Literal} (plain, {@code xsd:string}), {@code IRI} (as IRI
     * string), {@code BNode} (as BNode ID), {@link Integer}, {@link Long}, {@link Double},
     * {@link Float}, {@link Short}, {@link Byte}, {@link BigDecimal}, {@link BigInteger},
     * {@link AtomicInteger}, {@link AtomicLong}, {@link Boolean}, {@link XMLGregorianCalendar},
     * {@link GregorianCalendar}, {@link Date} (via parsing), {@link Character} (length &gt;=
     * 1)</td>
     * </tr>
     * <tr>
     * <td>{@link Number}, {@link Literal} (any numeric {@code xsd:} type)</td>
     * <td>{@link Literal} (top-level numeric {@code xsd:} type), {@link Integer}, {@link Long},
     * {@link Double}, {@link Float}, {@link Short}, {@link Byte}, {@link BigDecimal},
     * {@link BigInteger}, {@link AtomicInteger}, {@link AtomicLong}, {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Date}, {@link GregorianCalendar}, {@link XMLGregorianCalendar}, {@link Literal}
     * ({@code xsd:dateTime}, {@code xsd:date})</td>
     * <td>{@link Date}, {@link GregorianCalendar}, {@link XMLGregorianCalendar}, {@link Literal}
     * ({@code xsd:dateTime}), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link IRI}</td>
     * <td>{@link IRI}, {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link BNode}</td>
     * <td>{@link BNode}, {@link IRI} (skolemization), {@link String}</td>
     * </tr>
     * <tr>
     * <td>{@link Statement}</td>
     * <td>{@link Statement}, {@link String}</td>
     * </tr>
     * </tbody>
     * </table>
     * </blockquote>
     *
     * @param object
     *            the object to convert, possibly null
     * @param clazz
     *            the class to convert to, not null
     * @param <T>
     *            the type of result
     * @return the result of the conversion, or null if {@code object} was null
     * @throws IllegalArgumentException
     *             in case conversion fails or is unsupported for the {@code object} and class
     *             specified
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T convert(@Nullable final Object object, final Class<T> clazz)
            throws IllegalArgumentException {
        if (object == null) {
            Objects.requireNonNull(clazz);
            return null;
        }
        if (clazz.isInstance(object)) {
            return (T) object;
        }
        final T result = (T) Statements.convertObject(object, clazz);
        if (result != null) {
            return result;
        }
        throw new IllegalArgumentException("Unsupported conversion of " + object + " to " + clazz);
    }

    /**
     * General conversion facility, with fall back to default value. This method operates as
     * {@link #convert(Object, Class)}, but in case the input is null or conversion is not
     * supported returns the specified default value.
     *
     * @param object
     *            the object to convert, possibly null
     * @param clazz
     *            the class to convert to, not null
     * @param defaultValue
     *            the default value to fall back to
     * @param <T>
     *            the type of result
     * @return the result of the conversion, or the default value if {@code object} was null,
     *         conversion failed or is unsupported
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public static <T> T convert(@Nullable final Object object, final Class<T> clazz,
            @Nullable final T defaultValue) {
        if (object == null) {
            Objects.requireNonNull(clazz);
            return defaultValue;
        }
        if (clazz.isInstance(object)) {
            return (T) object;
        }
        try {
            final T result = (T) Statements.convertObject(object, clazz);
            return result != null ? result : defaultValue;
        } catch (final RuntimeException ex) {
            return defaultValue;
        }
    }

    @Nullable
    private static Object convertObject(final Object object, final Class<?> clazz) {
        if (object instanceof Literal) {
            return Statements.convertLiteral((Literal) object, clazz);
        } else if (object instanceof IRI) {
            return Statements.convertIRI((IRI) object, clazz);
        } else if (object instanceof String) {
            return Statements.convertString((String) object, clazz);
        } else if (object instanceof Number) {
            return Statements.convertNumber((Number) object, clazz);
        } else if (object instanceof Boolean) {
            return Statements.convertBoolean((Boolean) object, clazz);
        } else if (object instanceof XMLGregorianCalendar) {
            return Statements.convertCalendar((XMLGregorianCalendar) object, clazz);
        } else if (object instanceof BNode) {
            return Statements.convertBNode((BNode) object, clazz);
        } else if (object instanceof Statement) {
            return Statements.convertStatement((Statement) object, clazz);
        } else if (object instanceof GregorianCalendar) {
            final XMLGregorianCalendar calendar = Statements.DATATYPE_FACTORY
                    .newXMLGregorianCalendar((GregorianCalendar) object);
            return clazz == XMLGregorianCalendar.class ? calendar
                    : Statements.convertCalendar(calendar, clazz);
        } else if (object instanceof Date) {
            final GregorianCalendar calendar = new GregorianCalendar();
            calendar.setTime((Date) object);
            final XMLGregorianCalendar xmlCalendar = Statements.DATATYPE_FACTORY
                    .newXMLGregorianCalendar(calendar);
            return clazz == XMLGregorianCalendar.class ? xmlCalendar
                    : Statements.convertCalendar(xmlCalendar, clazz);
        } else if (object instanceof Enum<?>) {
            return Statements.convertEnum((Enum<?>) object, clazz);
        } else if (object instanceof File) {
            return Statements.convertFile((File) object, clazz);
        }
        return null;
    }

    @Nullable
    private static Object convertStatement(final Statement statement, final Class<?> clazz) {
        if (clazz.isAssignableFrom(String.class)) {
            return statement.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertLiteral(final Literal literal, final Class<?> clazz) {
        final IRI datatype = literal.getDatatype();
        if (datatype == null || datatype.equals(XMLSchema.STRING)) {
            return Statements.convertString(literal.getLabel(), clazz);
        } else if (datatype.equals(XMLSchema.BOOLEAN)) {
            return Statements.convertBoolean(literal.booleanValue(), clazz);
        } else if (datatype.equals(XMLSchema.DATE) || datatype.equals(XMLSchema.DATETIME)) {
            return Statements.convertCalendar(literal.calendarValue(), clazz);
        } else if (datatype.equals(XMLSchema.INT)) {
            return Statements.convertNumber(literal.intValue(), clazz);
        } else if (datatype.equals(XMLSchema.LONG)) {
            return Statements.convertNumber(literal.longValue(), clazz);
        } else if (datatype.equals(XMLSchema.DOUBLE)) {
            return Statements.convertNumber(literal.doubleValue(), clazz);
        } else if (datatype.equals(XMLSchema.FLOAT)) {
            return Statements.convertNumber(literal.floatValue(), clazz);
        } else if (datatype.equals(XMLSchema.SHORT)) {
            return Statements.convertNumber(literal.shortValue(), clazz);
        } else if (datatype.equals(XMLSchema.BYTE)) {
            return Statements.convertNumber(literal.byteValue(), clazz);
        } else if (datatype.equals(XMLSchema.DECIMAL)) {
            return Statements.convertNumber(literal.decimalValue(), clazz);
        } else if (datatype.equals(XMLSchema.INTEGER)) {
            return Statements.convertNumber(literal.integerValue(), clazz);
        } else if (datatype.equals(XMLSchema.NON_NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.NON_POSITIVE_INTEGER)
                || datatype.equals(XMLSchema.NEGATIVE_INTEGER)
                || datatype.equals(XMLSchema.POSITIVE_INTEGER)) {
            return Statements.convertNumber(literal.integerValue(), clazz); // infrequent integer
                                                                            // cases
        } else if (datatype.equals(XMLSchema.NORMALIZEDSTRING) || datatype.equals(XMLSchema.TOKEN)
                || datatype.equals(XMLSchema.NMTOKEN) || datatype.equals(XMLSchema.LANGUAGE)
                || datatype.equals(XMLSchema.NAME) || datatype.equals(XMLSchema.NCNAME)) {
            return Statements.convertString(literal.getLabel(), clazz); // infrequent string cases
        }
        return null;
    }

    @Nullable
    private static Object convertBoolean(final Boolean bool, final Class<?> clazz) {
        if (clazz == Boolean.class || clazz == boolean.class) {
            return bool;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return Statements.VALUE_FACTORY.createLiteral(bool);
        } else if (clazz.isAssignableFrom(String.class)) {
            return bool.toString();
        }
        return null;
    }

    @Nullable
    private static Object convertString(final String string, final Class<?> clazz) {
        if (clazz.isInstance(string)) {
            return string;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return Statements.VALUE_FACTORY.createLiteral(string, XMLSchema.STRING);
        } else if (clazz.isAssignableFrom(IRI.class)) {
            return Statements.VALUE_FACTORY.createIRI(string);
        } else if (clazz.isAssignableFrom(BNode.class)) {
            return Statements.VALUE_FACTORY
                    .createBNode(string.startsWith("_:") ? string.substring(2) : string);
        } else if (clazz == Boolean.class || clazz == boolean.class) {
            return Boolean.valueOf(string);
        } else if (clazz == Integer.class || clazz == int.class) {
            return Integer.valueOf((int) Statements.toLong(string));
        } else if (clazz == Long.class || clazz == long.class) {
            return Long.valueOf(Statements.toLong(string));
        } else if (clazz == Double.class || clazz == double.class) {
            return Double.valueOf(string);
        } else if (clazz == Float.class || clazz == float.class) {
            return Float.valueOf(string);
        } else if (clazz == Short.class || clazz == short.class) {
            return Short.valueOf((short) Statements.toLong(string));
        } else if (clazz == Byte.class || clazz == byte.class) {
            return Byte.valueOf((byte) Statements.toLong(string));
        } else if (clazz == BigDecimal.class) {
            return new BigDecimal(string);
        } else if (clazz == BigInteger.class) {
            return new BigInteger(string);
        } else if (clazz == AtomicInteger.class) {
            return new AtomicInteger(Integer.parseInt(string));
        } else if (clazz == AtomicLong.class) {
            return new AtomicLong(Long.parseLong(string));
        } else if (clazz == Date.class) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return Statements.DATATYPE_FACTORY.newXMLGregorianCalendar(fixed).toGregorianCalendar()
                    .getTime();
        } else if (clazz.isAssignableFrom(GregorianCalendar.class)) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return Statements.DATATYPE_FACTORY.newXMLGregorianCalendar(fixed)
                    .toGregorianCalendar();
        } else if (clazz.isAssignableFrom(XMLGregorianCalendar.class)) {
            final String fixed = string.contains("T") ? string : string + "T00:00:00";
            return Statements.DATATYPE_FACTORY.newXMLGregorianCalendar(fixed);
        } else if (clazz == Character.class || clazz == char.class) {
            return string.isEmpty() ? null : string.charAt(0);
        } else if (clazz.isEnum()) {
            for (final Object constant : clazz.getEnumConstants()) {
                if (string.equalsIgnoreCase(((Enum<?>) constant).name())) {
                    return constant;
                }
            }
            throw new IllegalArgumentException(
                    "Illegal " + clazz.getSimpleName() + " constant: " + string);
        } else if (clazz == File.class) {
            return new File(string);
        }
        return null;
    }

    @Nullable
    private static Object convertNumber(final Number number, final Class<?> clazz) {
        if (clazz.isAssignableFrom(Literal.class)) {
            if (number instanceof Integer || number instanceof AtomicInteger) {
                return Statements.VALUE_FACTORY.createLiteral(number.intValue());
            } else if (number instanceof Long || number instanceof AtomicLong) {
                return Statements.VALUE_FACTORY.createLiteral(number.longValue());
            } else if (number instanceof Double) {
                return Statements.VALUE_FACTORY.createLiteral(number.doubleValue());
            } else if (number instanceof Float) {
                return Statements.VALUE_FACTORY.createLiteral(number.floatValue());
            } else if (number instanceof Short) {
                return Statements.VALUE_FACTORY.createLiteral(number.shortValue());
            } else if (number instanceof Byte) {
                return Statements.VALUE_FACTORY.createLiteral(number.byteValue());
            } else if (number instanceof BigDecimal) {
                return Statements.VALUE_FACTORY.createLiteral(number.toString(),
                        XMLSchema.DECIMAL);
            } else if (number instanceof BigInteger) {
                return Statements.VALUE_FACTORY.createLiteral(number.toString(),
                        XMLSchema.INTEGER);
            }
        } else if (clazz.isAssignableFrom(String.class)) {
            return number.toString();
        } else if (clazz == Integer.class || clazz == int.class) {
            return Integer.valueOf(number.intValue());
        } else if (clazz == Long.class || clazz == long.class) {
            return Long.valueOf(number.longValue());
        } else if (clazz == Double.class || clazz == double.class) {
            return Double.valueOf(number.doubleValue());
        } else if (clazz == Float.class || clazz == float.class) {
            return Float.valueOf(number.floatValue());
        } else if (clazz == Short.class || clazz == short.class) {
            return Short.valueOf(number.shortValue());
        } else if (clazz == Byte.class || clazz == byte.class) {
            return Byte.valueOf(number.byteValue());
        } else if (clazz == BigDecimal.class) {
            return Statements.toBigDecimal(number);
        } else if (clazz == BigInteger.class) {
            return Statements.toBigInteger(number);
        } else if (clazz == AtomicInteger.class) {
            return new AtomicInteger(number.intValue());
        } else if (clazz == AtomicLong.class) {
            return new AtomicLong(number.longValue());
        }
        return null;
    }

    @Nullable
    private static Object convertCalendar(final XMLGregorianCalendar calendar, //
            final Class<?> clazz) {
        if (clazz.isInstance(calendar)) {
            return calendar;
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return Statements.VALUE_FACTORY.createLiteral(calendar);
        } else if (clazz.isAssignableFrom(String.class)) {
            return calendar.toXMLFormat();
        } else if (clazz == Date.class) {
            return calendar.toGregorianCalendar().getTime();
        } else if (clazz.isAssignableFrom(GregorianCalendar.class)) {
            return calendar.toGregorianCalendar();
        }
        return null;
    }

    @Nullable
    private static Object convertIRI(final IRI iri, final Class<?> clazz) {
        if (clazz.isInstance(iri)) {
            return iri;
        } else if (clazz.isAssignableFrom(String.class)) {
            return iri.stringValue();
        } else if (clazz == File.class && iri.stringValue().startsWith("file://")) {
            return new File(iri.stringValue().substring(7));
        }
        return null;
    }

    @Nullable
    private static Object convertBNode(final BNode bnode, final Class<?> clazz) {
        if (clazz.isInstance(bnode)) {
            return bnode;
        } else if (clazz.isAssignableFrom(IRI.class)) {
            return Statements.VALUE_FACTORY.createIRI("bnode:" + bnode.getID());
        } else if (clazz.isAssignableFrom(String.class)) {
            return "_:" + bnode.getID();
        }
        return null;
    }

    @Nullable
    private static Object convertEnum(final Enum<?> constant, final Class<?> clazz) {
        if (clazz.isInstance(constant)) {
            return constant;
        } else if (clazz.isAssignableFrom(String.class)) {
            return constant.name();
        } else if (clazz.isAssignableFrom(Literal.class)) {
            return Statements.VALUE_FACTORY.createLiteral(constant.name(), XMLSchema.STRING);
        }
        return null;
    }

    @Nullable
    private static Object convertFile(final File file, final Class<?> clazz) {
        if (clazz.isInstance(file)) {
            return clazz.cast(file);
        } else if (clazz.isAssignableFrom(IRI.class)) {
            return Statements.VALUE_FACTORY.createIRI("file://" + file.getAbsolutePath());
        } else if (clazz.isAssignableFrom(String.class)) {
            return file.getAbsolutePath();
        }
        return null;
    }

    private static BigDecimal toBigDecimal(final Number number) {
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        } else if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        } else if (number instanceof Double || number instanceof Float) {
            final double value = number.doubleValue();
            return Double.isInfinite(value) || Double.isNaN(value) ? null : new BigDecimal(value);
        } else {
            return new BigDecimal(number.longValue());
        }
    }

    private static BigInteger toBigInteger(final Number number) {
        if (number instanceof BigInteger) {
            return (BigInteger) number;
        } else if (number instanceof BigDecimal) {
            return ((BigDecimal) number).toBigInteger();
        } else if (number instanceof Double || number instanceof Float) {
            return new BigDecimal(number.doubleValue()).toBigInteger();
        } else {
            return BigInteger.valueOf(number.longValue());
        }
    }

    private static long toLong(final String string) {
        long multiplier = 1;
        final char c = string.charAt(string.length() - 1);
        if (c == 'k' || c == 'K') {
            multiplier = 1024;
        } else if (c == 'm' || c == 'M') {
            multiplier = 1024 * 1024;
        } else if (c == 'g' || c == 'G') {
            multiplier = 1024 * 1024 * 1024;
        }
        return Long.parseLong(multiplier == 1 ? string : string.substring(0, string.length() - 1))
                * multiplier;
    }

    private Statements() {
    }

    private static final class ValueComparator implements Comparator<Value> {

        private final boolean shortestFirst;

        private final List<String> rankedNamespaces;

        public ValueComparator(final boolean shortestFirst,
                @Nullable final String... rankedNamespaces) {
            this.shortestFirst = shortestFirst;
            this.rankedNamespaces = Arrays.asList(rankedNamespaces);
        }

        @Override
        public int compare(final Value v1, final Value v2) {
            if (v1 instanceof IRI) {
                if (v2 instanceof IRI) {
                    final int rank1 = rankOf(((IRI) v1).getNamespace());
                    final int rank2 = rankOf(((IRI) v2).getNamespace());
                    if (rank1 >= 0 && (rank1 < rank2 || rank2 < 0)) {
                        return -1;
                    } else if (rank2 >= 0 && (rank2 < rank1 || rank1 < 0)) {
                        return 1;
                    }
                    final String string1 = v1.stringValue();
                    final String string2 = v2.stringValue();
                    return compareStrings(string1, string2);
                } else {
                    return -1;
                }
            } else if (v1 instanceof BNode) {
                if (v2 instanceof BNode) {
                    final String id1 = ((BNode) v1).getID();
                    final String id2 = ((BNode) v2).getID();
                    return compareStrings(id1, id2);
                } else if (v2 instanceof IRI) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (v1 instanceof Literal) {
                if (v2 instanceof Literal) {
                    final String label1 = ((Literal) v1).getLabel();
                    final String label2 = ((Literal) v2).getLabel();
                    return compareStrings(label1, label2);
                } else if (v2 instanceof Resource) {
                    return 1;
                } else {
                    return -1;
                }
            } else {
                if (v1 == v2) {
                    return 0;
                } else {
                    return 1;
                }
            }
        }

        private int compareStrings(final String s1, final String s2) {
            if (this.shortestFirst) {
                final int len1 = s1.length();
                final int len2 = s2.length();
                if (len1 != len2) {
                    return len1 - len2;
                }
            }
            return s1.compareTo(s2);
        }

        private int rankOf(final String ns) {
            for (int i = 0; i < this.rankedNamespaces.size(); ++i) {
                if (ns.startsWith(this.rankedNamespaces.get(i))) {
                    return i;
                }
            }
            return -1;
        }

    }

    private static final class StatementComparator implements Comparator<Statement> {

        private final String components;

        private final Comparator<? super Value> valueComparator;

        public StatementComparator(final String components,
                final Comparator<? super Value> valueComparator) {
            this.components = components.trim().toLowerCase();
            this.valueComparator = Objects.requireNonNull(valueComparator);
            for (int i = 0; i < this.components.length(); ++i) {
                final char c = this.components.charAt(i);
                if (c != 's' && c != 'p' && c != 'o' && c != 'c') {
                    throw new IllegalArgumentException("Invalid components: " + components);
                }
            }
        }

        @Override
        public int compare(final Statement s1, final Statement s2) {
            for (int i = 0; i < this.components.length(); ++i) {
                final char c = this.components.charAt(i);
                final Value v1 = getValue(s1, c);
                final Value v2 = getValue(s2, c);
                final int result = this.valueComparator.compare(v1, v2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }

        private Value getValue(final Statement statement, final char component) {
            switch (component) {
            case 's':
                return statement.getSubject();
            case 'p':
                return statement.getPredicate();
            case 'o':
                return statement.getObject();
            case 'c':
                return statement.getContext();
            default:
                throw new Error();
            }
        }

    }

    private static final class StatementMatcher implements Predicate<Statement> {

        @Nullable
        private final ValueMatcher subjMatcher;

        @Nullable
        private final ValueMatcher predMatcher;

        @Nullable
        private final ValueMatcher objMatcher;

        @Nullable
        private final ValueMatcher ctxMatcher;

        @SuppressWarnings("unchecked")
        public StatementMatcher(final String spec) {

            // Initialize the arrays used to create the ValueTransformers
            final List<?>[] expressions = new List<?>[4];
            final Boolean[] includes = new Boolean[4];
            for (int i = 0; i < 4; ++i) {
                expressions[i] = new ArrayList<String>();
            }

            // Parse the specification string
            char action = 0;
            final List<Integer> components = new ArrayList<Integer>();
            for (final String token : spec.split("\\s+")) {
                final char ch0 = token.charAt(0);
                if (ch0 == '+' || ch0 == '-') {
                    action = ch0;
                    if (token.length() == 1) {
                        throw new IllegalArgumentException(
                                "No component(s) specified in '" + spec + "'");
                    }
                    components.clear();
                    for (int i = 1; i < token.length(); ++i) {
                        final char ch1 = Character.toLowerCase(token.charAt(i));
                        final int component = ch1 == 's' ? 0
                                : ch1 == 'p' ? 1 : ch1 == 'o' ? 2 : ch1 == 'c' ? 3 : -1;
                        if (component < 0) {
                            throw new IllegalArgumentException(
                                    "Invalid component '" + ch1 + "' in '" + spec + "'");
                        }
                        components.add(component);
                    }
                } else if (action == 0) {
                    throw new IllegalArgumentException("Missing selector in '" + spec + "'");
                } else {
                    for (final int component : components) {
                        ((List<String>) expressions[component]).add(token);
                        final Boolean include = action == '+' ? Boolean.TRUE : Boolean.FALSE;
                        if (includes[component] != null
                                && !Objects.equals(includes[component], include)) {
                            throw new IllegalArgumentException(
                                    "Include (+) and exclude (-) rules both "
                                            + "specified for same component in '" + spec + "'");
                        }
                        includes[component] = include;
                    }
                }
            }

            // Create ValueTransformers
            final ValueMatcher[] matchers = new ValueMatcher[4];
            for (int i = 0; i < 4; ++i) {
                matchers[i] = expressions[i].isEmpty() ? null
                        : new ValueMatcher((List<String>) expressions[i],
                                Boolean.TRUE.equals(includes[i]));
            }
            this.subjMatcher = matchers[0];
            this.predMatcher = matchers[1];
            this.objMatcher = matchers[2];
            this.ctxMatcher = matchers[3];
        }

        @Override
        public boolean test(final Statement stmt) {
            return (this.subjMatcher == null || this.subjMatcher.match(stmt.getSubject()))
                    && (this.predMatcher == null || this.predMatcher.match(stmt.getPredicate()))
                    && (this.objMatcher == null || this.objMatcher.match(stmt.getObject()))
                    && (this.ctxMatcher == null || this.ctxMatcher.match(stmt.getContext()));
        }

        private static final class ValueMatcher {

            private final boolean include;

            // for IRIs

            private final boolean matchAnyIRI;

            private final Set<String> matchedIRINamespaces;

            private final Set<IRI> matchedIRIs;

            // for BNodes

            private final boolean matchAnyBNode;

            private final Set<BNode> matchedBNodes;

            // for Literals

            private final boolean matchAnyPlainLiteral;

            private final boolean matchAnyLangLiteral;

            private final boolean matchAnyTypedLiteral;

            private final Set<String> matchedLanguages;

            private final Set<IRI> matchedDatatypeIRIs;

            private final Set<String> matchedDatatypeNamespaces;

            private final Set<Literal> matchedLiterals;

            ValueMatcher(final Iterable<String> matchExpressions, final boolean include) {

                this.include = include;

                this.matchedIRINamespaces = new HashSet<>();
                this.matchedIRIs = new HashSet<>();
                this.matchedBNodes = new HashSet<>();
                this.matchedLanguages = new HashSet<>();
                this.matchedDatatypeIRIs = new HashSet<>();
                this.matchedDatatypeNamespaces = new HashSet<>();
                this.matchedLiterals = new HashSet<>();

                boolean matchAnyIRI = false;
                boolean matchAnyBNode = false;
                boolean matchAnyPlainLiteral = false;
                boolean matchAnyLangLiteral = false;
                boolean matchAnyTypedLiteral = false;

                for (final String expression : matchExpressions) {
                    if ("<*>".equals(expression)) {
                        matchAnyIRI = true;
                    } else if ("_:*".equals(expression)) {
                        matchAnyBNode = true;
                    } else if ("*".equals(expression)) {
                        matchAnyPlainLiteral = true;
                    } else if ("*@*".equals(expression)) {
                        matchAnyLangLiteral = true;
                    } else if ("*^^*".equals(expression)) {
                        matchAnyTypedLiteral = true;
                    } else if (expression.startsWith("*@")) {
                        this.matchedLanguages.add(expression.substring(2));
                    } else if (expression.startsWith("*^^")) {
                        if (expression.endsWith(":*")) {
                            this.matchedDatatypeNamespaces.add(Namespaces.DEFAULT
                                    .uriFor(expression.substring(3, expression.length() - 2)));
                        } else {
                            this.matchedDatatypeIRIs.add((IRI) Statements
                                    .parseValue(expression.substring(3), Namespaces.DEFAULT));
                        }
                    } else if (expression.endsWith(":*")) {
                        this.matchedIRINamespaces.add(Namespaces.DEFAULT
                                .uriFor(expression.substring(0, expression.length() - 2)));

                    } else if (expression.endsWith("*>")) {
                        this.matchedIRINamespaces
                                .add(expression.substring(1, expression.length() - 2));
                    } else {
                        final Value value = Statements.parseValue(expression, Namespaces.DEFAULT);
                        if (value instanceof IRI) {
                            this.matchedIRIs.add((IRI) value);
                        } else if (value instanceof BNode) {
                            this.matchedBNodes.add((BNode) value);
                        } else if (value instanceof Literal) {
                            this.matchedLiterals.add((Literal) value);
                        }

                    }
                }

                this.matchAnyIRI = matchAnyIRI;
                this.matchAnyBNode = matchAnyBNode;
                this.matchAnyPlainLiteral = matchAnyPlainLiteral;
                this.matchAnyLangLiteral = matchAnyLangLiteral;
                this.matchAnyTypedLiteral = matchAnyTypedLiteral;
            }

            boolean match(final Value value) {
                final boolean matched = matchHelper(value);
                return this.include == matched;
            }

            private boolean matchHelper(final Value value) {
                if (value instanceof IRI) {
                    return this.matchAnyIRI //
                            || ValueMatcher.contains(this.matchedIRIs, value)
                            || ValueMatcher.containsNs(this.matchedIRINamespaces, (IRI) value);
                } else if (value instanceof Literal) {
                    final Literal lit = (Literal) value;
                    final String lang = lit.getLanguage().orElse(null);
                    final IRI dt = lit.getDatatype();
                    return lang == null && (dt == null || XMLSchema.STRING.equals(dt))
                            && this.matchAnyPlainLiteral //
                            || lang != null //
                                    && (this.matchAnyLangLiteral
                                            || ValueMatcher.contains(this.matchedLanguages, lang)) //
                            || dt != null //
                                    && (this.matchAnyTypedLiteral
                                            || ValueMatcher.contains(this.matchedDatatypeIRIs, dt)
                                            || ValueMatcher.containsNs(
                                                    this.matchedDatatypeNamespaces, dt)) //
                            || ValueMatcher.contains(this.matchedLiterals, lit);
                } else {
                    return this.matchAnyBNode //
                            || ValueMatcher.contains(this.matchedBNodes, value);
                }
            }

            private static boolean contains(final Set<?> set, final Object value) {
                return !set.isEmpty() && set.contains(value);
            }

            private static boolean containsNs(final Set<String> set, final IRI iri) {
                if (set.isEmpty()) {
                    return false;
                }
                if (set.contains(iri.getNamespace())) {
                    return true; // exact lookup
                }
                final String iriString = iri.stringValue();
                for (final String elem : set) {
                    if (iriString.startsWith(elem)) {
                        return true; // prefix match
                    }
                }
                return false;
            }

        }

    }

}
