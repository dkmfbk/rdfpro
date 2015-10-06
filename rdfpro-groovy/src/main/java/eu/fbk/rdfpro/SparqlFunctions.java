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
package eu.fbk.rdfpro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.annotation.Nullable;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.BooleanCast;
import org.openrdf.query.algebra.evaluation.function.DateTimeCast;
import org.openrdf.query.algebra.evaluation.function.DecimalCast;
import org.openrdf.query.algebra.evaluation.function.DoubleCast;
import org.openrdf.query.algebra.evaluation.function.FloatCast;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.IntegerCast;
import org.openrdf.query.algebra.evaluation.function.datetime.Day;
import org.openrdf.query.algebra.evaluation.function.datetime.Hours;
import org.openrdf.query.algebra.evaluation.function.datetime.Minutes;
import org.openrdf.query.algebra.evaluation.function.datetime.Month;
import org.openrdf.query.algebra.evaluation.function.datetime.Now;
import org.openrdf.query.algebra.evaluation.function.datetime.Seconds;
import org.openrdf.query.algebra.evaluation.function.datetime.Timezone;
import org.openrdf.query.algebra.evaluation.function.datetime.Tz;
import org.openrdf.query.algebra.evaluation.function.datetime.Year;
import org.openrdf.query.algebra.evaluation.function.hash.MD5;
import org.openrdf.query.algebra.evaluation.function.hash.SHA1;
import org.openrdf.query.algebra.evaluation.function.hash.SHA256;
import org.openrdf.query.algebra.evaluation.function.hash.SHA384;
import org.openrdf.query.algebra.evaluation.function.hash.SHA512;
import org.openrdf.query.algebra.evaluation.function.numeric.Abs;
import org.openrdf.query.algebra.evaluation.function.numeric.Ceil;
import org.openrdf.query.algebra.evaluation.function.numeric.Floor;
import org.openrdf.query.algebra.evaluation.function.numeric.Rand;
import org.openrdf.query.algebra.evaluation.function.numeric.Round;
import org.openrdf.query.algebra.evaluation.function.rdfterm.StrDt;
import org.openrdf.query.algebra.evaluation.function.rdfterm.StrLang;
import org.openrdf.query.algebra.evaluation.function.string.Concat;
import org.openrdf.query.algebra.evaluation.function.string.Contains;
import org.openrdf.query.algebra.evaluation.function.string.EncodeForUri;
import org.openrdf.query.algebra.evaluation.function.string.LowerCase;
import org.openrdf.query.algebra.evaluation.function.string.Replace;
import org.openrdf.query.algebra.evaluation.function.string.StrAfter;
import org.openrdf.query.algebra.evaluation.function.string.StrBefore;
import org.openrdf.query.algebra.evaluation.function.string.StrEnds;
import org.openrdf.query.algebra.evaluation.function.string.StrLen;
import org.openrdf.query.algebra.evaluation.function.string.StrStarts;
import org.openrdf.query.algebra.evaluation.function.string.Substring;
import org.openrdf.query.algebra.evaluation.function.string.UpperCase;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import eu.fbk.rdfpro.GroovyProcessor.GroovyLiteral;
import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.Statements;

final class SparqlFunctions {

    private static final DatatypeFactory DATATYPE_FACTORY;

    static {
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (final DatatypeConfigurationException ex) {
            throw new Error("Could not instantiate javax.xml.datatype.DatatypeFactory", ex);
        }
    }

    public static boolean isiri(final Object arg) {
        return toRDF(arg) instanceof URI;
    }

    public static boolean isblank(final Object arg) {
        return toRDF(arg) instanceof BNode;
    }

    public static boolean isliteral(final Object arg) {
        return toRDF(arg) instanceof Literal;
    }

    public static boolean isnumeric(final Object arg) {
        final Value value = toRDF(arg);
        if (value instanceof Literal) {
            final URI datatype = ((Literal) value).getDatatype();
            return XMLDatatypeUtil.isNumericDatatype(datatype);
        }
        return false;
    }

    public static String str(final Object arg) {
        final Value value = toRDF(arg);
        if (value instanceof URI || value instanceof Literal) {
            return value.stringValue();
        }
        throw new IllegalArgumentException("str() argument is not an URI or literal");
    }

    public static String lang(final Object arg) {
        final Value value = toRDF(arg);
        if (value instanceof Literal) {
            final String lang = ((Literal) value).getLanguage();
            return lang == null ? "" : lang;
        }
        throw new IllegalArgumentException("lang() argument is not a literal");
    }

    public static URI datatype(final Object arg) {
        final Value value = toRDF(arg);
        if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            final URI datatype = literal.getDatatype();
            if (datatype != null) {
                return datatype;
            } else if (literal.getLanguage() != null) {
                return RDF.LANGSTRING;
            } else {
                return XMLSchema.STRING;
            }
        }
        throw new IllegalArgumentException("datatype() argument is not a literal");
    }

    public static URI iri(final Object arg) {
        if (arg instanceof URI) {
            return (URI) arg;
        }
        Objects.requireNonNull(arg);
        return Statements.VALUE_FACTORY.createURI(arg.toString());
    }

    public static BNode bnode() {
        return Statements.VALUE_FACTORY.createBNode();
    }

    public static BNode bnode(final Object arg) {
        if (arg instanceof BNode) {
            return (BNode) arg;
        }
        Objects.requireNonNull(arg);
        return Statements.VALUE_FACTORY.createBNode(Hash.murmur3(arg.toString()).toString());
    }

    public static Literal strdt(final Object value, final Object datatype) {
        return (Literal) evaluate(new StrDt(), value, datatype);
    }

    public static Literal strlang(@Nullable final Object value, @Nullable final Object lang) {
        return (Literal) evaluate(new StrLang(), value, lang);
    }

    public static URI uuid() {
        return Statements.VALUE_FACTORY.createURI("urn:uuid:" + UUID.randomUUID().toString());
    }

    public static String struuid() {
        return UUID.randomUUID().toString();
    }

    public static int strlen(final Object arg) {
        return ((Literal) evaluate(new StrLen(), arg)).intValue();
    }

    public static Literal substr(final Object string, final Object from) {
        return (Literal) evaluate(new Substring(), string, from);
    }

    public static Literal substr(final Object string, final Object from, final Object length) {
        return (Literal) evaluate(new Substring(), string, from);
    }

    public static Literal ucase(final Object arg) {
        return (Literal) evaluate(new UpperCase(), arg);
    }

    public static Literal lcase(final Object arg) {
        return (Literal) evaluate(new LowerCase(), arg);
    }

    public static boolean strstarts(final Object arg1, final Object arg2) {
        return ((Literal) evaluate(new StrStarts(), arg1, arg2)).booleanValue();
    }

    public static boolean strends(final Object arg1, final Object arg2) {
        return ((Literal) evaluate(new StrEnds(), arg1, arg2)).booleanValue();
    }

    public static boolean contains(final Object arg1, final Object arg2) {
        return ((Literal) evaluate(new Contains(), arg1, arg2)).booleanValue();
    }

    public static Literal strbefore(final Object arg1, final Object arg2) {
        return (Literal) evaluate(new StrBefore(), arg1, arg2);
    }

    public static Literal strafter(final Object arg1, final Object arg2) {
        return (Literal) evaluate(new StrAfter(), arg1, arg2);
    }

    public static String encode_for_uri(final Object arg) {
        return ((Literal) evaluate(new EncodeForUri(), arg)).stringValue();
    }

    public static Literal concat(final Object... args) {
        return (Literal) evaluate(new Concat(), args);
    }

    public static boolean langmatches(final Object languageTag, final Object languageRange) {

        final Value tagValue = toRDF(languageTag);
        final Value rangeValue = toRDF(languageRange);

        if (QueryEvaluationUtil.isSimpleLiteral(tagValue)
                && QueryEvaluationUtil.isSimpleLiteral(rangeValue)) {

            final String tag = ((Literal) tagValue).getLabel();
            final String range = ((Literal) rangeValue).getLabel();

            if (range.equals("*")) {
                return tag.length() > 0;
            } else if (tag.length() == range.length()) {
                return tag.equalsIgnoreCase(range);
            } else if (tag.length() > range.length()) {
                final String prefix = tag.substring(0, range.length());
                return prefix.equalsIgnoreCase(range) && tag.charAt(range.length()) == '-';
            } else {
                return false;
            }
        }

        throw new IllegalArgumentException("LANGMATCHES() cannot be applied to " + languageTag
                + ", " + languageRange);
    }

    public static boolean regex(final Object text, final Object pattern) {
        return regex(text, pattern, "");
    }

    public static boolean regex(final Object text, final Object pattern,
            @Nullable final Object flags) {

        final Value textValue = toRDF(text);
        final Value patternValue = toRDF(pattern);
        final Value flagsValue = toRDF(flags);

        if (QueryEvaluationUtil.isStringLiteral(textValue)
                && QueryEvaluationUtil.isSimpleLiteral(patternValue)
                && QueryEvaluationUtil.isSimpleLiteral(flagsValue)) {

            final String textStr = ((Literal) textValue).getLabel();
            final String patternStr = ((Literal) patternValue).getLabel();
            final String flagsStr = ((Literal) flagsValue).getLabel();

            int f = 0;
            for (final char c : flagsStr.toCharArray()) {
                switch (c) {
                case 's':
                    f |= Pattern.DOTALL;
                    break;
                case 'm':
                    f |= Pattern.MULTILINE;
                    break;
                case 'i':
                    f |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'x':
                    f |= Pattern.COMMENTS;
                    break;
                case 'd':
                    f |= Pattern.UNIX_LINES;
                    break;
                case 'u':
                    f |= Pattern.UNICODE_CASE;
                    break;
                default:
                    throw new IllegalArgumentException("REGEX() flag not valid: " + c);
                }
            }
            return Pattern.compile(patternStr, f).matcher(textStr).find();
        }

        throw new IllegalArgumentException("REGEX() arguments not valid: " + text + ", " + pattern
                + (flags == null ? "" : ", " + flags));
    }

    public static Literal replace(final Object arg, final Object pattern, final Object replacement) {
        return replace(arg, pattern, replacement, "");
    }

    public static Literal replace(final Object arg, final Object pattern,
            final Object replacement, final Object flags) {
        return (Literal) evaluate(new Replace(), arg, pattern, replacement, flags);
    }

    public static double abs(final Object arg) {
        return ((Literal) evaluate(new Abs(), arg)).doubleValue();
    }

    public static long round(final Object arg) {
        return ((Literal) evaluate(new Round(), arg)).longValue();
    }

    public static long ceil(final Object arg) {
        return ((Literal) evaluate(new Ceil(), arg)).longValue();
    }

    public static long floor(final Object arg) {
        return ((Literal) evaluate(new Floor(), arg)).longValue();
    }

    public static double rand() {
        return ((Literal) evaluate(new Rand())).doubleValue();
    }

    public static Literal now() {
        return (Literal) evaluate(new Now());
    }

    public static int year(final Object arg) {
        return ((Literal) evaluate(new Year(), arg)).intValue();
    }

    public static int month(final Object arg) {
        return ((Literal) evaluate(new Month(), arg)).intValue();
    }

    public static int day(final Object arg) {
        return ((Literal) evaluate(new Day(), arg)).intValue();
    }

    public static int hours(final Object arg) {
        return ((Literal) evaluate(new Hours(), arg)).intValue();
    }

    public static int minutes(final Object arg) {
        return ((Literal) evaluate(new Minutes(), arg)).intValue();
    }

    public static double seconds(final Object arg) {
        return ((Literal) evaluate(new Seconds(), arg)).doubleValue();
    }

    public static Literal timezone(final Object arg) {
        return (Literal) evaluate(new Timezone(), arg);
    }

    public static String tz(final Object arg) {
        return ((Literal) evaluate(new Tz(), arg)).stringValue();
    }

    public static String md5(final Object arg) {
        return ((Literal) evaluate(new MD5(), arg)).stringValue();
    }

    public static String sha1(final Object arg) {
        return ((Literal) evaluate(new SHA1(), arg)).stringValue();
    }

    public static String sha256(final Object arg) {
        return ((Literal) evaluate(new SHA256(), arg)).stringValue();
    }

    public static String sha384(final Object arg) {
        return ((Literal) evaluate(new SHA384(), arg)).stringValue();
    }

    public static String sha512(final Object arg) {
        return ((Literal) evaluate(new SHA512(), arg)).stringValue();
    }

    public static boolean bool(final Object arg) {
        return ((Literal) evaluate(new BooleanCast(), arg)).booleanValue();
    }

    public static double dbl(final Object arg) {
        return ((Literal) evaluate(new DoubleCast(), arg)).doubleValue();
    }

    public static float flt(final Object arg) {
        return ((Literal) evaluate(new FloatCast(), arg)).floatValue();
    }

    public static BigDecimal dec(final Object arg) {
        return ((Literal) evaluate(new DecimalCast(), arg)).decimalValue();
    }

    public static BigInteger integer(final Object arg) {
        return ((Literal) evaluate(new IntegerCast(), arg)).integerValue();
    }

    public static Literal dt(final Object arg) {
        return (Literal) evaluate(new DateTimeCast(), arg);
    }

    public static Value evaluate(final Function function, final Object... args) {
        final Value[] values = new Value[args.length];
        for (int i = 0; i < args.length; i++) {
            values[i] = toRDF(args[i]);
        }
        try {
            return function.evaluate(Statements.VALUE_FACTORY, values);
        } catch (final ValueExprEvaluationException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    public static Value toRDF(final Object object) {
        if (object instanceof Value) {
            return (Value) object;
        } else if (object instanceof Long) {
            return new GroovyLiteral(object.toString(), XMLSchema.LONG);
        } else if (object instanceof Integer) {
            return new GroovyLiteral(object.toString(), XMLSchema.INT);
        } else if (object instanceof Short) {
            return new GroovyLiteral(object.toString(), XMLSchema.SHORT);
        } else if (object instanceof Byte) {
            return new GroovyLiteral(object.toString(), XMLSchema.BYTE);
        } else if (object instanceof Double) {
            return new GroovyLiteral(object.toString(), XMLSchema.DOUBLE);
        } else if (object instanceof Float) {
            return new GroovyLiteral(object.toString(), XMLSchema.FLOAT);
        } else if (object instanceof Boolean) {
            return new GroovyLiteral(object.toString(), XMLSchema.BOOLEAN);
        } else if (object instanceof XMLGregorianCalendar) {
            final XMLGregorianCalendar c = (XMLGregorianCalendar) object;
            return new GroovyLiteral(c.toXMLFormat(), XMLDatatypeUtil.qnameToURI(c
                    .getXMLSchemaType()));
        } else if (object instanceof Date) {
            final GregorianCalendar c = new GregorianCalendar();
            c.setTime((Date) object);
            final XMLGregorianCalendar xc = DATATYPE_FACTORY.newXMLGregorianCalendar(c);
            return new GroovyLiteral(xc.toXMLFormat(), XMLDatatypeUtil.qnameToURI(xc
                    .getXMLSchemaType()));
        } else if (object instanceof CharSequence) {
            return new GroovyLiteral(object.toString(), XMLSchema.STRING);
        } else if (object != null) {
            return new GroovyLiteral(object.toString());
        }
        throw new NullPointerException();
    }

}
