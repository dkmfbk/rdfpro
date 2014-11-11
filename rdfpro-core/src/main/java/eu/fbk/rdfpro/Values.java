package eu.fbk.rdfpro;

import java.io.IOException;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

final class Values {

    @Nullable
    public static String formatValue(@Nullable final Value value) {
        if (value == null) {
            return null;
        }
        try {
            final StringBuilder builder = new StringBuilder(value.stringValue().length() * 2);
            formatValue(value, builder);
            return builder.toString();
        } catch (final Throwable ex) {
            throw new Error("Unexpected exception (!)", ex);
        }
    }

    public static void formatValue(final Value value, final Appendable out) throws IOException {
        if (value instanceof URI) {
            formatURI((URI) value, out);
        } else if (value instanceof BNode) {
            formatBNode((BNode) value, out);
        } else if (value instanceof Literal) {
            formatLiteral((Literal) value, out);
        }
        throw new Error("Unexpected value class (!): " + value.getClass().getName());
    }

    private static void formatURI(final URI uri, final Appendable out) throws IOException {
        final String string = uri.stringValue();
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
            if (isPN_CHARS_U(ch) || isNumber(ch)) {
                out.append(ch);
            } else {
                out.append("genid-start-").append(ch);
            }
            if (last > 0) {
                for (int i = 1; i < last; ++i) {
                    ch = id.charAt(i);
                    if (isPN_CHARS(ch) || ch == '.') {
                        out.append(ch);
                    } else {
                        out.append(Integer.toHexString(ch));
                    }
                }
                ch = id.charAt(last);
                if (isPN_CHARS(ch)) {
                    out.append(ch);
                } else {
                    out.append(Integer.toHexString(ch));
                }
            }
        }
    }

    private static void formatLiteral(final Literal literal, final Appendable out)
            throws IOException {
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
        final URI datatype = literal.getDatatype();
        if (datatype != null) {
            out.append('^');
            out.append('^');
            formatURI(datatype, out);
        } else {
            final String language = literal.getLanguage();
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
                            valid &= isLetter(prev) || isNumber(prev);
                        }
                    } else if (isNumber(ch)) {
                        valid &= minusFound;
                    } else {
                        valid &= isLetter(ch);
                    }
                    out.append(ch);
                }
                if (!valid || language.charAt(len - 1) == '-') {
                    throw new IllegalArgumentException("Invalid language tag '" + language
                            + "' in '" + literal + "'");
                }
            }
        }
    }

    @Nullable
    public static Value parseValue(@Nullable final CharSequence sequence) {
        if (sequence == null) {
            return null;
        }
        final int c = sequence.charAt(0);
        if (c == '<') {
            return parseURI(sequence);
        } else if (c == '_') {
            return parseBNode(sequence);
        } else if (c == '"' || c == '\'') {
            return parseLiteral(sequence);
        }
        throw new IllegalArgumentException("Invalid value '" + sequence + "'");
    }

    private static URI parseURI(final CharSequence sequence) {
        final int last = sequence.length() - 1;
        final StringBuilder builder = new StringBuilder(last - 1);
        if (sequence.charAt(last) != '>') {
            throw new IllegalArgumentException("Invalid URI: " + sequence);
        }
        int i = 1;
        while (i < last) {
            char c = sequence.charAt(i++);
            if (c < 32) { // discard control chars but accept other chars forbidden by W3C
                throw new IllegalArgumentException("Invalid char '" + c + "' in URI: " + sequence);
            } else if (c != '\\') {
                builder.append(c);
            } else {
                if (i == last) {
                    throw new IllegalArgumentException("Invalid URI: " + sequence);
                }
                c = sequence.charAt(i++);
                if (c == 'u') {
                    builder.append(parseHex(sequence, i, 4));
                    i += 4;
                } else if (c == 'U') {
                    builder.append(parseHex(sequence, i, 8));
                    i += 8;
                } else {
                    builder.append(c); // accept \> and \\ plus others
                }
            }
        }
        return Util.FACTORY.createURI(builder.toString());
    }

    private static BNode parseBNode(final CharSequence sequence) {
        final int len = sequence.length();
        if (len > 2 && sequence.charAt(1) == ':') {
            final StringBuilder builder = new StringBuilder(len - 2);
            boolean ok = true;
            char c = sequence.charAt(2);
            builder.append(c);
            ok &= isPN_CHARS_U(c) || isNumber(c);
            for (int i = 2; i < len - 1; ++i) {
                c = sequence.charAt(i);
                builder.append(c);
                ok &= isPN_CHARS(c) || c == '.';
            }
            c = sequence.charAt(len - 1);
            builder.append(c);
            ok &= isPN_CHARS(c);
            if (ok) {
                return Util.FACTORY.createBNode(builder.toString());
            }
        }
        throw new IllegalArgumentException("Invalid BNode '" + sequence + "'");
    }

    private static Literal parseLiteral(final CharSequence sequence) {

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
                    builder.append(parseHex(sequence, i, 4));
                    i += 4;
                    break;
                case 'U':
                    builder.append(parseHex(sequence, i, 8));
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
            return Util.FACTORY.createLiteral(label);

        } else if (i < len - 2 && sequence.charAt(i) == '^' && sequence.charAt(i + 1) == '^') {
            final URI datatype = parseURI(sequence.subSequence(i + 2, len));
            return Util.FACTORY.createLiteral(label, datatype);

        } else if (i < len - 1 && sequence.charAt(i) == '@') {
            builder.setLength(0);
            boolean minusFound = false;
            for (int j = i + 1; j < len; ++j) {
                c = sequence.charAt(j);
                if (!isLetter(c) && (c != '-' || j == i + 1 || j == len - 1)
                        && (!isNumber(c) || !minusFound)) {
                    throw new IllegalArgumentException("Invalid lang in '" + sequence + "'");
                }
                minusFound |= c == '-';
                builder.append(c);
            }
            return Util.FACTORY.createLiteral(label, builder.toString());
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
                throw new IllegalArgumentException("Invalid hex digit '" + c + "' in RDF value '"
                        + sequence + "'");
            }
            code = code * 16 + digit;
        }
        return (char) code;
    }

    private static boolean isPN_CHARS(final int c) { // ok
        return isPN_CHARS_U(c) || isNumber(c) || c == '-' || c == 0x00B7 || c >= 0x0300
                && c <= 0x036F || c >= 0x203F && c <= 0x2040;
    }

    private static boolean isPN_CHARS_U(final int c) { // ok
        return isPN_CHARS_BASE(c) || c == '_';
    }

    private static boolean isPN_CHARS_BASE(final int c) { // ok
        return isLetter(c) || c >= 0x00C0 && c <= 0x00D6 || c >= 0x00D8 && c <= 0x00F6
                || c >= 0x00F8 && c <= 0x02FF || c >= 0x0370 && c <= 0x037D || c >= 0x037F
                && c <= 0x1FFF || c >= 0x200C && c <= 0x200D || c >= 0x2070 && c <= 0x218F
                || c >= 0x2C00 && c <= 0x2FEF || c >= 0x3001 && c <= 0xD7FF || c >= 0xF900
                && c <= 0xFDCF || c >= 0xFDF0 && c <= 0xFFFD || c >= 0x10000 && c <= 0xEFFFF;
    }

    private static boolean isLetter(final int c) {
        return c >= 65 && c <= 90 || c >= 97 && c <= 122;
    }

    private static boolean isNumber(final int c) {
        return c >= 48 && c <= 57;
    }

    private Values() {
    }

}
