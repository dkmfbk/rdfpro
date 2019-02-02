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
package eu.fbk.rdfpro.tql;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.rio.ParseErrorListener;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;

/**
 * A parser that can parse RDF documents that are in the Turtle Quads (TQL) format. TQL is N-Quads
 * with the more permissive (and efficient!) Turtle encoding. TQL is used in DBpedia exports and
 * is supported in input by the Virtuoso triple store.
 */
public class TQLParser extends AbstractRDFParser {

    private static final int EOF = -1;

    private Reader reader;

    private int lineNo;

    private boolean lineInvalid;

    private StringBuilder builder;

    private Value value;

    private Map<String, String> namespaces;

    /**
     * Creates a new TQLParser that will use a {@link SimpleValueFactory} to create RDF model
     * objects.
     */
    public TQLParser() {
        this(SimpleValueFactory.getInstance());
    }

    /**
     * Creates a new TQLParser that will use the supplied ValueFactory to create RDF model
     * objects.
     *
     * @param valueFactory
     *            the ValueFactory to use
     */
    public TQLParser(final ValueFactory valueFactory) {
        super(valueFactory);
        setParseErrorListener(null);
    }

    @Override
    public RDFFormat getRDFFormat() {
        return TQL.FORMAT;
    }

    @Override
    public RDFParser setParseErrorListener(final ParseErrorListener listener) {

        // We wrap the error listener so to intercept errors notified by Sesame (e.g., for wrong
        // literals) which unfortunately do not cause the affected RDF value / quad to be omitted
        // from the output. This way we can mark the line as invalid and thus omit the affected
        // quad.
        super.setParseErrorListener(new ParseErrorListener() {

            @Override
            public void warning(final String msg, final long lineNo, final long colNo) {
                if (listener != null) {
                    listener.warning(msg, lineNo, colNo);
                }
            }

            @Override
            public void error(final String msg, final long lineNo, final long colNo) {
                TQLParser.this.lineInvalid = true;
                if (listener != null) {
                    listener.error(msg, lineNo, colNo);
                }
            }

            @Override
            public void fatalError(final String msg, final long lineNo, final long colNo) {
                TQLParser.this.lineInvalid = true;
                if (listener != null) {
                    listener.fatalError(msg, lineNo, colNo);
                }
            }

        });
        return this;
    }

    @Override
    public void parse(final InputStream stream, final String baseIRI)
            throws IOException, RDFParseException, RDFHandlerException {
        this.parse(new InputStreamReader(stream, Charset.forName("UTF-8")), baseIRI);
    }

    @Override
    public void parse(final Reader reader, final String baseIRI)
            throws IOException, RDFParseException, RDFHandlerException {

        if (reader == null) {
            throw new NullPointerException("Null reader");
        }

        if (this.rdfHandler != null) {
            this.rdfHandler.startRDF();
        }

        this.reader = reader;
        this.lineNo = 1;
        this.builder = new StringBuilder(1024);
        this.value = null;
        this.namespaces = Maps.newHashMap();

        this.setBaseURI(Strings.nullToEmpty(baseIRI));

        final Set<Namespace> ns = getParserConfig().get(BasicParserSettings.NAMESPACES);
        if (ns != null) {
            for (final Namespace n : ns) {
                this.namespaces.put(n.getPrefix(), n.getName());
            }
        }

        reportLocation(this.lineNo, 1);

        try {
            int c = read();
            c = skipWhitespace(c);
            while (c != TQLParser.EOF) {
                if (c == '#') {
                    c = skipLine(c);
                } else if (c == '@') {
                    c = parseDirective(c);
                } else if (c == '\r' || c == '\n') {
                    c = skipLine(c);
                } else {
                    c = parseQuad(c);
                }
                c = skipWhitespace(c);
            }
        } finally {
            clear();
            this.reader = null;
            this.builder = null;
            this.value = null;
        }

        if (this.rdfHandler != null) {
            this.rdfHandler.endRDF();
        }
    }

    private int skipLine(final int ch) throws IOException {
        int c = ch;
        while (c != TQLParser.EOF && c != '\r' && c != '\n') {
            c = read();
        }
        if (c == '\n') {
            c = read();
            this.lineNo++;
            reportLocation(this.lineNo, 1);
        } else if (c == '\r') {
            c = read();
            if (c == '\n') {
                c = read();
            }
            this.lineNo++;
            reportLocation(this.lineNo, 1);
        }
        this.lineInvalid = false;
        return c;
    }

    private int skipWhitespace(final int ch) throws IOException {
        int c = ch;
        while (c == ' ' || c == '\t') {
            c = read();
        }
        return c;
    }

    private int parseDirective(final int ch)
            throws IOException, RDFParseException, RDFHandlerException {
        int c = read();
        this.builder.setLength(0);
        while (c != TQLParser.EOF && TQL.isPN_CHARS(c)) {
            this.builder.append((char) c);
            c = read();
        }
        if (c == TQLParser.EOF) {
            reportEOF();
        }
        final String directive = this.builder.toString();
        c = skipWhitespace(c);
        if (directive.equalsIgnoreCase("base")) {
            c = parseIRIString(c);
            this.setBaseURI(this.value.stringValue());

        } else if (directive.equalsIgnoreCase("prefix")) {
            this.builder.setLength(0);
            while (c != TQLParser.EOF && c != ':' && TQL.isPN_CHARS(c)) {
                this.builder.append((char) c);
                c = read();
            }
            final String prefix = this.builder.toString();
            if (c != ':') {
                this.reportError(true, "Expected ':', found: '" + (char) c + "'");
            }
            c = skipWhitespace(read());
            c = parseIRIString(c);
            final String uri = this.value.stringValue();
            this.namespaces.put(prefix, uri);
            if (this.rdfHandler != null && !this.lineInvalid) {
                this.rdfHandler.handleNamespace(prefix, uri);
            }
        }
        c = skipWhitespace(c);
        if (c != '.') {
            this.reportError(true, "Expected '.', found: '" + (char) c + "'");
        }
        c = skipWhitespace(read());
        if (c != TQLParser.EOF && c != '\r' && c != '\n') {
            this.reportError(true, "Content after '.' is not allowed");
        }
        c = skipLine(c);
        return c;
    }

    private int parseQuad(final int ch)
            throws IOException, RDFParseException, RDFHandlerException {

        int c = ch;

        try {
            c = parseResource(c);
            boolean periodConsumed = (c & 0x80000000) != 0;
            final Resource subject = (Resource) this.value;
            if (periodConsumed) {
                this.reportError(true, "Found unexpected '.'");
            }

            c = skipWhitespace(c);
            c = parseIRI(c);
            periodConsumed = (c & 0x80000000) != 0;
            final IRI predicate = (IRI) this.value;
            if (periodConsumed) {
                this.reportError(true, "Found unexpected '.'");
            }

            c = skipWhitespace(c);
            c = parseValue(c);
            periodConsumed = (c & 0x80000000) != 0;
            final Value object = this.value;

            Resource context = null;
            if (!periodConsumed) {
                c = skipWhitespace(c);
                if (c != '.') {
                    c = parseResource(c);
                    periodConsumed = (c & 0x80000000) != 0;
                    context = (Resource) this.value;
                    if (!periodConsumed) {
                        c = skipWhitespace(c);
                    }
                }
            }

            if (c == TQLParser.EOF) {
                reportEOF();
            } else if (c != '.' && !periodConsumed) {
                this.reportError(true, "Expected '.', found: '" + (char) c + "'");
            }

            c = periodConsumed ? c & 0x7FFFFFFF : read();
            c = skipWhitespace(c);
            if (c != TQLParser.EOF && c != '\r' && c != '\n') {
                this.reportError(true, "Content after '.' is not allowed");
            }

            if (this.rdfHandler != null && !this.lineInvalid) {
                final Statement statement;
                if (context == null || context.equals(SESAME.NIL)) {
                    statement = this.createStatement(subject, predicate, object);
                } else {
                    statement = this.createStatement(subject, predicate, object, context);
                }
                this.rdfHandler.handleStatement(statement);
            }

        } catch (final IllegalStateException ex) {
            // Ignore, just skip line
        }

        c = skipLine(c);
        return c;
    }

    private int parseValue(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c == '_') {
            c = parseBNode(c);
        } else if (c == '"' || c == '\'') {
            c = parseLiteral(c);
        } else if (c == '<') {
            c = parseIRIString(c);
        } else if (c == ':' || TQL.isPN_CHARS(c)) {
            c = parseIRIQName(c);
        } else if (c == TQLParser.EOF) {
            reportEOF();
        } else {
            this.reportError(true,
                    "Expected '_', '\"', '\'', '<', ':', or a valid prefix, found: '" + (char) c
                            + "'");
        }
        return c;
    }

    private int parseResource(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c == '_') {
            c = parseBNode(c);
        } else if (c == '<') {
            c = parseIRIString(c);
        } else if (c == ':' || TQL.isPN_CHARS(c)) {
            c = parseIRIQName(c);
        } else if (c == TQLParser.EOF) {
            reportEOF();
        } else {
            this.reportError(true,
                    "Expected '_', '<', ':', or a valid prefix, found: '" + (char) c + "'");
        }
        return c;
    }

    private int parseIRI(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c == '<') {
            c = parseIRIString(ch);
        } else if (c == ':' || TQL.isPN_CHARS(c)) {
            c = parseIRIQName(ch);
        } else if (c == TQLParser.EOF) {
            reportEOF();
        } else {
            this.reportError(true,
                    "Expected '<', ':', or a valid prefix, found: '" + (char) c + "'");
        }
        return c;
    }

    private int parseIRIString(final int ch) throws IOException, RDFParseException {
        if (ch != '<') {
            this.reportError(true, "Expected '<', found: '" + (char) ch + "'");
        }
        this.builder.setLength(0);
        boolean relative = true;
        int c = read();
        while (c != '>') {
            switch (c) {
            case EOF:
                reportEOF();
                break;
            case ' ':
                this.reportError(false, "IRI includes an unencoded space: '" + c + "'",
                        BasicParserSettings.VERIFY_URI_SYNTAX);
                this.builder.append((char) c);
                break;
            case '\\':
                c = read();
                if (c == TQLParser.EOF) {
                    reportEOF();
                } else if (c == 'u' || c == 'U') {
                    parseUChar(c);
                } else {
                    this.reportError(false, "IRI includes string escapes: '\\" + c + "'",
                            BasicParserSettings.VERIFY_URI_SYNTAX);
                    this.builder.append((char) c); // accept \> and \\ plus others
                }
                break;
            case ':':
                relative = false;
                this.builder.append((char) c);
                break;
            default:
                if (c < 32) {
                    // discard control chars but accept other chars forbidden by W3C
                    // rec, for compatibility with previous Turtle specification
                    this.reportError(false, "Expected valid IRI char, found: '" + (char) c + "'",
                            BasicParserSettings.VERIFY_URI_SYNTAX);
                }
                this.builder.append((char) c);
                break;
            }
            c = read();
        }
        this.value = relative ? resolveURI(this.builder.toString())
                : createURI(this.builder.toString());
        c = read();
        return c;
    }

    private int parseIRIQName(final int ch) throws IOException, RDFParseException {
        int c = ch;
        this.builder.setLength(0);
        while (c != TQLParser.EOF && c != ':' && TQL.isPN_CHARS(c)) {
            this.builder.append((char) c);
            c = read();
        }
        if (c == TQLParser.EOF) {
            reportEOF();
        } else if (c != ':') {
            this.reportError(true, "Expected ':' or valid prefix, found: '" + (char) c + "'");
        }
        final String prefix = this.builder.toString();
        final String ns = this.namespaces.get(prefix);
        if (ns == null) {
            this.reportError(false, "Unknown prefix '" + prefix + "'");
        }
        this.builder.setLength(0);
        c = read();
        while (c != TQLParser.EOF && TQL.isPN_CHARS(c)) {
            this.builder.append((char) c);
            c = read();
        }
        final int last = this.builder.length() - 1;
        if (last >= 0 && this.builder.charAt(last) == '.') {
            this.builder.setLength(last); // remove trailing '.' and mark period found
            c = c | 0x80000000;
        }
        final String name = this.builder.toString();
        this.value = createURI(ns + name);
        return c;
    }

    private int parseBNode(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '_') {
            this.reportError(true, "Expected '_', found: '" + c + "'");
        }
        c = read();
        if (c == TQLParser.EOF) {
            reportEOF();
        } else if (c != ':') {
            this.reportError(true, "Expected ':', found: '" + (char) c + "'");
        }
        c = read();
        if (c == TQLParser.EOF) {
            reportEOF();
        } else if (!TQL.isPN_CHARS_U(c) && !TQL.isNumber(c)) {
            this.reportError(false, "Invalid bnode character: " + (char) c);
        }
        this.builder.setLength(0);
        this.builder.append((char) c);
        c = read();
        while (c != TQLParser.EOF && TQL.isPN_CHARS(c)) {
            this.builder.append((char) c);
            c = read();
        }
        final int last = this.builder.length() - 1;
        if (this.builder.charAt(last) == '.') {
            this.builder.setLength(last); // remove trailing '.' and mark period found
            c = c | 0x80000000;
        }
        this.value = this.createNode(this.builder.toString());
        return c;
    }

    private int parseLiteral(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '"' && c != '\'') {
            this.reportError(true, "Expected '\"' or '\'', found: '" + c + "'");
        }
        final int delim = c;
        this.builder.setLength(0);
        c = read();
        while (c != delim) {
            if (c == TQLParser.EOF) {
                reportEOF();
            } else if (c == '\\') {
                c = read();
                switch (c) {
                case EOF:
                    reportEOF();
                    break;
                case 'b':
                    this.builder.append('\b');
                    break;
                case 'f':
                    this.builder.append('\f');
                    break;
                case 'n':
                    this.builder.append('\n');
                    break;
                case 'r':
                    this.builder.append('\r');
                    break;
                case 't':
                    this.builder.append('\t');
                    break;
                case 'u':
                case 'U':
                    parseUChar(c);
                    break;
                default:
                    this.builder.append((char) c); // handles ' " \
                    break;
                }
            } else {
                this.builder.append((char) c);
            }
            c = read();
        }
        c = read();
        final String label = this.builder.toString();
        if (c == '@') {
            this.builder.setLength(0);
            c = read();
            boolean minusFound = false;
            while (true) {
                if (c == '-' && this.builder.length() > 0) {
                    minusFound = true;
                } else if (!TQL.isLetter(c) && !(TQL.isNumber(c) && minusFound)) {
                    break;
                }
                this.builder.append((char) c);
                c = read();
            }
            if (this.builder.charAt(this.builder.length() - 1) == '-') {
                this.reportError(false, "Invalid lang tag: " + this.builder.toString(),
                        BasicParserSettings.VERIFY_LANGUAGE_TAGS);
            }
            final String language = this.builder.toString();
            this.value = this.createLiteral(label, language, null, this.lineNo, -1);
        } else if (c == '^') {
            c = read();
            if (c == TQLParser.EOF) {
                reportEOF();
            } else if (c != '^') {
                this.reportError(true, "Expected '^', found: '" + (char) c + "'");
            }
            c = read();
            if (c == TQLParser.EOF) {
                reportEOF();
            }
            c = parseIRI(c);
            final IRI datatype = (IRI) this.value;
            this.value = this.createLiteral(label, null, datatype, this.lineNo, -1);
        } else {
            this.value = this.createLiteral(label, null, null, this.lineNo, -1);
        }
        return c;
    }

    private void parseUChar(final int ch) throws IOException, RDFParseException {
        int c = ch;
        int count = 0;
        if (c == 'u') {
            count = 4;
        } else if (c == 'U') {
            count = 8;
        } else {
            this.reportError(false, "Expected 'u' or 'U', found: '" + (char) c + "'");
        }
        int code = 0;
        for (int i = 0; i < count; ++i) {
            c = read();
            if (c == TQLParser.EOF) {
                reportEOF();
            } else {
                final int digit = Character.digit(c, 16);
                if (digit < 0) {
                    this.reportError(false, "Expected hex digit, found: '" + (char) c + "'");
                }
                code = code * 16 + digit;
            }
        }
        this.builder.append((char) code);
    }

    private int read() throws IOException {
        return this.reader.read();
    }

    private void reportEOF() throws RDFParseException {
        this.reportFatalError("Unexpected end of file", this.lineNo, -1);
    }

    private void reportError(final boolean skipParsingLine, final String message)
            throws RDFParseException {
        if (getParseErrorListener() != null) {
            getParseErrorListener().error(message, this.lineNo, -1);
        }
        if (!getParserConfig()
                .isNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES)) {
            throw new RDFParseException(message, this.lineNo, -1);
        }
        if (skipParsingLine) {
            throw new IllegalStateException();
        }
    }

    private void reportError(final boolean skipParsingLine, final String message,
            final RioSetting<Boolean> setting) throws RDFParseException {
        this.reportError(message, this.lineNo, -1, setting);
        if (skipParsingLine) {
            throw new IllegalStateException();
        }
    }

}