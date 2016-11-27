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

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;
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

    private StringBuilder builder;

    private Value value;

    /**
     * Creates a new TQLParser that will use a {@link SimpleValueFactory} to create RDF model
     * objects.
     */
    public TQLParser() {
        super();
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
    }

    @Override
    public RDFFormat getRDFFormat() {
        return TQL.FORMAT;
    }

    @Override
    public void parse(final InputStream stream, final String baseIRI)
            throws IOException, RDFParseException, RDFHandlerException {
        parse(new InputStreamReader(stream, Charset.forName("UTF-8")), baseIRI);
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

        reportLocation(this.lineNo, 1);

        try {
            int c = read();
            c = skipWhitespace(c);
            while (c != EOF) {
                if (c == '#') {
                    c = skipLine(c);
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
        while (c != EOF && c != '\r' && c != '\n') {
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
        return c;
    }

    private int skipWhitespace(final int ch) throws IOException {
        int c = ch;
        while (c == ' ' || c == '\t') {
            c = read();
        }
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
                throwParseException("Found unexpected '.' " + (char) c);
            }

            c = skipWhitespace(c);
            c = parseIRI(c);
            periodConsumed = (c & 0x80000000) != 0;
            final IRI predicate = (IRI) this.value;
            if (periodConsumed) {
                throwParseException("Found unexpected '.' " + (char) c);
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

            if (c == EOF) {
                throwEOFException();
            } else if (c != '.' && !periodConsumed) {
                throwParseException("Expected '.', found: " + (char) c);
            }

            c = periodConsumed ? c & 0x7FFFFFFF : read();
            c = skipWhitespace(c);
            if (c != EOF && c != '\r' && c != '\n') {
                throwParseException("Content after '.' is not allowed");
            }

            if (this.rdfHandler != null) {
                final Statement statement;
                if (context == null || context.equals(SESAME.NIL)) {
                    statement = createStatement(subject, predicate, object);
                } else {
                    statement = createStatement(subject, predicate, object, context);
                }
                this.rdfHandler.handleStatement(statement);
            }

        } catch (final RDFParseException ex) {
            if (getParserConfig()
                    .isNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES)) {
                reportError(ex, this.lineNo, -1,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            } else {
                throw ex;
            }
        }

        c = skipLine(c);
        return c;
    }

    private int parseValue(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c == '<') {
            c = parseIRI(c);
        } else if (c == '_') {
            c = parseBNode(c);
        } else if (c == '"' || c == '\'') {
            c = parseLiteral(c);
        } else if (c == EOF) {
            throwEOFException();
        } else {
            throwParseException("Expected '<', '_' or '\"', found: " + (char) c + "");
        }
        return c;
    }

    private int parseResource(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c == '<') {
            c = parseIRI(c);
        } else if (c == '_') {
            c = parseBNode(c);
        } else if (c == EOF) {
            throwEOFException();
        } else {
            throwParseException("Expected '<' or '_', found: " + (char) c);
        }
        return c;
    }

    private int parseIRI(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '<') {
            throwParseException("Supplied char should be a '<', it is: " + c);
        }
        this.builder.setLength(0);
        c = read();
        while (c != '>') {
            switch (c) {
            case EOF:
                throwEOFException();
                break;
            case '\\':
                c = read();
                if (c == EOF) {
                    throwEOFException();
                } else if (c == 'u' || c == 'U') {
                    parseUChar(c);
                } else {
                    this.builder.append((char) c); // accept \> and \\ plus others
                }
                break;
            default:
                if (c < 32) { // discard control chars but accept other chars forbidden by W3C
                    // rec, for compatibility with previous Turtle specification
                    throwParseException("Expected valid IRI char, found: " + (char) c);
                }
                this.builder.append((char) c);
                break;
            }
            c = read();
        }
        this.value = createURI(this.builder.toString());
        c = read();
        return c;
    }

    private int parseBNode(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '_') {
            throwParseException("Expected '_', found: " + c);
        }
        c = read();
        if (c == EOF) {
            throwEOFException();
        } else if (c != ':') {
            throwParseException("Expected ':', found: " + (char) c);
        }
        c = read();
        if (c == EOF) {
            throwEOFException();
        } else if (!TQL.isPN_CHARS_U(c) && !TQL.isNumber(c)) {
            throwParseException("Invalid bnode character: " + (char) c);
        }
        this.builder.setLength(0);
        this.builder.append((char) c);
        c = read();
        while (c != EOF && TQL.isPN_CHARS(c)) {
            this.builder.append((char) c);
            c = read();
        }
        final int last = this.builder.length() - 1;
        if (this.builder.charAt(last) == '.') {
            this.builder.setLength(last); // remove trailing '.' and mark period found
            c = c | 0x80000000;
        }
        this.value = createBNode(this.builder.toString());
        return c;
    }

    private int parseLiteral(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '"' && c != '\'') {
            throwParseException("Expected '\"' or '\'', found: " + c);
        }
        final int delim = c;
        this.builder.setLength(0);
        c = read();
        while (c != delim) {
            if (c == EOF) {
                throwEOFException();
            } else if (c == '\\') {
                c = read();
                switch (c) {
                case EOF:
                    throwEOFException();
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
                throwParseException("Invalid lang tag: " + this.builder.toString());
            }
            final String language = this.builder.toString();
            this.value = createLiteral(label, language, null, this.lineNo, -1);
        } else if (c == '^') {
            c = read();
            if (c == EOF) {
                throwEOFException();
            } else if (c != '^') {
                throwParseException("Expected '^', found: " + (char) c);
            }
            c = read();
            if (c == EOF) {
                throwEOFException();
            } else if (c != '<') {
                throwParseException("Expected '<', found: " + (char) c);
            }
            c = parseIRI(c);
            final IRI datatype = (IRI) this.value;
            this.value = createLiteral(label, null, datatype, this.lineNo, -1);
        } else {
            this.value = createLiteral(label, null, null, this.lineNo, -1);
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
            throwParseException("Expected 'u' or 'U', found: " + c);
        }
        int code = 0;
        for (int i = 0; i < count; ++i) {
            c = read();
            if (c == EOF) {
                throwEOFException();
            } else {
                final int digit = Character.digit(c, 16);
                if (digit < 0) {
                    throwParseException("Expected hex digit, found: " + (char) c);
                }
                code = code * 16 + digit;
            }
        }
        this.builder.append((char) code);
    }

    private int read() throws IOException {
        return this.reader.read();
    }

    private void throwEOFException() throws RDFParseException {
        throw new RDFParseException("Unexpected end of file", this.lineNo, -1);
    }

    private void throwParseException(final String message) throws RDFParseException {
        throw new RDFParseException(message, this.lineNo, -1);
    }

}