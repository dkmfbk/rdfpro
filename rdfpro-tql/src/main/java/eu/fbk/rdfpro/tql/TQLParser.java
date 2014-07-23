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
package eu.fbk.rdfpro.tql;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.helpers.RDFParserBase;

/**
 * A parser that can parse RDF documents that are in the Turtle Quads (TQL) format. TQL is N-Quads
 * with the more permissive (and efficient!) Turtle encoding. TQL is used in DBpedia exports and
 * is supported in input by the Virtuoso triple store.
 */
public class TQLParser extends RDFParserBase {

    private static final int EOF = -1;

    private Reader reader;

    private int lineNo;

    private StringBuilder builder;

    private Value value;

    /**
     * Creates a new TQLParser that will use a {@link ValueFactoryImpl} to create RDF model
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
    public void parse(final InputStream stream, final String baseURI) throws IOException,
            RDFParseException, RDFHandlerException {
        parse(new InputStreamReader(stream, Charset.forName("UTF-8")), baseURI);
    }

    @Override
    public void parse(final Reader reader, final String baseURI) throws IOException,
            RDFParseException, RDFHandlerException {

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

    private int parseQuad(final int ch) throws IOException, RDFParseException, RDFHandlerException {

        int c = ch;
        try {
            c = parseResource(c);
            c = skipWhitespace(c);
            final Resource subject = (Resource) this.value;

            c = parseURI(c);
            c = skipWhitespace(c);
            final URI predicate = (URI) this.value;

            c = parseValue(c);
            c = skipWhitespace(c);
            final Value object = this.value;

            Resource context = null;
            if (c != '.') {
                c = parseResource(c);
                c = skipWhitespace(c);
                context = (Resource) this.value;
            }

            if (c == EOF) {
                throwEOFException();
            } else if (c != '.') {
                reportError("Expected '.', found: " + (char) c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            c = read();
            c = skipWhitespace(c);
            if (c != EOF && c != '\r' && c != '\n') {
                reportFatalError("Content after '.' is not allowed");
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
            if (getParserConfig().isNonFatalError(
                    NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES)) {
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
            c = parseURI(c);
        } else if (c == '_') {
            c = parseBNode(c);
        } else if (c == '"') {
            c = parseLiteral(c);
        } else if (c == EOF) {
            throwEOFException();
        } else {
            reportFatalError("Expected '<', '_' or '\"', found: " + (char) c + "");
        }
        return c;
    }

    private int parseResource(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c == '<') {
            c = parseURI(c);
        } else if (c == '_') {
            c = parseBNode(c);
        } else if (c == EOF) {
            throwEOFException();
        } else {
            reportFatalError("Expected '<' or '_', found: " + (char) c);
        }
        return c;
    }

    private int parseURI(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '<') {
            reportError("Supplied char should be a '<', is: " + c,
                    NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
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
                }
                this.builder.append((char) c);
                break;
            default:
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
            reportError("Supplied char should be a '_', is: " + c,
                    NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
        }
        c = read();
        if (c == EOF) {
            throwEOFException();
        } else if (c != ':') {
            reportError("Expected ':', found: " + (char) c,
                    NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
        }
        c = read();
        if (c == EOF) {
            throwEOFException();
        } else if (!TQL.isLetter(c)) {
            reportError("Expected a letter, found: " + (char) c,
                    NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
        }
        this.builder.setLength(0);
        this.builder.append((char) c);
        c = read();
        while (c != EOF && TQL.isLetterOrNumber(c)) {
            this.builder.append((char) c);
            c = read();
        }
        this.value = createBNode(this.builder.toString());
        return c;
    }

    private int parseLiteral(final int ch) throws IOException, RDFParseException {
        int c = ch;
        if (c != '"') {
            reportError("Supplied char should be a '\"', is: " + c,
                    NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
        }
        this.builder.setLength(0);
        c = read();
        while (c != '"') {
            if (c == EOF) {
                throwEOFException();
            } else if (c == '\\') {
                c = read();
                switch (c) {
                case EOF:
                    throwEOFException();
                    break;
                case 't':
                    this.builder.append('\t');
                    break;
                case 'n':
                    this.builder.append('\n');
                    break;
                case 'r':
                    this.builder.append('\r');
                    break;
                default:
                    this.builder.append((char) c);
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
            while (TQL.isLangChar(c)) {
                this.builder.append((char) c);
                c = read();
            }
            final String language = this.builder.toString();
            this.value = createLiteral(label, language, null, this.lineNo, -1);
        } else if (c == '^') {
            c = read();
            if (c == EOF) {
                throwEOFException();
            } else if (c != '^') {
                reportError("Expected '^', found: " + (char) c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            c = read();
            if (c == EOF) {
                throwEOFException();
            } else if (c != '<') {
                reportError("Expected '<', found: " + (char) c,
                        NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
            }
            c = parseURI(c);
            final URI datatype = (URI) this.value;
            this.value = createLiteral(label, null, datatype, this.lineNo, -1);
        } else {
            this.value = createLiteral(label, null, null, this.lineNo, -1);
        }
        return c;
    }

    private int read() throws IOException {
        return this.reader.read();
    }

    private static void throwEOFException() throws RDFParseException {
        throw new RDFParseException("Unexpected end of file");
    }

}