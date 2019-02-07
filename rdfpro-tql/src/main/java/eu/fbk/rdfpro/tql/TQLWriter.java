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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFWriter;

/**
 * An implementation of the RDFWriter interface that writes RDF documents in the Turtle Quads
 * (TQL) format. TQL is N-Quads with the more permissive (and efficient!) Turtle encoding. TQL is
 * used in DBpedia exports and is supported in input by the Virtuoso triple store.
 */
public class TQLWriter extends AbstractRDFWriter {

    private final Writer writer;

    /**
     * Creates a new TQLWriter that will write to the supplied OutputStream. The UTF-8 character
     * encoding is used.
     *
     * @param stream
     *            the OutputStream to write to
     */
    public TQLWriter(final OutputStream stream) {
        this(new OutputStreamWriter(stream, Charset.forName("UTF-8")));
    }

    /**
     * Creates a new TurtleWriter that will write to the supplied Writer.
     *
     * @param writer
     *            the Writer to write to
     */
    public TQLWriter(final Writer writer) {
        if (writer == null) {
            throw new NullPointerException("Null writer");
        }
        this.writer = writer;
    }

    @Override
    public RDFFormat getRDFFormat() {
        return TQL.FORMAT;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        // nothing to do
    }

    @Override
    public void handleComment(final String comment) throws RDFHandlerException {
        // nothing to do
    }

    @Override
    public void handleNamespace(final String prefix, final String iri) throws RDFHandlerException {
        // nothing to do
    }

    @Override
    public void handleStatement(final Statement statement) throws RDFHandlerException {
        try {
            emitResource(statement.getSubject());
            this.writer.write(' ');
            emitIRI(statement.getPredicate());
            this.writer.write(' ');
            emitValue(statement.getObject());
            final Resource ctx = statement.getContext();
            if (ctx != null) {
                this.writer.write(' ');
                emitResource(statement.getContext());
            }
            this.writer.write(' ');
            this.writer.write('.');
            this.writer.write('\n');
        } catch (final IOException ex) {
            throw new RDFHandlerException(ex);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        try {
            this.writer.flush();
            this.writer.flush();
        } catch (final IOException ex) {
            throw new RDFHandlerException(ex);
        }
    }

    private void emitValue(final Value value) throws IOException, RDFHandlerException {
        if (value instanceof IRI) {
            emitIRI((IRI) value);
        } else if (value instanceof BNode) {
            emitBNode((BNode) value);
        } else if (value instanceof Literal) {
            emitLiteral((Literal) value);
        }
    }

    private void emitResource(final Resource resource) throws IOException, RDFHandlerException {
        if (resource instanceof IRI) {
            emitIRI((IRI) resource);
        } else if (resource instanceof BNode) {
            emitBNode((BNode) resource);
        }
    }

    private void emitIRI(final IRI iri) throws IOException, RDFHandlerException {
        final String string = iri.stringValue();
        final int length = string.length();
        this.writer.write('<');
        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            switch (ch) {
            case 0x22: // "
                this.writer.write("\\u0022");
                break;
            case 0x3C: // <
                this.writer.write("\\u003C");
                break;
            case 0x3E: // >
                this.writer.write("\\u003E");
                break;
            case 0x5C: // \
                this.writer.write("\\u005C");
                break;
            case 0x5E: // ^
                this.writer.write("\\u005E");
                break;
            case 0x60: // `
                this.writer.write("\\u0060");
                break;
            case 0x7B: // {
                this.writer.write("\\u007B");
                break;
            case 0x7C: // |
                this.writer.write("\\u007C");
                break;
            case 0x7D: // }
                this.writer.write("\\u007D");
                break;
            case 0x7F: // delete control char (not strictly necessary)
                this.writer.write("\\u007F");
                break;
            default:
                if (ch <= 32) { // control char and ' '
                    this.writer.write("\\u00");
                    this.writer.write(Character.forDigit(ch / 16, 16));
                    this.writer.write(Character.forDigit(ch % 16, 16));
                } else {
                    this.writer.write(ch);
                }
            }
        }
        this.writer.write('>');
    }

    private void emitBNode(final BNode bnode) throws IOException, RDFHandlerException {
        final String id = bnode.getID();
        final int last = id.length() - 1;
        this.writer.write('_');
        this.writer.write(':');
        if (last < 0) {
            this.writer.write("genid-hash-");
            this.writer.write(Integer.toHexString(System.identityHashCode(bnode)));
        } else {
            char ch = id.charAt(0);
            if (!TQL.isPN_CHARS_U(ch) && !TQL.isNumber(ch)) {
                this.writer.write("genid-start-");
                this.writer.write(ch);
            } else {
                this.writer.write(ch);
            }
            if (last > 0) {
                for (int i = 1; i < last; ++i) {
                    ch = id.charAt(i);
                    if (TQL.isPN_CHARS(ch) || ch == '.') {
                        this.writer.write(ch);
                    } else {
                        this.writer.write(Integer.toHexString(ch));
                    }
                }
                ch = id.charAt(last);
                if (TQL.isPN_CHARS(ch)) {
                    this.writer.write(ch);
                } else {
                    this.writer.write(Integer.toHexString(ch));
                }
            }
        }
    }

    private void emitLiteral(final Literal literal) throws IOException, RDFHandlerException {
        final String label = literal.getLabel();
        final int length = label.length();
        this.writer.write('"');
        for (int i = 0; i < length; ++i) {
            final char ch = label.charAt(i);
            switch (ch) {
            case 0x08: // \b
                this.writer.write('\\');
                this.writer.write('b');
                break;
            case 0x09: // \t
                this.writer.write('\\');
                this.writer.write('t');
                break;
            case 0x0A: // \n
                this.writer.write('\\');
                this.writer.write('n');
                break;
            case 0x0C: // \f
                this.writer.write('\\');
                this.writer.write('f');
                break;
            case 0x0D: // \r
                this.writer.write('\\');
                this.writer.write('r');
                break;
            case 0x22: // "
                this.writer.write('\\');
                this.writer.write('"');
                break;
            case 0x5C: // \
                this.writer.write('\\');
                this.writer.write('\\');
                break;
            case 0x7F: // delete control char
                this.writer.write("\\u007F");
                break;
            default:
                if (ch < 32) { // other control char (not strictly necessary)
                    this.writer.write("\\u00");
                    this.writer.write(Character.forDigit(ch / 16, 16));
                    this.writer.write(Character.forDigit(ch % 16, 16));
                } else {
                    this.writer.write(ch);
                }
            }
        }
        this.writer.write('"');
        final String language = literal.getLanguage().orElse(null);
        if (language != null) {
            this.writer.write('@');
            final int len = language.length();
            boolean minusFound = false;
            for (int i = 0; i < len; ++i) {
                final char ch = language.charAt(i);
                boolean valid = true;
                if (ch == '-') {
                    minusFound = true;
                    if (i == 0) {
                        valid = false;
                    } else {
                        final char prev = language.charAt(i - 1);
                        valid = TQL.isLetter(prev) || TQL.isNumber(prev);
                    }
                } else if (TQL.isNumber(ch)) {
                    valid = minusFound;
                } else {
                    valid = TQL.isLetter(ch);
                }
                if (!valid) {
                    throw new RDFHandlerException("Cannot serialize language tag '" + language
                            + "' in TQL: invalid char '" + ch + "' (see Turtle specs)");
                }
                this.writer.write(ch);
            }
            if (language.charAt(len - 1) == '-') {
                throw new RDFHandlerException("Cannot serialize language tag '" + language
                        + "' in TQL: invalid final char '-' (see Turtle specs)");
            }
        } else {
            final IRI datatype = literal.getDatatype();
            if (datatype != null && !XMLSchema.STRING.equals(datatype)) {
                this.writer.write('^');
                this.writer.write('^');
                emitIRI(datatype);
            }
        }
    }

}