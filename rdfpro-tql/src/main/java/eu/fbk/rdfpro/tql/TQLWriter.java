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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFWriterBase;

/**
 * An implementation of the RDFWriter interface that writes RDF documents in the Turtle Quads
 * (TQL) format. TQL is N-Quads with the more permissive (and efficient!) Turtle encoding. TQL is
 * used in DBpedia exports and is supported in input by the Virtuoso triple store.
 */
public class TQLWriter extends RDFWriterBase {

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
    public void handleNamespace(final String prefix, final String uri) throws RDFHandlerException {
        // nothing to do
    }

    @Override
    public void handleStatement(final Statement statement) throws RDFHandlerException {
        try {
            emitResource(statement.getSubject());
            this.writer.write(' ');
            emitURI(statement.getPredicate());
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
        if (value instanceof URI) {
            emitURI((URI) value);
        } else if (value instanceof BNode) {
            emitBNode((BNode) value);
        } else if (value instanceof Literal) {
            emitLiteral((Literal) value);
        }
    }

    private void emitResource(final Resource resource) throws IOException, RDFHandlerException {
        if (resource instanceof URI) {
            emitURI((URI) resource);
        } else if (resource instanceof BNode) {
            emitBNode((BNode) resource);
        }
    }

    private void emitURI(final URI uri) throws IOException, RDFHandlerException {
        final String string = uri.stringValue();
        final int length = string.length();
        this.writer.write('<');
        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            switch (ch) {
            case '>':
                this.writer.write('\\');
                this.writer.write('>');
                break;
            case '\\':
                this.writer.write('\\');
                this.writer.write('\\');
                break;
            default:
                this.writer.write(ch);
            }
        }
        this.writer.write('>');
    }

    private void emitBNode(final BNode bnode) throws IOException, RDFHandlerException {
        final String id = bnode.getID();
        this.writer.write('_');
        this.writer.write(':');
        final int length = id.length();
        for (int i = 0; i < length; ++i) {
            final char ch = id.charAt(i);
            if (!TQL.isLetterOrNumber(ch)) {
                throw new RDFHandlerException("Illegal BNode ID: " + id);
            }
            this.writer.write(ch);
        }
    }

    private void emitLiteral(final Literal literal) throws IOException, RDFHandlerException {
        final String label = literal.getLabel();
        final int length = label.length();
        this.writer.write('"');
        for (int i = 0; i < length; ++i) {
            final char ch = label.charAt(i);
            switch (ch) {
            case '\\':
                this.writer.write('\\');
                this.writer.write('\\');
                break;
            case '\t':
                this.writer.write('\\');
                this.writer.write('t');
                break;
            case '\n':
                this.writer.write('\\');
                this.writer.write('n');
                break;
            case '\r':
                this.writer.write('\\');
                this.writer.write('r');
                break;
            case '\"':
                this.writer.write('\\');
                this.writer.write('\"');
                break;
            default:
                this.writer.write(ch);
            }
        }
        this.writer.write('"');
        final URI datatype = literal.getDatatype();
        if (datatype != null) {
            this.writer.write('^');
            this.writer.write('^');
            emitURI(datatype);
        } else {
            final String language = literal.getLanguage();
            if (language != null) {
                this.writer.write('@');
                final int l = language.length();
                for (int i = 0; i < l; ++i) {
                    final char ch = language.charAt(i);
                    if (!TQL.isLangChar(ch)) {
                        throw new RDFHandlerException("Illegal language: '" + language + "'");
                    }
                    this.writer.write(ch);
                }
            }
        }
    }

}