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
package eu.fbk.rdfpro.jsonld;

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
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;
import org.semarglproject.jsonld.JsonLdParser;
import org.semarglproject.rdf.ParseException;
import org.semarglproject.sink.CharSink;
import org.semarglproject.sink.QuadSink;

/**
 * A parser that can parse RDF documents that are in the JSON-LD format.
 * <p>
 * JSON-LD is a JSON-based format for serializing data in (a superset of) RDF as JSON and
 * interpreting JSON contents as RDF. See http://www.w3.org/TR/json-ld/ for the format
 * specification.
 * </p>
 * <p>
 * This implementation wraps the parser provided by the SEMARGL project -
 * http://semarglproject.org/, adapting it to the RDF4J RIO API.
 * </p>
 */
public class JSONLDParser extends AbstractRDFParser {

    /**
     * Creates a new JSONLDParser that will use a {@link SimpleValueFactory} to create RDF model
     * objects.
     */
    public JSONLDParser() {
        super();
    }

    /**
     * Creates a new JSONLDParser that will use the supplied ValueFactory to create RDF model
     * objects.
     *
     * @param valueFactory
     *            the ValueFactory to use
     */
    public JSONLDParser(final ValueFactory valueFactory) {
        super(valueFactory);
    }

    @Override
    public RDFFormat getRDFFormat() {
        return RDFFormat.JSONLD;
    }

    @Override
    public void parse(final InputStream in, final String baseIRI)
            throws IOException, RDFParseException, RDFHandlerException {
        this.parse(new InputStreamReader(in, Charset.forName("UTF-8")), baseIRI);
    }

    @Override
    public void parse(final Reader reader, final String baseIRI)
            throws IOException, RDFParseException, RDFHandlerException {

        final QuadSink sink = new RDF4JSink(this.rdfHandler, this.valueFactory);
        try {
            final CharSink parser = JsonLdParser.connect(sink);
            parser.startStream();
            final char[] buffer = new char[4096];
            while (true) {
                final int length = reader.read(buffer);
                if (length < 0) {
                    break;
                }
                parser.process(buffer, 0, length);
            }
            parser.endStream();
        } catch (final ParseException ex) {
            throw new RDFParseException(ex.getMessage(), ex);
        }
    }

    private static final class RDF4JSink implements QuadSink {

        private final RDFHandler handler;

        private final ValueFactory factory;

        RDF4JSink(final RDFHandler handler, final ValueFactory factory) {
            this.handler = handler;
            this.factory = factory;
        }

        @Override
        public void setBaseUri(final String baseUri) {
        }

        @Override
        public boolean setProperty(final String key, final Object value) {
            return false;
        }

        @Override
        public void startStream() throws ParseException {
            try {
                this.handler.startRDF();
            } catch (final RDFHandlerException e) {
                throw new ParseException(e);
            }
        }

        @Override
        public void addNonLiteral(final String subj, final String pred, final String obj) {
            this.emit(subj, pred, obj, false, null, null, null);
        }

        @Override
        public void addPlainLiteral(final String subj, final String pred, final String obj,
                final String lang) {
            this.emit(subj, pred, obj, true, lang, null, null);
        }

        @Override
        public void addTypedLiteral(final String subj, final String pred, final String obj,
                final String dt) {
            this.emit(subj, pred, obj, true, null, dt, null);
        }

        @Override
        public void addNonLiteral(final String subj, final String pred, final String obj,
                final String ctx) {
            this.emit(subj, pred, obj, false, null, null, ctx);
        }

        @Override
        public void addPlainLiteral(final String subj, final String pred, final String obj,
                final String lang, final String ctx) {
            this.emit(subj, pred, obj, true, lang, null, ctx);
        }

        @Override
        public void addTypedLiteral(final String subj, final String pred, final String obj,
                final String dt, final String ctx) {
            this.emit(subj, pred, obj, true, null, dt, ctx);
        }

        @Override
        public void endStream() throws ParseException {
            try {
                this.handler.endRDF();
            } catch (final RDFHandlerException e) {
                throw new ParseException(e);
            }
        }

        private void emit(final String subj, final String pred, final String obj,
                final boolean literal, final String lang, final String dt, final String ctx) {

            final Resource s = subj.startsWith("_:") ? this.factory.createBNode(subj.substring(2))
                    : this.factory.createIRI(subj);

            final IRI p = this.factory.createIRI(pred);

            final Value o;
            if (!literal) {
                o = obj.startsWith("_:") ? this.factory.createBNode(obj.substring(2))
                        : this.factory.createIRI(obj);
            } else if (lang != null) {
                o = this.factory.createLiteral(obj, lang);
            } else if (dt != null) {
                o = this.factory.createLiteral(obj, this.factory.createIRI(dt));
            } else {
                o = this.factory.createLiteral(obj);
            }

            Statement stmt = null;
            if (ctx == null) {
                stmt = this.factory.createStatement(s, p, o);
            } else {
                final Resource c = ctx.startsWith("_:")
                        ? this.factory.createBNode(ctx.substring(2)) : this.factory.createIRI(ctx);
                stmt = this.factory.createStatement(s, p, o, c);
            }

            try {
                this.handler.handleStatement(stmt);
            } catch (final RDFHandlerException ex) {
                throw new RuntimeException(ex);
            }
        }

    }

}