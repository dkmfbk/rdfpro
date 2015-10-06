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

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.RDFParserBase;
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
 * http://semarglproject.org/, adapting it to the Sesame RIO API.
 * </p>
 */
public class JSONLDParser extends RDFParserBase {

    /**
     * Creates a new JSONLDParser that will use a {@link ValueFactoryImpl} to create RDF model
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
    public void parse(final InputStream in, final String baseURI) throws IOException,
            RDFParseException, RDFHandlerException {
        parse(new InputStreamReader(in, Charset.forName("UTF-8")), baseURI);
    }

    @Override
    public void parse(final Reader reader, final String baseURI) throws IOException,
            RDFParseException, RDFHandlerException {

        final QuadSink sink = new SesameSink(this.rdfHandler, this.valueFactory);
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

    private static final class SesameSink implements QuadSink {

        private final RDFHandler handler;

        private final ValueFactory factory;

        SesameSink(final RDFHandler handler, final ValueFactory factory) {
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
            emit(subj, pred, obj, false, null, null, null);
        }

        @Override
        public void addPlainLiteral(final String subj, final String pred, final String obj,
                final String lang) {
            emit(subj, pred, obj, true, lang, null, null);
        }

        @Override
        public void addTypedLiteral(final String subj, final String pred, final String obj,
                final String dt) {
            emit(subj, pred, obj, true, null, dt, null);
        }

        @Override
        public void addNonLiteral(final String subj, final String pred, final String obj,
                final String ctx) {
            emit(subj, pred, obj, false, null, null, ctx);
        }

        @Override
        public void addPlainLiteral(final String subj, final String pred, final String obj,
                final String lang, final String ctx) {
            emit(subj, pred, obj, true, lang, null, ctx);
        }

        @Override
        public void addTypedLiteral(final String subj, final String pred, final String obj,
                final String dt, final String ctx) {
            emit(subj, pred, obj, true, null, dt, ctx);
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
                    : this.factory.createURI(subj);

            final URI p = this.factory.createURI(pred);

            final Value o;
            if (!literal) {
                o = obj.startsWith("_:") ? this.factory.createBNode(obj.substring(2))
                        : this.factory.createURI(obj);
            } else if (lang != null) {
                o = this.factory.createLiteral(obj, lang);
            } else if (dt != null) {
                o = this.factory.createLiteral(obj, this.factory.createURI(dt));
            } else {
                o = this.factory.createLiteral(obj);
            }

            Statement stmt = null;
            if (ctx == null) {
                stmt = this.factory.createStatement(s, p, o);
            } else {
                final Resource c = ctx.startsWith("_:") ? this.factory.createBNode(ctx
                        .substring(2)) : this.factory.createURI(ctx);
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