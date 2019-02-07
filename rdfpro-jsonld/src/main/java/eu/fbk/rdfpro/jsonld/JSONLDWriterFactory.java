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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.RDFWriterFactory;

/**
 * An {@link RDFWriterFactory} for JSONLD writers.
 */
public class JSONLDWriterFactory implements RDFWriterFactory {

    /**
     * Returns {@link RDFFormat#JSONLD}.
     */
    @Override
    public RDFFormat getRDFFormat() {
        return RDFFormat.JSONLD;
    }

    /**
     * Returns a new instance of {@link JSONLDWriter}. {@inheritDoc}
     */
    @Override
    public RDFWriter getWriter(final OutputStream out) {
        return getWriter(new OutputStreamWriter(out, Charset.forName("UTF-8")));
    }

    /**
     * Returns a new instance of {@link JSONLDWriter}. {@inheritDoc}
     */
    @Override
    public RDFWriter getWriter(final Writer writer) {
        return new JSONLDWriter(writer);
    }

    /**
     * Returns a new instance of {@link JSONLDWriter}. Argument {@code baseURI} is ignored.
     * {@inheritDoc}
     */
    @Override
    public RDFWriter getWriter(final OutputStream out, @Nullable final String baseURI) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Returns a new instance of {@link JSONLDWriter}. Argument {@code baseURI} is ignored.
     * {@inheritDoc}
     */
    @Override
    public RDFWriter getWriter(final Writer writer, @Nullable final String baseURI) {
        // TODO Auto-generated method stub
        return null;
    }

}
