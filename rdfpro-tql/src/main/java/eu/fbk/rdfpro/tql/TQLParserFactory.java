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

import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFParserFactory;

/**
 * An {@link RDFParserFactory} for TQL parsers.
 */
public class TQLParserFactory implements RDFParserFactory {

    /**
     * Returns {@link TQL#FORMAT}.
     */
    @Override
    public RDFFormat getRDFFormat() {
        return TQL.FORMAT;
    }

    /**
     * Returns a new instance of {@link TQLParser}.
     */
    @Override
    public RDFParser getParser() {
        return new TQLParser();
    }

}
