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
package eu.fbk.rdfpro.jsonld;

import java.util.Collections;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RioSetting;
import org.openrdf.rio.helpers.RioSettingImpl;

/**
 * Constants for the Turtle Quads (TQL) format.
 * <p>
 * Constant {@link #FORMAT} is a local alias for the JSON-LD RDFFormat defined in
 * {@link RDFFormat#JSONLD}. Constant {@link #ROOT_TYPES} is an optional setting controlling the
 * writing of JSON-LD data by {@link JSONLDWriter}.
 * </p>
 */
public class JSONLD {

    /** RDFFormat constant for the JSON-LD format (alias of {@link RDFFormat#JSONLD}). */
    public static final RDFFormat FORMAT = RDFFormat.JSONLD;

    /**
     * Optional setting specifying the {@code rdf:type}(s) of RDF resources to be emitted as top
     * level JSONLD nodes.
     */
    public static final RioSetting<Set<URI>> ROOT_TYPES = new RioSettingImpl<Set<URI>>(
            "eu.fbk.jsonld.roottypes", "The rdf:type(s) of RDF resources to be emitted "
                    + "as root (top level) nodes in the produced JSONLD",
            Collections.<URI>emptySet());

}
