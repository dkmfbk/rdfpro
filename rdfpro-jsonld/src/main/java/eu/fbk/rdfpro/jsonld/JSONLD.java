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

import java.util.Collections;
import java.util.Set;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RioSetting;
import org.eclipse.rdf4j.rio.helpers.RioSettingImpl;

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
    public static final RioSetting<Set<IRI>> ROOT_TYPES = new RioSettingImpl<Set<IRI>>(
            "eu.fbk.jsonld.roottypes",
            "The rdf:type(s) of RDF resources to be emitted "
                    + "as root (top level) nodes in the produced JSONLD",
            Collections.<IRI>emptySet());

}
