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

/**
 * Parser and writer for RDF in JSON-LD format (depends on Semargl lib).
 * <p>
 * Parsing depends on the {@code Semargl} JSONLD parser, while writing is implemented directly by
 * this factory and can be configured via setting {@code #ROOT_TYPES}, which specifies the types
 * of RDF resources to be emitted as top level JSONLD nodes.
 * </p>
 */
package eu.fbk.rdfpro.jsonld;

