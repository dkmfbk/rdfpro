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

import java.nio.charset.Charset;

import org.openrdf.rio.RDFFormat;

/**
 * Constants for the Turtle Quads (TQL) format.
 * <p>
 * The Turtle Quads {@link RDFFormat} is defined by constant {@link #FORMAT}. As this constant is
 * not part of the predefined set of formats in {@link RDFFormat}, it is necessary to register it.
 * This can be done either via {@link RDFFormat#register(RDFFormat)}, or by simply calling method
 * {@link #register()} on this class, which ensures that multiple calls will result in a single
 * registration.
 * </p>
 */
public final class TQL {

    /** RDFFormat constant for the Turtle Quads (TQL) format). */
    public static final RDFFormat FORMAT = new RDFFormat("Turtle Quads", "application/x-tql",
            Charset.forName("UTF-8"), "tql", false, true);

    /**
     * Registers the Turtle Quads format in the RIO registry. Calling this method multiple times
     * results in a single registration. Note that registration is also done transparently the
     * first time this class is accessed.
     * 
     * @deprecated as of Sesame 2.8, calling this method is not necessary - it is enough to avoid
     *             using the deprecated RDFFormat.forFileName() (and other static methods) for
     *             looking up an RDFFormat, and instead rely on methods in the Rio class
     */
    @Deprecated
    public static void register() {
        // Calling this method will cause the static initializer to run once
    }

    // Package protected utility methods

    static boolean isPN_CHARS(final int c) { // ok
        return isPN_CHARS_U(c) || isNumber(c) || c == '-' || c == 0x00B7 || c >= 0x0300
                && c <= 0x036F || c >= 0x203F && c <= 0x2040;
    }

    static boolean isPN_CHARS_U(final int c) { // ok
        return isPN_CHARS_BASE(c) || c == '_';
    }

    static boolean isPN_CHARS_BASE(final int c) { // ok
        return isLetter(c) || c >= 0x00C0 && c <= 0x00D6 || c >= 0x00D8 && c <= 0x00F6
                || c >= 0x00F8 && c <= 0x02FF || c >= 0x0370 && c <= 0x037D || c >= 0x037F
                && c <= 0x1FFF || c >= 0x200C && c <= 0x200D || c >= 0x2070 && c <= 0x218F
                || c >= 0x2C00 && c <= 0x2FEF || c >= 0x3001 && c <= 0xD7FF || c >= 0xF900
                && c <= 0xFDCF || c >= 0xFDF0 && c <= 0xFFFD || c >= 0x10000 && c <= 0xEFFFF;
    }

    static boolean isLetter(final int c) {
        return c >= 65 && c <= 90 || c >= 97 && c <= 122;
    }

    static boolean isNumber(final int c) {
        return c >= 48 && c <= 57;
    }

}
