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
package eu.fbk.rdfpro;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.rio.RDFHandlerException;

/**
 * Signals a failure in retrieving data from an {@code RDFSource}
 * <p>
 * This unchecked exception is used for differentiating errors in retrieving data from errors in
 * consuming it, which are conveyed by {@link RDFHandlerException}.
 * </p>
 */
public class RDFSourceException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance with the message and cause specified.
     *
     * @param message
     *            the message, possibly null
     * @param cause
     *            the cause, possibly null
     */
    public RDFSourceException(@Nullable final String message, @Nullable final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance with the message specified.
     *
     * @param message
     *            the message, possibly null
     */
    public RDFSourceException(@Nullable final String message) {
        super(message);
    }

    /**
     * Creates a new instance with the cause specified.
     * 
     * @param cause
     *            the cause, possibly null
     */
    public RDFSourceException(@Nullable final Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new instance.
     */
    public RDFSourceException() {
        super();
    }

}
