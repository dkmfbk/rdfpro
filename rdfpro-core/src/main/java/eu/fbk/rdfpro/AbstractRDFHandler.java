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

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * Base implementation of {@code RDFHandler} + {@code AutoCloseable}.
 * <p>
 * The implementation of {@code AutoCloseable} allows for this class and its subclasses to be
 * notified by RDFpro runtime when they are no more needed, so that allocated resources can be
 * released if necessary. For this reason, it may be convenient to start from this class when
 * implementing your specialized {@code RDFHandler}.
 * </p>
 */
public abstract class AbstractRDFHandler implements RDFHandler, AutoCloseable {

    /**
     * Default constructor.
     */
    protected AbstractRDFHandler() {
    }

    /**
     * Does nothing.
     */
    @Override
    public void startRDF() throws RDFHandlerException {
    }

    /**
     * Does nothing.
     */
    @Override
    public void handleComment(final String comment) throws RDFHandlerException {
    }

    /**
     * Does nothing.
     */
    @Override
    public void handleNamespace(final String prefix, final String uri) throws RDFHandlerException {
    }

    /**
     * Does nothing.
     */
    @Override
    public void handleStatement(final Statement statement) throws RDFHandlerException {
    }

    /**
     * Does nothing.
     */
    @Override
    public void endRDF() throws RDFHandlerException {
    }

    /**
     * Does nothing.
     */
    @Override
    public void close() {
    }

}