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
package eu.fbk.rdfpro;

import java.util.Objects;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerWrapper;

import eu.fbk.rdfpro.util.IO;

/**
 * Base implementation of an {@code RDFHandler} + {@code AutoCloseable} wrapper.
 * <p>
 * This class wraps a {@code RDFHandler} and delegates all the methods to that instance. If the
 * wrapped {@code RDFHandler} is {@code AutoCloseable}, then also calls to
 * {@link AutoCloseable#close()} are delegated, with errors logged but ignored. Differently from
 * Sesame {@link RDFHandlerWrapper}, this class wraps a unique {@code RDFHandler} and thus does
 * not need array traversal (and its overhead) to notify a pool of {@code RDFHandlers}.
 * </p>
 */
public abstract class AbstractRDFHandlerWrapper extends AbstractRDFHandler {

    /** The wrapped {@code RDFHandler}. */
    protected final RDFHandler handler;

    /**
     * Creates a new instance wrapping the supplied {@code RDFHandler}.
     *
     * @param handler
     *            the {@code RDFHandler} to wrap, not null
     */
    protected AbstractRDFHandlerWrapper(final RDFHandler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    /**
     * Delegates to the wrapped {@code RDFHandler}.
     */
    @Override
    public void startRDF() throws RDFHandlerException {
        this.handler.startRDF();
    }

    /**
     * Delegates to the wrapped {@code RDFHandler}.
     */
    @Override
    public void handleComment(final String comment) throws RDFHandlerException {
        this.handler.handleComment(comment);
    }

    /**
     * Delegates to the wrapped {@code RDFHandler}.
     */
    @Override
    public void handleNamespace(final String prefix, final String uri) throws RDFHandlerException {
        this.handler.handleNamespace(prefix, uri);
    }

    /**
     * Delegates to the wrapped {@code RDFHandler}.
     */
    @Override
    public void handleStatement(final Statement statement) throws RDFHandlerException {
        this.handler.handleStatement(statement);
    }

    /**
     * Delegates to the wrapped {@code RDFHandler}.
     */
    @Override
    public void endRDF() throws RDFHandlerException {
        this.handler.endRDF();
    }

    /**
     * Delegates to the wrapped {@code RDFHandler}, if it implements {@code Closeable}.
     */
    @Override
    public void close() {
        IO.closeQuietly(this.handler);
    }

}