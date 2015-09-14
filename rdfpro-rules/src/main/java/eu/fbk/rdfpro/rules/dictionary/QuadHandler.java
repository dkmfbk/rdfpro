package eu.fbk.rdfpro.rules.dictionary;

import org.openrdf.rio.RDFHandlerException;

public interface QuadHandler extends AutoCloseable {

    final QuadHandler NIL = new QuadHandler() {

        @Override
        public void handle(final int subj, final int pred, final int obj, final int ctx) {
        }

    };

    default void start() throws RDFHandlerException {
    }

    void handle(int subj, int pred, int obj, int ctx) throws RDFHandlerException;

    default void end() throws RDFHandlerException {
    }

    @Override
    default void close() {
    }

}
