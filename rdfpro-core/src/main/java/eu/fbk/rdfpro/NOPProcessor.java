package eu.fbk.rdfpro;

import org.openrdf.rio.RDFHandler;

final class NOPProcessor extends RDFProcessor {

    public static final NOPProcessor INSTANCE = new NOPProcessor();

    @Override
    public RDFHandler getHandler(final RDFHandler sink) {
        return sink;
    }

}
