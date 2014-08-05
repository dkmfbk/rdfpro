package eu.fbk.rdfpro;

import org.openrdf.rio.RDFHandler;

final class NOPProcessor extends RDFProcessor {

    public static final NOPProcessor INSTANCE = new NOPProcessor();

    @Override
    public int getExtraPasses() {
        return 0;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        return handler;
    }

}
