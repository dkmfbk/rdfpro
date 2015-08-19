package eu.fbk.rdfpro.rules.util;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public interface StatementHandler extends RDFHandler {

    void handleStatement(Resource subj, URI pred, Value obj, Resource ctx)
            throws RDFHandlerException;

}
