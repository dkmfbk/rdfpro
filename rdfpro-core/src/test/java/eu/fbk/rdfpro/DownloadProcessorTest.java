package eu.fbk.rdfpro;

import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

public class DownloadProcessorTest {

    @Ignore
    @Test
    public void test() throws RDFHandlerException {
        final RDFProcessor p = RDFProcessor.download(false, "http://kermadec:50090/sparql",
                "construct { ?s ?p ?o } where { ?s ?p ?o } limit 1000");

        final RDFHandler h = p.getHandler();
        h.startRDF();
        h.endRDF();
    }

}
