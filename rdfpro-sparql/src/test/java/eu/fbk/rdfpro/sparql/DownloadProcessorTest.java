package eu.fbk.rdfpro.sparql;

import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.RDFProcessor;

public class DownloadProcessorTest {

    @Ignore
    @Test
    public void test() throws RDFHandlerException {
        final RDFProcessor p = new DownloadProcessor(false, "http://kermadec:50090/sparql",
                "construct { ?s ?p ?o } where { ?s ?p ?o } limit 1000");
        p.process(null, null);
    }

}
