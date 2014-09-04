package eu.fbk.rdfpro;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

public class GroovyFilterProcessorTest {

    @Test
    public void test() throws Throwable {

        final String script = "" //
                + "def init(args) { println args[0]; println args[1] }; " //
                + "def start(x) { println 'start ' + x; i = 0 }; " //
                + "def end(x) { println 'end ' + x + ' ' + i }; " //
                + "if (q.p == iri('ex:p1')) i ++; ";

        final RDFProcessor processor = RDFProcessor.parse("@groovy \"" + script + "\" arg1 arg2");

        final AtomicInteger n = new AtomicInteger(0);
        final RDFHandler handler = processor.getHandler(new RDFHandlerBase() {

            @Override
            public void handleStatement(final Statement st) throws RDFHandlerException {
                n.incrementAndGet();
            }
        });
        final int c = 10000000;
        final long ts = System.currentTimeMillis();
        handler.startRDF();
        for (int i = 0; i < c; ++i) {
            handler.handleStatement(newStatement("ex:s", "ex:p", "ex:o" + i, "ex:c"));
        }
        handler.endRDF();
        System.out.println(1000L * c / (System.currentTimeMillis() - ts));
        System.out.println(n);
    }

    private Statement newStatement(final String s, final String p, final String o, final String c) {
        return Util.FACTORY.createStatement(Util.FACTORY.createURI(s), Util.FACTORY.createURI(p),
                Util.FACTORY.createURI(o), Util.FACTORY.createURI(c));
    }

}
