package eu.fbk.rdfpro;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

public class GroovyFilterProcessorTest {

    @Test
    public void test() throws Throwable {

        final String script = "" //
                + "def init(args) { println args[0]; println args[1]; _a = iri('ex:a'); _b = iri('ex:b'); _p = iri('ex:p') }; " //
                + "def start(x) { println 'start ' + x; i = 0 }; " //
                + "def end(x) { println 'end ' + x + ' ' + i }; " //
                + "emitIf(p == iri('ex:a') || p == iri('ex:b') || p == iri('ex:p'))";

        final RDFProcessor processor = RDFProcessor.parse("@groovy \"" + script + "\" arg1 arg2");

        final AtomicInteger n = new AtomicInteger(0);
        final RDFHandler sink = new RDFHandlerBase() {

            @Override
            public void handleStatement(final Statement st) throws RDFHandlerException {
                n.incrementAndGet();
            }
        };
        final RDFHandler handler = processor.getHandler(sink);

        // final RDFHandler handler = new Baseline(sink);

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

    private static class Baseline implements RDFHandler {

        private final RDFHandler handler;

        public Baseline(final RDFHandler handler) {
            this.handler = handler;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            this.handler.startRDF();
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            this.handler.handleComment(comment);
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            this.handler.handleNamespace(prefix, uri);
        }

        private final URI a = new URIImpl("ex:a");

        private final URI b = new URIImpl("ex:b");

        private final URI p = new URIImpl("ex:p");

        @Override
        public void handleStatement(final Statement st) throws RDFHandlerException {
            final URI p = st.getPredicate();
            if (p.equals(this.a) || p.equals(this.b) || p.equals(this.p)) {
                // this.handler.handleStatement(new StatementImpl(st.getSubject(),
                // st.getPredicate(),
                // st.getObject()));
                this.handler.handleStatement(st);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            this.handler.endRDF();
        }

    }

}
