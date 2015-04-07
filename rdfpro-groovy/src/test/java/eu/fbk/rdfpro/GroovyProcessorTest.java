package eu.fbk.rdfpro;

import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import eu.fbk.rdfpro.util.Statements;

public class GroovyProcessorTest {

    @Test
    public void testPerformances() throws Throwable {

        final String script = "" //
                + "def init(args) { println args[0]; println args[1]; }; " //
                + "def start(x) { println 'start ' + x; i = 0 }; " //
                + "def end(x) { println 'end ' + x + ' ' + i }; " //
                + "emitIf(p == <ex:b> || p == <ex:p> || p == foaf:name) ";

        final RDFProcessor processor = RDFProcessors.parse(true, "@transform -p \"" + script
                + "\" arg1 arg2");

        final AtomicInteger n = new AtomicInteger(0);
        final RDFHandler sink = new RDFHandlerBase() {

            @Override
            public void handleStatement(final Statement st) throws RDFHandlerException {
                n.incrementAndGet();
            }
        };
        final RDFHandler handler = processor.wrap(sink);

        // final RDFHandler handler = new Baseline(sink);

        final int c = 20000000;
        final long ts = System.currentTimeMillis();
        handler.startRDF();
        for (int i = 0; i < c; ++i) {
            handler.handleStatement(newStatement("ex:s", FOAF.NAME.stringValue(), "ex:o" + i,
                    "ex:c"));
        }
        handler.endRDF();
        System.out.println(1000L * c / (System.currentTimeMillis() - ts));
        System.out.println(n);
    }

    @Test
    public void testValueSet() throws RDFHandlerException, URISyntaxException {
        final File target = new File(GroovyProcessorTest.class.getResource("file.nt").toURI());
        final String script = "def init(args) { valMatchCount = 0; statMatchCount = 0; valueset = loadSet('"
                + target.getAbsolutePath()
                + "', 'spo'); };"
                + "def end(x) { log('# Matched statements: ' + statMatchCount + ' Matched values: ' + valMatchCount); };"
                + "if( valueset.match(q, 'spo') ) statMatchCount++;"
                + "if( valueset.match(s) ) valMatchCount++;";

        final GroovyProcessor processor = (GroovyProcessor) RDFProcessors.parse(true,
                "@transform \"" + script + "\" arg1 arg2");
        final RDFHandler handler = processor.wrap(RDFHandlers.NIL);
        final int c = 500000;
        final long ts = System.currentTimeMillis();
        handler.startRDF();
        for (int i = 0; i < c; ++i) {
            handler.handleStatement(newStatement("ex:s", "ex:p", "ex:o" + i, "ex:c"));
        }
        handler.handleStatement(newStatement("http://s11", "http://PP", "http://OO", "ex:c"));
        handler.handleStatement(newStatement("http://SS", "http://sy", "http://OO", "ex:c"));
        handler.handleStatement(newStatement("http://SS", "http://PP", "http://o33", "ex:c"));
        for (int i = c; i < c + c; ++i) {
            handler.handleStatement(newStatement("ex:s", "ex:p", "ex:o" + i, "ex:c"));
        }
        handler.endRDF();
        System.out.println("Statements per sec: " + 1000L * c / (System.currentTimeMillis() - ts));

        Assert.assertEquals(3, processor.getProperty(handler, "statMatchCount"));
        Assert.assertEquals(1, processor.getProperty(handler, "valMatchCount"));
    }

    private Statement newStatement(final String s, final String p, final String o, final String c) {
        final ValueFactory vf = Statements.VALUE_FACTORY;
        return vf.createStatement(vf.createURI(s), vf.createURI(p), vf.createURI(o),
                vf.createURI(c));
    }

    static class Baseline implements RDFHandler {

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
