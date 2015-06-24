package eu.fbk.rdfpro;

import java.util.List;

import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Tracker;

public class ScriptTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptTest.class);

    private static final String SCRIPT = "var k = new org.openrdf.model.impl.URIImpl(\"ex:blabla\"); logger.info(pippo); function filter (s, h) { if (s.getSubject().equals(new org.openrdf.model.impl.URIImpl(\"ex:blabla\"))) { print(1); } h.handleStatement(s); }";

    // private static final String SCRIPT =
    // "def k = new org.openrdf.model.impl.URIImpl(\"ex:blabla\"); def filter (s, h) { if (s.getSubject() == new org.openrdf.model.impl.URIImpl(\"ex:blabla\")) { println(1); }; h.handleStatement(s); }";

    @Test
    public void test() throws Throwable {
        final ScriptEngineManager manager = new ScriptEngineManager();
        final List<ScriptEngineFactory> factories = manager.getEngineFactories();

        for (final ScriptEngineFactory factory : factories) {

            System.out.println("ScriptEngineFactory Info");

            final String engName = factory.getEngineName();
            final String engVersion = factory.getEngineVersion();
            final String langName = factory.getLanguageName();
            final String langVersion = factory.getLanguageVersion();

            System.out.printf("\tScript Engine: %s (%s)%n", engName, engVersion);

            final List<String> engNames = factory.getNames();
            for (final String name : engNames) {
                System.out.printf("\tEngine Alias: %s%n", name);
            }

            System.out.printf("\tLanguage: %s (%s)%n", langName, langVersion);

        }

        final ScriptEngine engine = manager.getEngineByExtension("js");
        engine.getContext().setAttribute("logger", LOGGER, ScriptContext.ENGINE_SCOPE);
        engine.eval("var pippo=\"ouch\";\n");
        engine.eval(SCRIPT);
        @SuppressWarnings("unchecked")
        final Filter<Statement> filter = ((Invocable) engine).getInterface(Filter.class);
        final RDFHandler handler = RDFProcessors.track(
                new Tracker(LOGGER, "Started", "Done %d %d", "%d %d %d")).wrap(RDFHandlers.NIL);
        handler.startRDF();
        for (int i = 0; i < 100000000; ++i) {
            final Statement statement = new StatementImpl(new URIImpl("ex:entity" + i), RDF.TYPE,
                    OWL.THING);
            // handler.handleStatement(statement);
            filter.filter(statement, handler);
        }
        handler.endRDF();
    }

    public interface Filter<T> {

        void filter(Statement T, RDFHandler handler);

    }

}
