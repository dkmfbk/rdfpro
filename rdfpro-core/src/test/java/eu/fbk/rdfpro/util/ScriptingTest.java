package eu.fbk.rdfpro.util;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.junit.Test;

import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.Transformer;

public class ScriptingTest {

    @Test
    public void test() throws RDFHandlerException {
        final Transformer transformer = Scripting.compile(Transformer.class,
                "js: emit(h, subj(q), rdf:type, 'john');", "q", "h");
        final List<Statement> stmts = new ArrayList<>();
        final RDFHandler handler = RDFHandlers.wrap(stmts);
        ValueFactory vf = Statements.VALUE_FACTORY;
        final Statement stmt = vf.createStatement(vf.createIRI("ex:s"), RDF.TYPE,
                vf.createIRI("ex:c"));
        transformer.transform(stmt, handler);
        System.out.println(stmts);
    }

}
