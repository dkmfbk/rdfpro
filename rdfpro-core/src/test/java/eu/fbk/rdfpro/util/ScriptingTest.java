package eu.fbk.rdfpro.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.Transformer;

public class ScriptingTest {

    @Test
    public void test() throws RDFHandlerException {
        final Transformer transformer = Scripting.compile(Transformer.class,
                "js: emit(h, subj(q), rdf:type, 'john');", "q", "h");
        final List<Statement> stmts = new ArrayList<>();
        final RDFHandler handler = RDFHandlers.wrap(stmts);
        final Statement stmt = new StatementImpl(new URIImpl("ex:s"), RDF.TYPE,
                new URIImpl("ex:c"));
        transformer.transform(stmt, handler);
        System.out.println(stmts);
    }

}
