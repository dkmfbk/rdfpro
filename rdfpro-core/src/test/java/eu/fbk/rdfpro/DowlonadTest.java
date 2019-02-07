package eu.fbk.rdfpro;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;

public class DowlonadTest {

    public static void main(final String... args) {
        final RDFProcessor p = RDFProcessors.parse(true,
                "@download -q 'select ?s ?p ?o ?c { graph ?c { ?s ?p ?o } }' "
                        + "<http://localhost:50080/openrdf-sesame/repositories/owlimprova_1524>");
        p.apply(RDFSources.NIL, new AbstractRDFHandler() {

            @Override
            public void handleStatement(Statement statement) throws RDFHandlerException {
                System.out.println(statement);
            }

        }, 1);
    }

}
