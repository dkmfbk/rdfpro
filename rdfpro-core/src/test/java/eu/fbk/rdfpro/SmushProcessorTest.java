package eu.fbk.rdfpro;

import org.junit.Test;

public class SmushProcessorTest {

    @Test
    public void test() {
        // final RDFProcessor p = RDFProcessors.parse(true,
        // "@read /home/corcoglio/sameas.tql.gz @smush @write /tmp/sameas.unique.tql.gz");
        // p.apply(RDFSources.NIL, RDFHandlers.NIL, 1);

        run("@read /tmp/in.ttl @smush '<http://fm.fbk.eu/testing#>' @transform '+p owl:sameAs' @write /tmp/sameas.ttl");
        run("@read /tmp/in2.ttl @smush -s -c /tmp/sameas.ttl @write /tmp/out2.ttl");
    }

    private static void run(final String command) {
        final RDFProcessor p = RDFProcessors.parse(true, command);
        p.apply(RDFSources.NIL, RDFHandlers.NIL, 1);
    }

}
