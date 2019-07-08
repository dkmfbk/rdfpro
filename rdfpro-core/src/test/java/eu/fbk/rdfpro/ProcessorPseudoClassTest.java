package eu.fbk.rdfpro;

import org.junit.Test;

public class ProcessorPseudoClassTest {

    @Test
    public void test() {
        run("@read sample.tql.gz @pseudoclass -i 100 @write sample.out.tql.gz");
    }

    private static void run(final String command) {
        final RDFProcessor p = RDFProcessors.parse(true, command);
        p.apply(RDFSources.NIL, RDFHandlers.NIL, 1);
    }

}
