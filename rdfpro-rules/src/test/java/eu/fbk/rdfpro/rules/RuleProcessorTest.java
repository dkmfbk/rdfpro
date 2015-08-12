package eu.fbk.rdfpro.rules;

import java.util.Arrays;

import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.RDFProcessors;
import eu.fbk.rdfpro.RDFSource;
import eu.fbk.rdfpro.RDFSources;

public class RuleProcessorTest {

    // args: filename_dynamic_data_file + @rules_parameters
    // example: data.tql.gz -r rdfs tbox.tql.gz

    public static void main(final String... args) throws Throwable {

        int index = 0;
        for (; index < args.length; ++index) {
            if (args[index].equals("--")) {
                break;
            }
        }

        RDFSource source = RDFSources.read(true, true, null, null,
                Arrays.copyOfRange(args, 0, index));

        // final List<Statement> statements = new ArrayList<>();
        // source.emit(RDFHandlers.wrap(statements), 1);
        // source = RDFSources.wrap(statements);

        final RDFProcessor processor = RDFProcessors.parse(true,
                "@rules " + String.join(" ", Arrays.copyOfRange(args, index + 1, args.length)));

        final int n = 10;
        long ts = 0;
        for (int i = 0; i < n; ++i) {
            if (i == 5) {
                ts = System.currentTimeMillis();
            }
            processor.apply(source, RDFHandlers.NIL, 1);
        }
        System.out.println("Average: " + (System.currentTimeMillis() - ts) / (n - 5));
    }

}
