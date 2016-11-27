package eu.fbk.rdfpro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;

import eu.fbk.rdfpro.util.Statements;

public class RuleProcessorTest {

    // args: filename_dynamic_data_file + @rules_parameters
    // example: data.tql.gz -r rdfs tbox.tql.gz

    public static void main(final String... args) throws Throwable {
        RuleProcessorTest.mainRules(args);
        // mainTemplate(args);
    }

    private static void mainRules(final String... args) throws Throwable {

        int index = 0;
        for (; index < args.length; ++index) {
            if (args[index].equals("--")) {
                break;
            }
        }

        final RDFSource source = RDFSources.read(true, true, null, null,
                Arrays.copyOfRange(args, 0, index));

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

    private static void mainTemplate(final String... args) throws Throwable {

        int index = 0;
        for (; index < args.length; ++index) {
            if (args[index].equals("--")) {
                break;
            }
        }

        RDFSource source = RDFSources.read(true, true, null, null,
                Arrays.copyOfRange(args, 0, index));

        final List<Statement> statements = new ArrayList<>();
        source.emit(RDFHandlers.wrap(statements), 1);
        source = RDFSources.wrap(statements);

        System.out.println(statements.size());

        final long t = System.nanoTime();
        final Value[] array = new Value[4];
        final AtomicInteger counter = new AtomicInteger(0);
        for (int i = 0; i < 1000000; ++i) {
            for (final Statement stmt : statements) {

                RuleProcessorTest.method1(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                        stmt.getContext(), array, 0);
                counter.addAndGet(array[1].hashCode() + array[3].hashCode());

                // method2(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                // stmt.getContext(), (final Resource s, final URI p, final Value o,
                // final Resource c) -> {
                // counter.addAndGet(p.hashCode() + c.hashCode());
                // });

                // Statement stmt2 = method3(stmt);
                // counter.addAndGet(stmt2.getPredicate().hashCode() +
                // stmt2.getContext().hashCode());
            }
        }
        System.out.println((System.nanoTime() - t) / 1000000);
        System.out.println(counter);
    }

    // method2 14.4 14.5 14.3 14.3
    // method1/method3 10.0 10.0 10.0

    private static void method1(final Resource s, final IRI p, final Value o, final Resource c,
            final Value[] out, int offset) {
        out[offset++] = s;
        out[offset++] = OWL.SAMEAS;
        out[offset++] = s;
        out[offset++] = OWL.THING;
    }

    private static void method2(final Resource s, final IRI p, final Value o, final Resource c,
            final QuadHandler handler) {
        handler.handle(s, OWL.SAMEAS, s, OWL.THING);
    }

    private static interface QuadHandler {

        void handle(Resource s, IRI p, Value o, Resource c);

    }

    private static Statement method3(final Statement s) {
        return Statements.VALUE_FACTORY.createStatement(s.getSubject(), OWL.SAMEAS, s.getSubject(),
                OWL.THING);
    }

}
