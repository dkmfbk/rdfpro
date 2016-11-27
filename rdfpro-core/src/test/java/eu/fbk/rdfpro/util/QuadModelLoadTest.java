package eu.fbk.rdfpro.util;

import org.eclipse.rdf4j.model.Statement;

import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFSource;
import eu.fbk.rdfpro.RDFSources;

public class QuadModelLoadTest {

    public static void main(final String... args) throws Throwable {

        final QuadModel model = QuadModel.create();

        // final Path path = Files.createTempDirectory("sailmodel");
        // path.toFile().deleteOnExit();
        // final MemoryStore sail = new MemoryStore(path.toFile());
        // sail.setPersist(false);
        // sail.initialize();
        // final SailConnection connection = sail.getConnection();
        // connection.begin(IsolationLevels.NONE);
        // final QuadModel model = QuadModel.wrap(connection, true);

        final long ts = System.currentTimeMillis();

        final RDFSource source = RDFSources.read(false, true, null, null, args);
        source.emit(RDFHandlers.synchronize(RDFHandlers.wrap(model)), 1);
        Runtime.getRuntime().gc();
        final long mem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        final long ts2 = System.currentTimeMillis();
        System.out.println(model.size() + " " + mem + " " + (ts2 - ts));

        int sum = 0;
        for (final Statement stmt : model) {
            // sum += stmt.hashCode();
            // ++sum;
            sum += stmt.getSubject().stringValue().hashCode();
            sum += stmt.getObject().stringValue().hashCode();
            sum += stmt.getPredicate().stringValue().hashCode();
        }

        final long ts3 = System.currentTimeMillis();
        System.out.println(sum + " " + (ts3 - ts2));

        IO.closeQuietly(model);
    }

}
