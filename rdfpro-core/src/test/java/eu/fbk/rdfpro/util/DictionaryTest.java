package eu.fbk.rdfpro.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.junit.Assert;
import org.junit.Test;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.RDFSources;

public class DictionaryTest {

    @Test
    public void test() {
        final ValueFactory vf = Statements.VALUE_FACTORY;
        final List<Value> values = Lists.newArrayList();
        values.add(vf.createLiteral("str"));
        values.add(vf.createLiteral("str", "en"));
        values.add(vf.createLiteral("str", "it"));
        values.add(vf.createLiteral("12", XMLSchema.INT));
        values.add(vf.createLiteral("13.1", XMLSchema.DECIMAL));
        values.add(vf.createLiteral("13.5", XMLSchema.FLOAT));
        values.add(vf.createLiteral("13311", XMLSchema.INTEGER));
        values.add(vf.createLiteral(true));
        values.add(OWL.THING);
        values.add(OWL.THING);
        final int codes[] = new int[values.size()];
        final Dictionary d = Dictionary.newMemoryDictionary();
        for (int i = 0; i < values.size(); ++i) {
            System.out.println(values.get(i));
            codes[i] = d.encode(values.get(i));
        }
        for (int i = 0; i < values.size(); ++i) {
            System.out.println(Integer.toHexString(codes[i]));
            final Value v = d.decode(codes[i]);
            Assert.assertEquals(values.get(i), v);
        }
    }

    @Test
    public void test2() throws RDFHandlerException {
        final Set<Value> set = new HashSet<>();
        final Dictionary d = Dictionary.newMemoryDictionary();
        final long ts = System.currentTimeMillis();
        RDFSources
                .read(true, true, null, null, false, false, "/mnt/data/pikes/data/abox10m.tql.gz")
                .emit(new AbstractRDFHandler() {

                    private final AtomicInteger counter = new AtomicInteger(0);

                    @Override
                    public void handleStatement(final Statement stmt) throws RDFHandlerException {
                        final int counter = this.counter.incrementAndGet();
                        if (counter % 10000 == 0) {
                            System.out.println(counter);
                        }
                        final int sc = d.encode(stmt.getSubject());
                        final int pc = d.encode(stmt.getPredicate());
                        final int oc = d.encode(stmt.getObject());
                        final int cc = d.encode(stmt.getContext());
                        final Resource sv = (Resource) d.decode(sc);
                        final IRI pv = (IRI) d.decode(pc);
                        final Value ov = d.decode(oc);
                        final Resource cv = (Resource) d.decode(cc);
                        Assert.assertEquals(stmt.getSubject(), sv);
                        Assert.assertEquals(stmt.getPredicate(), pv);
                        Assert.assertEquals(stmt.getObject(), ov);
                        Assert.assertEquals(stmt.getContext(), cv);
                        // add(stmt.getSubject());
                        // add(stmt.getPredicate());
                        // add(stmt.getObject());
                        // set.add(stmt.getContext());
                    }

                    private void add(final Value value) {
                        set.add(value);
                        if (value instanceof IRI) {
                            final IRI uri = (IRI) value;
                            if (uri.getLocalName().length() > 0) {
                                set.add(Statements.VALUE_FACTORY.createIRI(uri.getNamespace()));
                            }
                        }
                    }

                }, 1);
        System.out.println(System.currentTimeMillis() - ts);
        System.out.println(d);
        System.out.println(set.size());
    }

}
