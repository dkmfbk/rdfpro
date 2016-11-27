package eu.fbk.rdfpro.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class QuadModelTest {

    @Rule
    public Timeout timeout = new Timeout(1000, TimeUnit.MILLISECONDS);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final String parameter;

    private Literal literal1;

    private Literal literal2;

    private IRI iri1;

    private IRI iri2;

    private BNode bnode1;

    private BNode bnode2;

    private IRI ctx1;

    private IRI ctx2;

    @Parameters
    public static Collection<String> parameters() {
        return Arrays.asList(new String[] { "memory", "sail", "repository", "hash", "tree" });
    }

    public QuadModelTest(final String parameter) {
        this.parameter = parameter;
    }

    private QuadModel newModel() {
        try {
            switch (this.parameter) {
            case "memory": {
                return QuadModel.create();
            }
            case "sail": {
                final Path path = Files.createTempDirectory("sailmodel");
                path.toFile().deleteOnExit();
                final MemoryStore sail = new MemoryStore(path.toFile());
                sail.setPersist(false);
                sail.initialize();
                final SailConnection connection = sail.getConnection();
                connection.begin(IsolationLevels.NONE);
                return QuadModel.wrap(connection, true);
            }
            case "repository": {
                final Path path = Files.createTempDirectory("sailmodel");
                path.toFile().deleteOnExit();
                final MemoryStore sail = new MemoryStore(path.toFile());
                sail.setPersist(false);
                final Repository repository = new SailRepository(sail);
                repository.initialize();
                final RepositoryConnection connection = repository.getConnection();
                connection.begin();
                return QuadModel.wrap(connection, true);
            }
            case "hash": {
                return QuadModel.wrap(new LinkedHashModel());
            }
            case "tree": {
                return QuadModel.wrap(new TreeModel());
            }
            default:
                throw new Error();
            }
        } catch (final Throwable ex) {
            Throwables.throwIfUnchecked(ex);
            throw new RuntimeException(ex);
        }
    }

    private void disposeModel(final QuadModel model) {
        IO.closeQuietly(model);
    }

    @Before
    public void setUp() {
        final ValueFactory vf = Statements.VALUE_FACTORY;
        this.iri1 = vf.createIRI("urn:test:iri:1");
        this.iri2 = vf.createIRI("urn:test:iri:2");
        this.bnode1 = vf.createBNode("bnode1");
        this.bnode2 = vf.createBNode("bnode2");
        this.literal1 = vf.createLiteral("test literal 1");
        this.literal2 = vf.createLiteral("test literal 2");
        this.ctx1 = vf.createIRI("urn:test:ctx:1");
        this.ctx2 = vf.createIRI("urn:test:ctx:2");
    }

    @After
    public void tearDown() {
    }

    @Test
    public final void testEmpty() {
        final QuadModel model = this.newModel();
        try {
            Assert.assertEquals(0, model.size());
            Assert.assertEquals(null, model.objectValue());
            Assert.assertEquals(null, model.objectLiteral());
            Assert.assertEquals(null, model.objectResource());
            Assert.assertEquals(null, model.objectURI());
            Assert.assertEquals(null, model.objectString());
        } finally {
            this.disposeModel(model);
        }
    }

    @Test
    public final void testSingleLiteral() {
        final QuadModel model = this.newModel();
        try {
            model.add(this.iri1, RDFS.LABEL, this.literal1, this.ctx1);
            Assert.assertEquals(1, model.size());
            Assert.assertEquals(
                    new HashSet<Statement>(ImmutableSet.of(Statements.VALUE_FACTORY
                            .createStatement(this.iri1, RDFS.LABEL, this.literal1, this.ctx1))),
                    new HashSet<Statement>(model));
            Assert.assertTrue(model.contains(null, null, this.literal1));
            Assert.assertTrue(model.contains(null, null, this.literal1, this.ctx1));
            Assert.assertFalse(model.filter(null, null, this.literal1).isEmpty());
            Assert.assertFalse(model.filter(null, null, this.literal1, this.ctx1).isEmpty());
            Assert.assertTrue(model.filter(null, null, this.literal1, (Resource) null).isEmpty());
            Assert.assertEquals(this.literal1, model.objectValue());
            Assert.assertEquals(this.literal1, model.objectLiteral());
            Assert.assertEquals(this.literal1.stringValue(), model.objectString());
            QuadModelTest.assertThrown(Throwable.class, () -> {
                model.objectResource();
            });
            QuadModelTest.assertThrown(Throwable.class, () -> {
                model.objectURI();
            });
        } finally {
            this.disposeModel(model);
        }
    }

    @Test
    public final void testSingleIRI() {
        final QuadModel model = this.newModel();
        try {
            final Statement stmt = Statements.VALUE_FACTORY.createStatement(this.iri1, RDFS.LABEL,
                    this.iri2, this.ctx1);
            model.add(stmt);
            Assert.assertTrue(model.contains(this.iri1, RDFS.LABEL, this.iri2, this.ctx1));
            Assert.assertTrue(model.contains(this.iri1, RDFS.LABEL, this.iri2));
            Assert.assertTrue(model.contains(stmt));
            Assert.assertTrue(ImmutableSet.copyOf(model).equals(ImmutableSet.of(stmt)));
            Assert.assertEquals(1, model.size());
            Assert.assertEquals(this.iri2, model.objectValue());
            Assert.assertEquals(this.iri2, model.objectResource());
            Assert.assertEquals(this.iri2, model.objectURI());
            QuadModelTest.assertThrown(Throwable.class, () -> {
                model.objectLiteral();
            });
        } finally {
            this.disposeModel(model);
        }
    }

    @Test
    public final void testSingleBNode() {
        final QuadModel model = this.newModel();
        try {
            model.add(this.iri1, RDFS.LABEL, this.bnode1, this.ctx1);
            Assert.assertEquals(1, model.size());
            Assert.assertEquals(this.bnode1, model.objectValue());
            Assert.assertEquals(this.bnode1, model.objectResource());
            Assert.assertEquals(this.bnode1.stringValue(), model.objectString());
            QuadModelTest.assertThrown(Throwable.class, () -> {
                model.objectLiteral();
            });
            QuadModelTest.assertThrown(Throwable.class, () -> {
                model.objectURI();
            });
        } finally {
            this.disposeModel(model);
        }
    }

    @Test
    public final void testMultiple() {
        for (final Value obj1 : new Value[] { this.iri1, this.bnode1, this.literal1 }) {
            for (final Value obj2 : new Value[] { this.iri2, this.bnode2, this.literal2 }) {
                final QuadModel model = this.newModel();
                try {
                    Assert.assertEquals(true, model.isEmpty());
                    Assert.assertEquals(0, model.size());
                    model.add(this.iri1, RDFS.LABEL, obj1, this.ctx1);
                    model.add(this.iri1, RDFS.LABEL, obj2, this.ctx2);
                    Assert.assertEquals(false, model.isEmpty());
                    Assert.assertEquals(2, model.size());
                    Assert.assertEquals(2, model.size(null, RDFS.LABEL, null));
                    Assert.assertEquals(1, model.size(null, RDFS.LABEL, null, this.ctx1));
                    Assert.assertEquals(1, model.size(null, RDFS.LABEL, null, this.ctx2));
                    Assert.assertEquals(1, model.size(this.iri1, RDFS.LABEL, obj2, this.ctx2));
                    Assert.assertEquals(0, model.size(this.iri2, null, null));
                    Assert.assertEquals(2,
                            model.size(this.iri1, null, null, this.ctx1, this.ctx2));
                    Assert.assertEquals(ImmutableSet.of(RDFS.LABEL), model.predicates());
                    Assert.assertEquals(ImmutableSet.of(this.iri1), model.subjects());
                    Assert.assertEquals(ImmutableSet.of(obj1, obj2), model.objects());
                    Assert.assertEquals(ImmutableSet.of(this.ctx1, this.ctx2), model.contexts());
                    QuadModelTest.assertThrown(Throwable.class, () -> {
                        model.objectLiteral();
                    });
                    QuadModelTest.assertThrown(Throwable.class, () -> {
                        model.objectURI();
                    });
                    QuadModelTest.assertThrown(Throwable.class, () -> {
                        model.objectValue();
                    });
                    QuadModelTest.assertThrown(Throwable.class, () -> {
                        model.objectResource();
                    });
                    QuadModelTest.assertThrown(Throwable.class, () -> {
                        model.objectString();
                    });
                    ValueFactory vf = Statements.VALUE_FACTORY;
                    final Set<Statement> set = new HashSet<>();
                    set.add(vf.createStatement(this.iri1, RDFS.LABEL, obj1, this.ctx1));
                    set.add(vf.createStatement(this.iri1, RDFS.LABEL, obj2, this.ctx2));
                    Set<Statement> actual = ImmutableSet.copyOf(model);
                    Assert.assertEquals(set, actual);
                    Assert.assertFalse(model.remove(null, null, obj2, this.ctx1));
                    Assert.assertEquals(2, model.size());
                    Assert.assertTrue(model.remove(null, null, obj2, this.ctx2));
                    Assert.assertEquals(1, model.size());
                    set.clear();
                    set.add(vf.createStatement(this.iri1, RDFS.LABEL, obj1, this.ctx1));
                    actual = ImmutableSet.copyOf(model);
                    Assert.assertEquals(set, actual);
                    model.clear();
                    Assert.assertEquals(0, model.size());
                } finally {
                    this.disposeModel(model);
                }
            }
        }
    }

    @Test
    public final void testNamespaces() {
        final QuadModel model = this.newModel();
        Assert.assertEquals(0, model.getNamespaces().size());
        model.setNamespace("test", "urn:test");
        Assert.assertEquals(1, model.getNamespaces().size());
        Assert.assertEquals(new SimpleNamespace("test", "urn:test"), model.getNamespace("test"));
        model.setNamespace(new SimpleNamespace("test", "urn:test2"));
        Assert.assertEquals(1, model.getNamespaces().size());
        Assert.assertEquals(new SimpleNamespace("test", "urn:test2"), model.getNamespace("test"));
        model.removeNamespace("test");
        Assert.assertEquals(0, model.getNamespaces().size());
        Assert.assertEquals(null, model.getNamespace("test"));
    }

    @Test
    public final void testUnmodifiable() {
        final QuadModel model = this.newModel();
        model.add(this.iri1, RDFS.LABEL, this.bnode1, this.ctx1);
        QuadModelTest.assertThrown(Throwable.class, () -> {
            model.unmodifiable().add(this.iri1, RDFS.LABEL, this.literal1, this.ctx2);
        });
    }

    @Test
    public final void testEvaluate() throws MalformedQueryException {
        final String queryString = "SELECT ?s WHERE { GRAPH <" + this.ctx1 + "> { ?s ?p ?o } }";
        final TupleExpr expr = Algebra.parseTupleExpr(queryString, null, null);
        final QuadModel model = this.newModel();
        model.add(this.iri1, RDFS.LABEL, this.literal1, this.ctx1);
        model.add(this.iri1, RDFS.LABEL, this.literal2, this.ctx2);
        final Iterator<BindingSet> iterator = model.evaluate(expr, null,
                new ListBindingSet(ImmutableList.of("p"), RDFS.LABEL));
        try {
            Assert.assertTrue(iterator.hasNext());
            final BindingSet bindings = iterator.next();
            Assert.assertEquals(1, bindings.size());
            Assert.assertEquals(this.iri1, bindings.getValue("s"));
            Assert.assertFalse(iterator.hasNext());
        } finally {
            IO.closeQuietly(iterator);
        }
    }

    private static <T extends Throwable> T assertThrown(final Class<T> exceptionClazz,
            final Runnable runnable) {
        try {
            runnable.run();
        } catch (final Throwable ex) {
            if (exceptionClazz.isInstance(ex)) {
                return exceptionClazz.cast(ex);
            }
        }
        Assert.fail("Expected " + exceptionClazz.getName());
        return null;
    }

}
