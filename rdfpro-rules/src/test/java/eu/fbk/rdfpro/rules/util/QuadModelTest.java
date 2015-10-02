package eu.fbk.rdfpro.rules.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openrdf.IsolationLevels;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LinkedHashModel;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.model.impl.TreeModel;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.memory.MemoryStore;

import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.util.IO;

@RunWith(Parameterized.class)
public final class QuadModelTest {

    @Rule
    public Timeout timeout = new Timeout(1000, TimeUnit.MILLISECONDS);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final String parameter;

    private Literal literal1;

    private Literal literal2;

    private URI uri1;

    private URI uri2;

    private BNode bnode1;

    private BNode bnode2;

    private URI ctx1;

    private URI ctx2;

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
            throw Throwables.propagate(ex);
        }
    }

    private void disposeModel(final QuadModel model) {
        IO.closeQuietly(model);
    }

    @Before
    public void setUp() {
        final ValueFactory vf = ValueFactoryImpl.getInstance();
        this.uri1 = vf.createURI("urn:test:uri:1");
        this.uri2 = vf.createURI("urn:test:uri:2");
        this.bnode1 = vf.createBNode("bnode1");
        this.bnode2 = vf.createBNode("bnode2");
        this.literal1 = vf.createLiteral("test literal 1");
        this.literal2 = vf.createLiteral("test literal 2");
        this.ctx1 = vf.createURI("urn:test:ctx:1");
        this.ctx2 = vf.createURI("urn:test:ctx:2");
    }

    @After
    public void tearDown() {
    }

    @Test
    public final void testEmpty() {
        final QuadModel model = newModel();
        try {
            assertEquals(0, model.size());
            assertEquals(null, model.objectValue());
            assertEquals(null, model.objectLiteral());
            assertEquals(null, model.objectResource());
            assertEquals(null, model.objectURI());
            assertEquals(null, model.objectString());
        } finally {
            disposeModel(model);
        }
    }

    @Test
    public final void testSingleLiteral() {
        final QuadModel model = newModel();
        try {
            model.add(this.uri1, RDFS.LABEL, this.literal1, this.ctx1);
            assertEquals(1, model.size());
            assertEquals(
                    new HashSet<Statement>(ImmutableSet.of(new ContextStatementImpl(this.uri1,
                            RDFS.LABEL, this.literal1, this.ctx1))), new HashSet<Statement>(model));
            assertTrue(model.contains(null, null, this.literal1));
            assertTrue(model.contains(null, null, this.literal1, this.ctx1));
            assertFalse(model.filter(null, null, this.literal1).isEmpty());
            assertFalse(model.filter(null, null, this.literal1, this.ctx1).isEmpty());
            assertTrue(model.filter(null, null, this.literal1, (Resource) null).isEmpty());
            assertEquals(this.literal1, model.objectValue());
            assertEquals(this.literal1, model.objectLiteral());
            assertEquals(this.literal1.stringValue(), model.objectString());
            assertThrown(Throwable.class, () -> {
                model.objectResource();
            });
            assertThrown(Throwable.class, () -> {
                model.objectURI();
            });
        } finally {
            disposeModel(model);
        }
    }

    @Test
    public final void testSingleURI() {
        final QuadModel model = newModel();
        try {
            final Statement stmt = new ContextStatementImpl(this.uri1, RDFS.LABEL, this.uri2,
                    this.ctx1);
            model.add(stmt);
            assertTrue(model.contains(this.uri1, RDFS.LABEL, this.uri2, this.ctx1));
            assertTrue(model.contains(this.uri1, RDFS.LABEL, this.uri2));
            assertTrue(model.contains(stmt));
            assertTrue(ImmutableSet.copyOf(model).equals(ImmutableSet.of(stmt)));
            assertEquals(1, model.size());
            assertEquals(this.uri2, model.objectValue());
            assertEquals(this.uri2, model.objectResource());
            assertEquals(this.uri2, model.objectURI());
            assertThrown(Throwable.class, () -> {
                model.objectLiteral();
            });
        } finally {
            disposeModel(model);
        }
    }

    @Test
    public final void testSingleBNode() {
        final QuadModel model = newModel();
        try {
            model.add(this.uri1, RDFS.LABEL, this.bnode1, this.ctx1);
            assertEquals(1, model.size());
            assertEquals(this.bnode1, model.objectValue());
            assertEquals(this.bnode1, model.objectResource());
            assertEquals(this.bnode1.stringValue(), model.objectString());
            assertThrown(Throwable.class, () -> {
                model.objectLiteral();
            });
            assertThrown(Throwable.class, () -> {
                model.objectURI();
            });
        } finally {
            disposeModel(model);
        }
    }

    @Test
    public final void testMultiple() {
        for (final Value obj1 : new Value[] { this.uri1, this.bnode1, this.literal1 }) {
            for (final Value obj2 : new Value[] { this.uri2, this.bnode2, this.literal2 }) {
                final QuadModel model = newModel();
                try {
                    assertEquals(true, model.isEmpty());
                    assertEquals(0, model.size());
                    model.add(this.uri1, RDFS.LABEL, obj1, this.ctx1);
                    model.add(this.uri1, RDFS.LABEL, obj2, this.ctx2);
                    assertEquals(false, model.isEmpty());
                    assertEquals(2, model.size());
                    assertEquals(2, model.size(null, RDFS.LABEL, null));
                    assertEquals(1, model.size(null, RDFS.LABEL, null, this.ctx1));
                    assertEquals(1, model.size(null, RDFS.LABEL, null, this.ctx2));
                    assertEquals(1, model.size(this.uri1, RDFS.LABEL, obj2, this.ctx2));
                    assertEquals(0, model.size(this.uri2, null, null));
                    assertEquals(2, model.size(this.uri1, null, null, this.ctx1, this.ctx2));
                    assertEquals(ImmutableSet.of(RDFS.LABEL), model.predicates());
                    assertEquals(ImmutableSet.of(this.uri1), model.subjects());
                    assertEquals(ImmutableSet.of(obj1, obj2), model.objects());
                    assertEquals(ImmutableSet.of(this.ctx1, this.ctx2), model.contexts());
                    assertThrown(Throwable.class, () -> {
                        model.objectLiteral();
                    });
                    assertThrown(Throwable.class, () -> {
                        model.objectURI();
                    });
                    assertThrown(Throwable.class, () -> {
                        model.objectValue();
                    });
                    assertThrown(Throwable.class, () -> {
                        model.objectResource();
                    });
                    assertThrown(Throwable.class, () -> {
                        model.objectString();
                    });
                    final Set<Statement> set = new HashSet<>();
                    set.add(new ContextStatementImpl(this.uri1, RDFS.LABEL, obj1, this.ctx1));
                    set.add(new ContextStatementImpl(this.uri1, RDFS.LABEL, obj2, this.ctx2));
                    Set<Statement> actual = ImmutableSet.copyOf(model);
                    assertEquals(set, actual);
                    assertFalse(model.remove(null, null, obj2, this.ctx1));
                    assertEquals(2, model.size());
                    assertTrue(model.remove(null, null, obj2, this.ctx2));
                    assertEquals(1, model.size());
                    set.clear();
                    set.add(new ContextStatementImpl(this.uri1, RDFS.LABEL, obj1, this.ctx1));
                    actual = ImmutableSet.copyOf(model);
                    assertEquals(set, actual);
                    model.clear();
                    assertEquals(0, model.size());
                } finally {
                    disposeModel(model);
                }
            }
        }
    }

    @Test
    public final void testNamespaces() {
        final QuadModel model = newModel();
        assertEquals(0, model.getNamespaces().size());
        model.setNamespace("test", "urn:test");
        assertEquals(1, model.getNamespaces().size());
        assertEquals(new NamespaceImpl("test", "urn:test"), model.getNamespace("test"));
        model.setNamespace(new NamespaceImpl("test", "urn:test2"));
        assertEquals(1, model.getNamespaces().size());
        assertEquals(new NamespaceImpl("test", "urn:test2"), model.getNamespace("test"));
        model.removeNamespace("test");
        assertEquals(0, model.getNamespaces().size());
        assertEquals(null, model.getNamespace("test"));
    }

    @Test
    public final void testUnmodifiable() {
        final QuadModel model = newModel();
        model.add(this.uri1, RDFS.LABEL, this.bnode1, this.ctx1);
        assertThrown(Throwable.class, () -> {
            model.unmodifiable().add(this.uri1, RDFS.LABEL, this.literal1, this.ctx2);
        });
    }

    @Test
    public final void testEvaluate() throws MalformedQueryException {
        final String queryString = "SELECT ?s WHERE { GRAPH <" + this.ctx1 + "> { ?s ?p ?o } }";
        final TupleExpr expr = Algebra.parseTupleExpr(queryString, null, null);
        final QuadModel model = newModel();
        model.add(this.uri1, RDFS.LABEL, this.literal1, this.ctx1);
        model.add(this.uri1, RDFS.LABEL, this.literal2, this.ctx2);
        final Iterator<BindingSet> iterator = model.evaluate(expr, null, new ListBindingSet(
                ImmutableList.of("p"), RDFS.LABEL));
        try {
            assertTrue(iterator.hasNext());
            final BindingSet bindings = iterator.next();
            assertEquals(1, bindings.size());
            assertEquals(this.uri1, bindings.getValue("s"));
            assertFalse(iterator.hasNext());
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
        fail("Expected " + exceptionClazz.getName());
        return null;
    }

}
