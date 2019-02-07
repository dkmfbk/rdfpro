package eu.fbk.rdfpro.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.junit.Assert;
import org.junit.Test;

public class StatementMatcherTest {

    @Test
    public void test() {

        final ValueFactory vf = Statements.VALUE_FACTORY;
        final IRI uri1 = vf.createIRI("ex:uri1");
        final IRI uri2 = vf.createIRI("ex:uri2");
        final Literal lit1 = vf.createLiteral("label");

        final StatementMatcher matcher = StatementMatcher.builder()
                .addValues(null, RDF.TYPE, OWL.THING, null, null, "x")
                .addValues(null, RDFS.LABEL, null, null, null, "y", "w").build(null);

        Assert.assertFalse(matcher.match(uri1, OWL.SAMEAS, uri1, null));

        Assert.assertFalse(matcher.match(uri1, RDF.TYPE, RDFS.CLASS, uri2));
        Assert.assertEquals(ImmutableList.of(),
                matcher.map(uri1, RDF.TYPE, RDFS.CLASS, uri2, String.class));

        Assert.assertTrue(matcher.match(uri1, RDF.TYPE, OWL.THING, uri2));
        Assert.assertEquals(ImmutableList.of("x"),
                matcher.map(uri1, RDF.TYPE, OWL.THING, uri2, String.class));
        Assert.assertEquals(ImmutableList.of("x"),
                matcher.map(uri1, RDF.TYPE, OWL.THING, uri2, Object.class));
        Assert.assertEquals(ImmutableList.of(),
                matcher.map(uri1, RDF.TYPE, OWL.THING, uri2, Integer.class));

        Assert.assertTrue(matcher.match(uri1, RDFS.LABEL, lit1, uri2));
        Assert.assertEquals(ImmutableSet.of("y", "w"),
                ImmutableSet.copyOf(matcher.map(uri1, RDFS.LABEL, lit1, uri2, String.class)));
    }

    @Test
    public void test2() {

        final ValueFactory vf = Statements.VALUE_FACTORY;
        final IRI uri1 = vf.createIRI("ex:uri1");
        final IRI uri2 = vf.createIRI("ex:uri2");
        final Literal lit1 = vf.createLiteral("label");

        final StatementMatcher matcher = StatementMatcher.builder()
                .addValues(null, null, null, null, null, "x")
                .addValues(null, RDFS.LABEL, null, null, null, "y", "w").build(null);

        Assert.assertTrue(matcher.match(uri1, RDF.TYPE, RDFS.CLASS, uri2));
        Assert.assertEquals(ImmutableList.of("x"),
                matcher.map(uri1, RDF.TYPE, RDFS.CLASS, uri2, String.class));

        Assert.assertTrue(matcher.match(uri1, RDFS.LABEL, lit1, uri2));
        Assert.assertEquals(ImmutableSet.of("x", "y", "w"),
                ImmutableSet.copyOf(matcher.map(uri1, RDFS.LABEL, lit1, uri2, String.class)));
    }

}
