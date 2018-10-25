package eu.fbk.rdfpro.util;

import java.util.Arrays;

import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.junit.Test;

import eu.fbk.rdfpro.Ruleset;

public class AlgebraTest {

    @Test
    public void test() throws Throwable {
        System.out.println(Ruleset.RHODF);
        // final ValueExpr expr = Algebra.parseValueExpr("CONCAT(STR(rr:mint(rdf:type)), ?x)",
        // null,
        // Namespaces.DEFAULT.uriMap());
        // System.out.println(Algebra.evaluateValueExpr(expr, new
        // ListBindingSet(Arrays.asList("x"),
        // Statements.VALUE_FACTORY.createLiteral("__test"))));
    }

}
