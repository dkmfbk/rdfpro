package eu.fbk.rdfpro.util;

import org.junit.Test;

public class NamespacesTest {

    @Test
    public void test() {
        System.out.println(Namespaces.DEFAULT.prefixesFor("http://purl.org/dc/terms/"));
        System.out.println(Namespaces.DEFAULT.prefixesFor("http://purl.org/dc/dcmitype/"));
        System.out.println(Namespaces.DEFAULT.prefixFor("http://purl.org/dc/terms/"));
    }

}
