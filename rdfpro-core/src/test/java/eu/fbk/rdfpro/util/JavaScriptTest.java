package eu.fbk.rdfpro.util;

import org.junit.Test;

public class JavaScriptTest {

    @Test
    public void testCompile() throws Throwable {
        JavaScript.include(JavaScriptTest.class.getResource("JavaScriptTest.js"));
        compileAndCall("function call(name, surname) { var x = function(s) { print(s + ' ' + name); }; x(foaf:name); return 'hello ' + name + ' ' + surname; }");
        compileAndCall("'hello ' + n + ' ' + s;");
    }

    private void compileAndCall(final String script) throws Throwable {
        JavaScript.compile(Callback.class, script, "n", "s").call("john", "smith");
    }

    public interface Callback {

        String call(String name, String surname);

    }

}
