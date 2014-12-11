/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti <francesco.corcoglioniti@gmail.com> with support by
 * Marco Rospocher, Marco Amadori and Michele Mostarda.
 * 
 * To the extent possible under law, the author has dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JavaScript {

    private static final Logger LOGGER = LoggerFactory.getLogger(JavaScript.class);

    private static final ScriptEngineManager MANAGER = new ScriptEngineManager();

    private static final Set<URL> INCLUDES = new LinkedHashSet<URL>();

    public static void include(final URL url) {
        Objects.requireNonNull(url);
        synchronized (INCLUDES) {
            INCLUDES.add(url);
        }
    }

    public static <T> T compile(final Class<T> interfaceClass, final String spec,
            final String... parameterNames) throws Throwable {

        // Validate parameters
        Objects.requireNonNull(interfaceClass);
        Objects.requireNonNull(spec);
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("Class " + interfaceClass.getName()
                    + " is not an interface");
        }

        // Obtain the script code, either from the spec itself or from the file it points to
        String scriptText = spec;
        try {
            final Path path = Paths.get(spec);
            if (Files.isReadable(path)) {
                scriptText = new String(Files.readAllBytes(path), Charset.forName("UTF-8"));
            }
        } catch (final Throwable ex) {
            // ignore
        }

        try {
            // Try to extract the interface from the script as it is
            return compileScript(interfaceClass, scriptText);

        } catch (final Throwable ex1) {
            try {
                // On failure, add the interface single function declaration before the script
                // body and try again
                final String extendedScript = wrapScript(interfaceClass, scriptText,
                        parameterNames);
                return compileScript(interfaceClass, extendedScript);

            } catch (final Throwable ex2) {
                // On failure, report all the errors to help debugging
                final StringBuilder builder = new StringBuilder();
                builder.append("Cannot extract ").append(interfaceClass).append(" from script");
                builder.append("\nError when compiled as is:\n").append(ex1.getMessage());
                builder.append("\nError when wrapped in function:\n").append(ex2.getMessage());
                builder.append("\nScript spec is:\n").append(spec);
                throw new IllegalArgumentException(builder.toString());
            }
        }
    }

    private static <T> T compileScript(final Class<T> interfaceClass, final String script)
            throws Throwable {

        // Log the operation
        LOGGER.debug("Compiling [{}]", script);

        // Obtain a new script engine for the JavaScript language (Nashorn in Java 8)
        final ScriptEngine engine = MANAGER.getEngineByExtension("js");

        // Evaluate includes
        List<URL> includes;
        synchronized (INCLUDES) {
            includes = new ArrayList<URL>(INCLUDES);
        }
        for (final URL include : includes) {
            engine.eval("load('" + include + "');");
        }

        // Evaluate script
        engine.eval(script);

        // Try to extract the desired interface out of defined script functions
        return ((Invocable) engine).getInterface(interfaceClass);
    }

    private static String wrapScript(final Class<?> interfaceClass, final String script,
            final String... parameterNames) {

        // Identify the single abstract method in the interface (throw error if more than one)
        Method method = null;
        for (final Method m : interfaceClass.getMethods()) {
            if (Modifier.isAbstract(m.getModifiers())) {
                if (method != null) {
                    throw new IllegalArgumentException("Cannot wrap script body: "
                            + interfaceClass + " defines multiple methods");
                }
                method = m;
            }
        }

        // Check that method parameters match the supplied parameter names
        final String parameterNameList = String.join(", ", parameterNames);
        if (method.getParameterCount() != parameterNames.length) {
            throw new IllegalArgumentException("Signature of method " + method
                    + " does not match parameters " + parameterNameList);
        }

        // Wrap the script body in a function using method name and parameter names
        final StringBuilder builder = new StringBuilder();
        builder.append("function ");
        builder.append(method.getName());
        builder.append("(");
        builder.append(parameterNameList);
        builder.append(") {\n");
        builder.append(script);
        builder.append("\n}");
        return builder.toString();
    }

}
