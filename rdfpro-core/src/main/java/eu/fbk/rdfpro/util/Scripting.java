/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2015 by Francesco Corcoglioniti with support by Alessio Palmero Aprosio and Marco
 * Rospocher. Contact info on http://rdfpro.fbk.eu/
 * 
 * To the extent possible under law, the authors have dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.aduna.text.ASCIIUtil;

public final class Scripting {

    private static final Logger LOGGER = LoggerFactory.getLogger(Scripting.class);

    private static final ScriptEngineManager MANAGER = new ScriptEngineManager();

    private static final Pattern LANGUAGE_PATTERN = Pattern.compile("[a-z]+");

    private static final Map<String, String> INCLUDES;

    static {
        final Map<String, String> includes = new HashMap<>();
        for (final String property : Environment.getPropertyNames()) {
            if (property.startsWith("scripting")) {
                final String[] tokens = property.split("\\.");
                if (tokens.length == 4 && tokens[2].equals("includes")) {
                    try {
                        final String language = tokens[1];
                        final String location = Environment.getProperty(property);
                        if (!LANGUAGE_PATTERN.matcher(language).matches()) {
                            throw new Error("Invalid language: " + language);
                        }
                        final StringBuilder builder = new StringBuilder();
                        try (BufferedReader reader = new BufferedReader(
                                IO.utf8Reader(IO.read(location)))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                builder.append(line).append("\n");
                            }
                        } catch (final IOException ex) {
                            throw new Error("Could not read " + location);
                        }
                        final String newInclude = builder.toString();
                        final String oldInclude = includes.get(language);
                        includes.put(language,
                                oldInclude == null ? newInclude : oldInclude + newInclude);
                        LOGGER.debug("Loaded {}", location);
                    } catch (final Throwable ex) {
                        LOGGER.error("Failed to process include " + property, ex);
                    }
                }
            }
        }
        INCLUDES = includes;
    }

    public static boolean isScript(final String spec) {
        final int index = spec.indexOf(':');
        if (index > 0) {
            final String language = spec.substring(0, index);
            return LANGUAGE_PATTERN.matcher(language).matches();
        }
        return false;
    }

    public static <T> T compile(final Class<T> interfaceClass, final String spec,
            final String... parameterNames) {

        // Validate parameters
        Objects.requireNonNull(interfaceClass);
        Objects.requireNonNull(spec);
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException(
                    "Class " + interfaceClass.getName() + " is not an interface");
        }

        // Identify the single abstract method in the interface (throw error if more than one)
        Method method = null;
        for (final Method m : interfaceClass.getMethods()) {
            if (Modifier.isAbstract(m.getModifiers())) {
                if (method != null) {
                    throw new IllegalArgumentException(
                            interfaceClass + " defines multiple methods");
                }
                method = m;
            }
        }

        // Check that method parameters match the supplied parameter names
        if (method.getParameterCount() != parameterNames.length) {
            throw new IllegalArgumentException("Signature of method " + method
                    + " does not match parameters " + String.join(", ", parameterNames));
        }

        // Extract script language and script expression from the specification
        final int index = spec.indexOf(':');
        final String language = index < 0 ? null : spec.substring(0, index);
        if (language == null || !LANGUAGE_PATTERN.matcher(language).matches()) {
            throw new IllegalArgumentException("Not a valid script specification: " + spec);
        }
        String expression = spec.substring(index + 1);

        // In case the expression points to a script file, load its content
        try {
            final Path path = Paths.get(expression);
            if (Files.isReadable(path)) {
                expression = new String(Files.readAllBytes(path), Charset.forName("UTF-8"));
            }
        } catch (final Throwable ex) {
            // ignore
        }

        // Rewrite <URI> and QNames in the script expression
        expression = rewrite(expression);

        try {
            // Try to extract the interface from the script as it is
            LOGGER.debug("Compiling expression:\n{}", expression);
            return compileHelper(interfaceClass, expression, language);

        } catch (final Throwable ex1) {
            try {
                // On failure, add the interface single function declaration before the script
                // body and try again
                expression = wrap(method, expression, language, parameterNames);
                LOGGER.debug("Compiling wrapped expression:\n{}", expression);
                return compileHelper(interfaceClass, expression, language);

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

    private static <T> T compileHelper(final Class<T> interfaceClass, final String expression,
            final String language) throws ScriptException {

        // Retrieve a script engine for the given language
        final ScriptEngine engine = MANAGER.getEngineByExtension(language);
        if (!(engine instanceof Invocable)) {
            throw new UnsupportedOperationException("Unsupported script language: " + language);
        }

        // Evaluate includes, if any
        final String include = INCLUDES.get(language);
        if (include != null) {
            engine.eval(include);
        }

        // Evaluate the script
        engine.eval(expression);

        // Extract an implementation of the class that delegates to the compiled script
        return interfaceClass.cast(((Invocable) engine).getInterface(interfaceClass));
    }

    private static String wrap(final Method method, final String expression, final String language,
            final String... parameterNames) {

        if ("js".equals(language)) {
            final StringBuilder builder = new StringBuilder();
            builder.append("function ");
            builder.append(method.getName());
            builder.append("(");
            builder.append(String.join(", ", parameterNames));
            builder.append(") {\n");
            builder.append(expression);
            if (method.getReturnType() != null && !method.getReturnType().equals(Void.class)) {
                builder.append("\nthrow \"missing return statement in supplied script\";");
            }
            builder.append("\n}");
            return builder.toString();

        } else if ("groovy".equals(language)) {
            final StringBuilder builder = new StringBuilder();
            builder.append("def ");
            builder.append(method.getName());
            builder.append("(");
            builder.append(String.join(", ", parameterNames));
            builder.append(") {\n");
            builder.append(expression);
            builder.append("\n}");
            return builder.toString();

        } else {
            throw new UnsupportedOperationException(
                    "Wrapping within a function is unsupported for language " + language);
        }
    }

    private static String rewrite(final String script) {

        final StringBuilder builder = new StringBuilder();
        final int length = script.length();
        int i = 0;

        try {
            while (i < length) {
                char c = script.charAt(i);
                if (c == '<') {
                    final int end = parseURI(script, i);
                    if (end >= 0) {
                        final String uri = script.substring(i, end);
                        builder.append("(new org.openrdf.model.impl.URIImpl(\"").append(uri)
                                .append("\"))");
                        i = end;
                    } else {
                        builder.append(c);
                        ++i;
                    }

                } else if (isPN_CHARS_BASE(c)) {
                    final int end = parseQName(script, i);
                    if (end >= 0) {
                        final String uri = Statements
                                .parseValue(script.substring(i, end), Namespaces.DEFAULT)
                                .stringValue();
                        builder.append("(new org.openrdf.model.impl.URIImpl(\"").append(uri)
                                .append("\"))");
                        i = end;
                    } else {
                        do {
                            builder.append(c);
                            c = script.charAt(++i);
                        } while (Character.isLetterOrDigit(c));
                    }

                } else if (c == '\'' || c == '\"') {
                    final char d = c; // delimiter
                    builder.append(d);
                    do {
                        c = script.charAt(++i);
                        builder.append(c);
                    } while (c != d || script.charAt(i - 1) == '\\');
                    ++i;

                } else {
                    builder.append(c);
                    ++i;
                }
            }
        } catch (final Exception ex) {
            throw new IllegalArgumentException("Illegal URI escaping near offset " + i, ex);
        }

        return builder.toString();
    }

    // Following code can be factored in Statements
    // rewriteRDFTerms(String, Function<Value, String>)

    private static int parseURI(final String string, int i) {
        final int len = string.length();
        if (string.charAt(i) != '<') {
            return -1;
        }
        for (++i; i < len; ++i) {
            final char c = string.charAt(i);
            if (c == '<' || c == '\"' || c == '{' || c == '}' || c == '|' || c == '^' || c == '`'
                    || c == '\\' || c == ' ') {
                return -1;
            }
            if (c == '>') {
                return i + 1;
            }
        }
        return -1;
    }

    private static int parseQName(final String string, int i) {
        final int len = string.length();
        char c;
        if (!isPN_CHARS_BASE(string.charAt(i))) {
            return -1;
        }
        for (; i < len; ++i) {
            c = string.charAt(i);
            if (!isPN_CHARS(c) && c != '.') {
                break;
            }
        }
        if (string.charAt(i - 1) == '.' || string.charAt(i) != ':' || i == len - 1) {
            return -1;
        }
        c = string.charAt(++i);
        if (!isPN_CHARS_U(c) && c != ':' && c != '%' && !Character.isDigit(c)) {
            return -1;
        }
        for (; i < len; ++i) {
            c = string.charAt(i);
            if (!isPN_CHARS(c) && c != '.' && c != ':' && c != '%') {
                break;
            }
        }
        if (string.charAt(i - 1) == '.') {
            return -1;
        }
        return i;
    }

    private static boolean isPN_CHARS(final int c) {
        return isPN_CHARS_U(c) || ASCIIUtil.isNumber(c) || c == 45 || c == 183
                || c >= 768 && c <= 879 || c >= 8255 && c <= 8256;
    }

    private static boolean isPN_CHARS_U(final int c) {
        return isPN_CHARS_BASE(c) || c == 95;
    }

    private static boolean isPN_CHARS_BASE(final int c) {
        return ASCIIUtil.isLetter(c) || c >= 192 && c <= 214 || c >= 216 && c <= 246
                || c >= 248 && c <= 767 || c >= 880 && c <= 893 || c >= 895 && c <= 8191
                || c >= 8204 && c <= 8205 || c >= 8304 && c <= 8591 || c >= 11264 && c <= 12271
                || c >= 12289 && c <= 55295 || c >= 63744 && c <= 64975 || c >= 65008 && c <= 65533
                || c >= 65536 && c <= 983039;
    }

}
