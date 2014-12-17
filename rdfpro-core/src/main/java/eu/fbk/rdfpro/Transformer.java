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
package eu.fbk.rdfpro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

/**
 * A generic {@code Statement -> Statement [0..*]} transformer.
 * <p>
 * A {@code Transformer} is a stateless function mapping an input {@code Statement} to zero or
 * more output {@code Statement}s, via method {@link #transform(Statement, RDFHandler)}; for
 * efficiency reasons, output statements are not returned as a list but are instead forwarded to a
 * supplied {@link RDFHandler} (this avoids creation of intermediate array/lists objects, as well
 * as the efficient mapping of a statement to a large number of output statements). While somehow
 * similar to an {@link RDFProcessor}, a {@code Transformer} operates on a statement at a time,
 * whereas an {@code RDFProcessor} transforms streams of statements, i.e., it can perform
 * transformations that depends on combinations or sets of statements rather than on a single
 * input statement.
 * </p>
 * <p>
 * Implementations of this interface should be thread-safe, i.e., they should support multiple
 * threads calling concurrently method {@code transform()}; this can be however achieved by means
 * of synchronization.
 * </p>
 * <p>
 * A number of constants and static factory methods provide access to basic {@code Transformer}
 * implementations:
 * </p>
 * <ul>
 * <li>{@link #NIL} is the null transformer, mapping every statement to an empty statement list;</li>
 * <li>{@link #IDENTITY} is the identity transformer, mapping every statement to itself;</li>
 * <li>{@link #filter(Predicate)} and {@link #filter(String, Predicate)} create transformers that
 * apply a given {@code Predicate} to check the input statements or its components, emitting the
 * statement only if the check is successful;</li>
 * <li>{@link #map(Function)} and {@link #map(String, Function)} create transformers that apply a
 * given {@code Function} to transform the input statement or its components, emitting the result
 * to the supplied {@code RDFHandler};</li>
 * <li>{@link #set(String, Value...)} create transformers that replace selected components of the
 * input statement;</li>
 * <li>{@link #sequence(Transformer...)} chains multiple transformers so that the output of one is
 * used as the input of the next;</li>
 * <li>{@link #parallel(Transformer...)} calls multiple transformers in parallel, emitting the
 * concatenation of their results;</li>
 * <li>{@link #rules(String)} return a transformer that applies the filtering rules encoded in a
 * supplied string.</li>
 * </ul>
 */
@FunctionalInterface
public interface Transformer {

    /** The null {@code Transformer}, that maps each statement to an empty list of statements. */
    static Transformer NIL = new Transformer() {

        @Override
        public void transform(final Statement statement, final RDFHandler handler) {
        }

    };

    /** The identity {@code Transformer}, that maps every input statement to itself. */
    static Transformer IDENTITY = new Transformer() {

        @Override
        public void transform(final Statement statement, final RDFHandler handler)
                throws RDFHandlerException {
            handler.handleStatement(statement);
        }

    };

    /**
     * Returns a {@code Transformer} that emits only statements matching the supplied
     * {@code Predicate}.
     *
     * @param predicate
     *            the predicate, not null
     * @return the created {@code Transformer}
     */
    static Transformer filter(final Predicate<? super Statement> predicate) {
        Objects.requireNonNull(predicate);
        return new Transformer() {

            @Override
            public void transform(final Statement statement, final RDFHandler handler)
                    throws RDFHandlerException {
                if (predicate.test(statement)) {
                    handler.handleStatement(statement);
                }
            }

        };
    }

    /**
     * Returns a {@code Transformer} that emits only statements whose selected component matches
     * the specified {@code Predicate}. The predicate is evaluated on each {@code s}, {@code p},
     * {@code o} , {@code c} component given by the {@code components} string. If any of these
     * tests fails, the input statement is dropped, otherwise it is emitted to the
     * {@code RDFHandler}.
     *
     * @param components
     *            a string of symbols {@code s} , {@code p}, {@code o}, {@code c} specifying which
     *            components to test, not null
     * @param predicate
     *            the predicate to apply to selected components, not null
     * @return the created {@code Transformer}
     */
    static Transformer filter(final String components, final Predicate<? super Value> predicate) {

        Objects.requireNonNull(predicate);

        final String comp = components.trim().toLowerCase();
        final boolean[] flags = new boolean[4];
        for (int i = 0; i < comp.length(); ++i) {
            final char c = comp.charAt(i);
            final int index = c == 's' ? 0 : c == 'p' ? 1 : c == 'o' ? 2
                    : c == 'c' || c == 'g' ? 3 : -1;
            if (index < 0 || flags[index]) {
                throw new IllegalArgumentException("Invalid components '" + components + "'");
            }
            flags[index] = true;
        }

        if (!flags[0] && !flags[1] && !flags[2] && !flags[3]) {
            return IDENTITY;
        }

        return new Transformer() {

            private final boolean skipSubj = !flags[0];

            private final boolean skipPred = !flags[1];

            private final boolean skipObj = !flags[2];

            private final boolean skipCtx = !flags[3];

            @Override
            public void transform(final Statement statement, final RDFHandler handler)
                    throws RDFHandlerException {
                if ((skipSubj || predicate.test(statement.getSubject()))
                        && (skipPred || predicate.test(statement.getPredicate()))
                        && (skipObj || predicate.test(statement.getObject()))
                        && (skipCtx || predicate.test(statement.getContext()))) {
                    handler.handleStatement(statement);
                }
            }

        };
    }

    /**
     * Returns a {@code Transformer} that applies the supplied function to each input statement,
     * emitting its output if not null.
     *
     * @param function
     *            the function to apply, not null
     * @return the created {@code Transformer}
     */
    static Transformer map(final Function<? super Statement, ? extends Statement> function) {

        Objects.requireNonNull(function);

        return new Transformer() {

            @Override
            public void transform(final Statement statement, final RDFHandler handler)
                    throws RDFHandlerException {
                final Statement transformed = function.apply(statement);
                if (transformed != null) {
                    handler.handleStatement(transformed);
                }
            }

        };
    }

    /**
     * Returns a {@code Transformer} that replaces selected components of the input statement
     * using the supplied function. The function is applied to each {@code s}, {@code p},
     * {@code o} , {@code c} component given by the {@code components} string. The resulting
     * component values, if not null and compatible with expected component types, are used to
     * assemble an output statement that is emitted to the supplied {@code RDFHandler}.
     *
     * @param components
     *            a string of symbols {@code s} , {@code p}, {@code o}, {@code c} specifying the
     *            components to replace, not null
     * @param function
     *            the function to apply to selected components, not null
     * @return the created {@code Transformer}
     */
    static Transformer map(final String components,
            final Function<? super Value, ? extends Value> function) {

        Objects.requireNonNull(function);

        final String comp = components.trim().toLowerCase();
        final boolean[] flags = new boolean[4];
        for (int i = 0; i < comp.length(); ++i) {
            final char c = comp.charAt(i);
            final int index = c == 's' ? 0 : c == 'p' ? 1 : c == 'o' ? 2
                    : c == 'c' || c == 'g' ? 3 : -1;
            if (index < 0 || flags[index]) {
                throw new IllegalArgumentException("Invalid components '" + components + "'");
            }
            flags[index] = true;
        }

        if (!flags[0] && !flags[1] && !flags[2] && !flags[3]) {
            return IDENTITY;
        }

        return new Transformer() {

            private final boolean mapSubj = flags[0];

            private final boolean mapPred = flags[1];

            private final boolean mapObj = flags[2];

            private final boolean mapCtx = flags[3];

            @Override
            public void transform(final Statement statement, final RDFHandler handler)
                    throws RDFHandlerException {

                Resource subj = statement.getSubject();
                URI pred = statement.getPredicate();
                Value obj = statement.getObject();
                Resource ctx = statement.getContext();

                boolean modified = false;

                if (this.mapSubj) {
                    final Value v = function.apply(subj);
                    if (!(v instanceof Resource)) {
                        return;
                    }
                    modified |= v != subj;
                    subj = (Resource) v;
                }

                if (this.mapPred) {
                    final Value v = function.apply(pred);
                    if (!(v instanceof URI)) {
                        return;
                    }
                    modified |= v != pred;
                    pred = (URI) v;
                }

                if (this.mapObj) {
                    final Value v = function.apply(obj);
                    if (v == null) {
                        return;
                    }
                    modified |= v != obj;
                    obj = v;
                }

                if (this.mapCtx) {
                    final Value v = function.apply(ctx);
                    if (!(v instanceof Resource)) {
                        return;
                    }
                    modified |= v != ctx;
                    ctx = (Resource) v;
                }

                if (!modified) {
                    handler.handleStatement(statement);
                } else if (ctx == null) {
                    handler.handleStatement(Statements.VALUE_FACTORY.createStatement(subj, pred,
                            obj));
                } else {
                    handler.handleStatement(Statements.VALUE_FACTORY.createStatement(subj, pred,
                            obj, ctx));
                }
            }

        };
    }

    /**
     * Returns a {@code Transformer} that replaces selected components of the input statement with
     * the replacement values supplied. Parameter {@code components} is a sequence of {@code s},
     * {@code p}, {@code o}, {@code c} symbols that specify the components to replace. The i-th
     * symbol corresponds to a replacement i-th value in the array {@code values} (if the array is
     * shorter, its last element is used). An output statement is assembled by mixing unmodified
     * components with replaced ones. If a valid statement is obtained, it is emitted to the
     * supplied {@code RDFHandler}.
     *
     * @param components
     *            a string of symbols {@code s} , {@code p}, {@code o}, {@code c} specifying the
     *            components to replace, not null
     * @param values
     *            the replacement values, with i-th value corresponding to the i-th symbol (if
     *            fewer values are supplied, the last value is used for unmatched component
     *            symbols).
     * @return the created {@code Transformer}
     */
    static Transformer set(final String components, final Value... values) {

        final boolean[] flags = new boolean[4];
        final Value[] vals = new Value[4];

        final String comp = components.trim().toLowerCase();
        for (int i = 0; i < comp.length(); ++i) {
            final char c = comp.charAt(i);
            final int index = c == 's' ? 0 : c == 'p' ? 1 : c == 'o' ? 2
                    : c == 'c' || c == 'g' ? 3 : -1;
            if (index < 0 || flags[index]) {
                throw new IllegalArgumentException("Invalid components '" + components + "'");
            }
            flags[index] = true;
            vals[index] = values[Math.min(values.length - 1, i)];
        }

        if (!flags[0] && !flags[1] && !flags[2] && !flags[3]) {
            return IDENTITY;

        } else if (vals[0] != null && !(vals[0] instanceof Resource) || vals[1] != null
                && !(vals[1] instanceof URI) || vals[3] != null && !(vals[3] instanceof Resource)) {
            return NIL;

        } else {
            return new Transformer() {

                private final boolean subjFlag = flags[0];

                private final boolean predFlag = flags[1];

                private final boolean objFlag = flags[2];

                private final boolean ctxFlag = flags[3];

                private final Resource subjVal = (Resource) vals[0];

                private final URI predVal = (URI) vals[1];

                private final Value objVal = vals[2];

                private final Resource ctxVal = (Resource) vals[3];

                @Override
                public void transform(final Statement statement, final RDFHandler handler)
                        throws RDFHandlerException {

                    final Resource subj = this.subjFlag ? this.subjVal : statement.getSubject();
                    final URI pred = this.predFlag ? this.predVal : statement.getPredicate();
                    final Value obj = this.objFlag ? this.objVal : statement.getObject();
                    final Resource ctx = this.ctxFlag ? this.ctxVal : statement.getContext();

                    final ValueFactory vf = Statements.VALUE_FACTORY;
                    if (ctx == null) {
                        handler.handleStatement(vf.createStatement(subj, pred, obj));
                    } else {
                        handler.handleStatement(vf.createStatement(subj, pred, obj, ctx));
                    }
                }

            };
        }
    }

    /**
     * Returns a {@code Transformer} chaining the specified {@code Transformer}s. The first
     * supplied transformer is applied first, its output piped to the next transformer and so on,
     * with the final result sent to the supplied {@code RDFHandler}. If no transformer is
     * supplied, {@link #NIL} is returned.
     *
     * @param transformers
     *            the transformers to chain
     * @return the created {@code Transformer}
     */
    static Transformer sequence(final Transformer... transformers) {
        if (Arrays.asList(transformers).contains(null)) {
            throw new NullPointerException();
        }
        if (transformers.length == 0) {
            return NIL;
        } else if (transformers.length == 1) {
            return transformers[0];
        } else if (transformers.length == 2) {
            final Transformer first = transformers[0];
            final Transformer second = transformers[1];
            return new Transformer() {

                @Override
                public void transform(final Statement statement, final RDFHandler handler)
                        throws RDFHandlerException {
                    final List<Statement> buffer = new ArrayList<>();
                    final RDFHandler collector = RDFHandlers.wrap(buffer);
                    first.transform(statement, collector);
                    for (final Statement intermediateStatement : buffer) {
                        second.transform(intermediateStatement, handler);
                    }
                }

            };
        } else {
            final Transformer[] newTransformers = new Transformer[transformers.length - 1];
            System.arraycopy(transformers, 2, newTransformers, 1, transformers.length - 2);
            newTransformers[0] = sequence(transformers[0], transformers[1]);
            return sequence(newTransformers); // recursion
        }
    }

    /**
     * Returns a {@code Transfomer} that applies each supplied transformer in parallel, emitting
     * the concatenation of their results to the supplied {@code RDFHandler}.
     *
     * @param transformers
     *            the transformers to compose in parallel
     * @return the created {@code Transformer}
     */
    static Transformer parallel(final Transformer... transformers) {
        if (Arrays.asList(transformers).contains(null)) {
            throw new NullPointerException();
        }
        if (transformers.length == 0) {
            return NIL;
        } else if (transformers.length == 1) {
            return transformers[0];
        } else {
            return new Transformer() {

                @Override
                public void transform(final Statement statement, final RDFHandler handler)
                        throws RDFHandlerException {
                    for (final Transformer transformer : transformers) {
                        transformer.transform(statement, handler);
                    }
                }

            };
        }
    }

    /**
     * Returns a {@code Transformer} that applies the filtering rules specified by the supplied
     * string. The string is a sequence of space-separated tokens. Two-chars tokens {@code cs}
     * identify the statement component {@code c} ({@code s}, {@code p}, {@code o}, {@code c}) and
     * the its sub-component {@code s} ({@code u} = URI string, {@code n} = namespace, {@code b} =
     * BNode ID, {@code l} = literal label, {@code @} = literal language, {@code ^} = literal
     * datatype). Action tokens {@code +X}, {@code -X} and {@code *X} (X = {@code prefix:name},
     * {@code <uri>}, {@code 'literal'}, {@code langid}) respectively include, exclude or assign
     * the value of a previously selected component. Their order is irrelevant. Inclusion and
     * exclusion actions are mutually exclusive: the first require a component to match a value,
     * otherwise the statement is dropped; the latter drops the statement if the component matches
     * the given value. Assignment actions are applied to modify a statement if it has been
     * accepted by other actions.
     *
     * @param rules
     *            the filtering rules, not null
     * @return the created {@code Transformer}
     */
    static Transformer rules(final String rules) {
        return new RuleTransformer(rules);
    }

    /**
     * Applies the {@code Transformer} to the specified statement, emitting its output (possibly
     * empty) to the supplied {@code RDFHandler}. Statement emission should be performed calling
     * method {@link RDFHandler#handleStatement(Statement)}; other {@code RDFHandler} methods
     * should not be called. This method is meant to be called concurrently by multiple thread, so
     * a thread-safe implementation is required.
     *
     * @param statement
     *            the input statement, not null
     * @param handler
     *            the {@code RDFHandler} where to emit output statements
     * @throws RDFHandlerException
     *             in case of error
     */
    void transform(Statement statement, RDFHandler handler) throws RDFHandlerException;

}

final class RuleTransformer implements Transformer {

    @Nullable
    private final ValueTransformer subjectTransformer;

    @Nullable
    private final ValueTransformer predicateTransformer;

    @Nullable
    private final ValueTransformer objectTransformer;

    @Nullable
    private final ValueTransformer contextTransformer;

    @SuppressWarnings("unchecked")
    public RuleTransformer(final String spec) {

        // Initialize the arrays used to create the ValueTransformers
        final List<?>[] expressions = new List<?>[4];
        final Value[] replacements = new Value[4];
        final Boolean[] includes = new Boolean[4];
        for (int i = 0; i < 4; ++i) {
            expressions[i] = new ArrayList<String>();
        }

        // Parse the specification string
        char action = 0;
        int component = 0;
        for (final String token : spec.split("\\s+")) {
            final char ch0 = token.charAt(0);
            if (ch0 == '+' || ch0 == '-' || ch0 == '=') {
                final char ch1 = token.length() > 1 ? Character.toLowerCase(token.charAt(1)) : '?';
                component = ch1 == 's' ? 0 : ch1 == 'p' ? 1 : ch1 == 'o' ? 2 : ch1 == 'c' ? 3 : -1;
                if (component < 0) {
                    throw new IllegalArgumentException("Invalid component '" + ch1 + "' in '"
                            + spec + "'");
                }
                action = ch0;
            } else if (action == 0) {
                throw new IllegalArgumentException("Missing selector in '" + spec + "'");
            } else if (action == '=') {
                replacements[component] = Statements.parseValue(token, Namespaces.DEFAULT);
            } else {
                ((List<String>) expressions[component]).add(token);
                final Boolean include = action == '+' ? Boolean.TRUE : Boolean.FALSE;
                if (includes[component] != null && !Objects.equals(includes[component], include)) {
                    throw new IllegalArgumentException("Include (+) and exclude (-) rules both "
                            + "specified for same component in '" + spec + "'");
                }
                includes[component] = include;
            }
        }

        // Create ValueTransformers
        final ValueTransformer[] transformers = new ValueTransformer[4];
        for (int i = 0; i < 4; ++i) {
            transformers[i] = expressions[i].isEmpty() && replacements[i] == null ? null
                    : new ValueTransformer((List<String>) expressions[i], replacements[i],
                            Boolean.TRUE.equals(includes[i]));
        }
        this.subjectTransformer = transformers[0];
        this.predicateTransformer = transformers[1];
        this.objectTransformer = transformers[2];
        this.contextTransformer = transformers[3];
    }

    @Override
    public void transform(final Statement statement, final RDFHandler handler)
            throws RDFHandlerException {

        // Transform the subject; abort if result is null
        final Resource oldSubj = statement.getSubject();
        Resource newSubj = oldSubj;
        if (this.subjectTransformer != null) {
            newSubj = (Resource) this.subjectTransformer.transform(oldSubj);
            if (newSubj == null) {
                return;
            }
        }

        // Transform the predicate; abort if result is null
        final URI oldPred = statement.getPredicate();
        URI newPred = oldPred;
        if (this.predicateTransformer != null) {
            newPred = (URI) this.predicateTransformer.transform(oldPred);
            if (newPred == null) {
                return;
            }
        }

        // Transform the object; abort if result is null
        final Value oldObj = statement.getObject();
        Value newObj = oldObj;
        if (this.objectTransformer != null) {
            newObj = this.objectTransformer.transform(oldObj);
            if (newObj == null) {
                return;
            }
        }

        // Transform the context; if null use SESAME.NIL; abort if result is null
        Resource oldCtx = statement.getContext();
        oldCtx = oldCtx != null ? oldCtx : SESAME.NIL;
        Resource newCtx = oldCtx;
        if (this.contextTransformer != null) {
            newCtx = (Resource) this.contextTransformer.transform(oldCtx);
            if (newCtx == null) {
                return;
            }
        }

        // Return the possibly modified statement
        if (newSubj == oldSubj && newPred == oldPred && newObj == oldObj && newCtx == oldCtx) {
            handler.handleStatement(statement); // unchanged;
        } else if (newCtx.equals(SESAME.NIL)) {
            handler.handleStatement(Statements.VALUE_FACTORY.createStatement(newSubj, newPred,
                    newObj));
        } else {
            handler.handleStatement(Statements.VALUE_FACTORY.createStatement(newSubj, newPred,
                    newObj, newCtx));
        }
    }

    private static class ValueTransformer {

        @Nullable
        private final Value replacement;

        private final boolean include;

        // for URIs

        private final boolean matchAnyURI;

        private final Set<String> matchedURINamespaces;

        private final Set<URI> matchedURIs;

        // for BNodes

        private final boolean matchAnyBNode;

        private final Set<BNode> matchedBNodes;

        // for Literals

        private final boolean matchAnyPlainLiteral;

        private final boolean matchAnyLangLiteral;

        private final boolean matchAnyTypedLiteral;

        private final Set<String> matchedLanguages;

        private final Set<URI> matchedDatatypeURIs;

        private final Set<String> matchedDatatypeNamespaces;

        private final Set<Literal> matchedLiterals;

        ValueTransformer(final Iterable<String> matchExpressions,
                @Nullable final Value replacement, final boolean include) {

            this.replacement = replacement;
            this.include = include;

            this.matchedURINamespaces = new HashSet<>();
            this.matchedURIs = new HashSet<>();
            this.matchedBNodes = new HashSet<>();
            this.matchedLanguages = new HashSet<>();
            this.matchedDatatypeURIs = new HashSet<>();
            this.matchedDatatypeNamespaces = new HashSet<>();
            this.matchedLiterals = new HashSet<>();

            boolean matchAnyURI = false;
            boolean matchAnyBNode = false;
            boolean matchAnyPlainLiteral = false;
            boolean matchAnyLangLiteral = false;
            boolean matchAnyTypedLiteral = false;

            for (final String expression : matchExpressions) {
                if ("<*>".equals(expression)) {
                    matchAnyURI = true;
                } else if ("_:*".equals(expression)) {
                    matchAnyBNode = true;
                } else if ("*".equals(expression)) {
                    matchAnyPlainLiteral = true;
                } else if ("*@*".equals(expression)) {
                    matchAnyLangLiteral = true;
                } else if ("*^^*".equals(expression)) {
                    matchAnyTypedLiteral = true;
                } else if (expression.startsWith("*@")) {
                    this.matchedLanguages.add(expression.substring(2));
                } else if (expression.startsWith("*^^")) {
                    if (expression.endsWith(":*")) {
                        this.matchedDatatypeNamespaces.add(Namespaces.DEFAULT.uriFor(expression
                                .substring(3, expression.length() - 2)));
                    } else {
                        this.matchedDatatypeURIs.add((URI) Statements.parseValue(
                                expression.substring(3), Namespaces.DEFAULT));
                    }
                } else if (expression.endsWith(":*")) {
                    this.matchedURINamespaces.add(Namespaces.DEFAULT.uriFor(expression.substring(
                            0, expression.length() - 2)));

                } else if (expression.endsWith("*>")) {
                    this.matchedURINamespaces
                            .add(expression.substring(1, expression.length() - 2));
                } else {
                    final Value value = Statements.parseValue(expression, Namespaces.DEFAULT);
                    if (value instanceof URI) {
                        this.matchedURIs.add((URI) value);
                    } else if (value instanceof BNode) {
                        this.matchedBNodes.add((BNode) value);
                    } else if (value instanceof Literal) {
                        this.matchedLiterals.add((Literal) value);
                    }

                }
            }

            this.matchAnyURI = matchAnyURI;
            this.matchAnyBNode = matchAnyBNode;
            this.matchAnyPlainLiteral = matchAnyPlainLiteral;
            this.matchAnyLangLiteral = matchAnyLangLiteral;
            this.matchAnyTypedLiteral = matchAnyTypedLiteral;
        }

        @Nullable
        Value transform(final Value value) {
            final boolean matched = match(value);
            return this.include && !matched || !this.include && matched ? null
                    : this.replacement == null ? value : this.replacement;
        }

        private boolean match(final Value value) {
            if (value instanceof URI) {
                return this.matchAnyURI //
                        || contains(this.matchedURIs, value)
                        || containsNs(this.matchedURINamespaces, (URI) value);
            } else if (value instanceof Literal) {
                final Literal lit = (Literal) value;
                final String lang = lit.getLanguage();
                final URI dt = lit.getDatatype();
                return lang == null && dt == null
                        && this.matchAnyPlainLiteral //
                        || lang != null //
                        && (this.matchAnyLangLiteral || contains(this.matchedLanguages, lang)) //
                        || dt != null //
                        && (this.matchAnyTypedLiteral || contains(this.matchedDatatypeURIs, dt) || containsNs(
                                this.matchedDatatypeNamespaces, dt)) //
                        || contains(this.matchedLiterals, lit);
            } else {
                return this.matchAnyBNode //
                        || contains(this.matchedBNodes, value);
            }
        }

        private static boolean contains(final Set<?> set, final Object value) {
            return !set.isEmpty() && set.contains(value);
        }

        private static boolean containsNs(final Set<String> set, final URI uri) {
            return !set.isEmpty() && set.contains(uri.getNamespace());
        }

    }

}