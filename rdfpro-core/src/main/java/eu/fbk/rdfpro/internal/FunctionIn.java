package eu.fbk.rdfpro.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandler;
import eu.fbk.rdfpro.RDFSource;
import eu.fbk.rdfpro.RDFSources;
import eu.fbk.rdfpro.util.Algebra;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.vocab.RR;

public class FunctionIn implements Function {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionIn.class);

    private static final Cache<List<Value>, Set<Value>> CACHE = CacheBuilder.newBuilder()
            .expireAfterAccess(60000, TimeUnit.MILLISECONDS).build();

    @Override
    public String getURI() {
        return RR.IN.stringValue();
    }

    @Override
    public Value evaluate(final ValueFactory factory, final Value... args)
            throws ValueExprEvaluationException {

        // Check arguments
        if (args.length < 2 || args.length > 3) {
            throw new ValueExprEvaluationException(
                    "Expected 2 to 3 parameters: value, file [, selector]; got " + args.length);
        }

        // Extract arguments
        final Value value = args[0];
        final Value file = args[1];
        final Value selector = args.length == 3 ? args[2] : null;

        // Build cache key for looking up set of admissible values
        final List<Value> key = selector == null ? ImmutableList.of(file)
                : ImmutableList.of(file, selector);

        // Lookup admissible values, or extract them from file and selector
        final Set<Value> set;
        try {
            set = CACHE.get(key, () -> load(file, selector));
        } catch (final ExecutionException ex) {
            Throwables.throwIfUnchecked(ex.getCause());
            throw new RuntimeException(ex);
        }

        // Return true if the value is in the set of admissible values
        return factory.createLiteral(set.contains(value));
    }

    private static Set<Value> load(final Value file, @Nullable final Value selector)
            throws IOException {

        // Build a value predicate based on the optional selector
        final Predicate<Value> predicate;
        if (selector == null) {
            predicate = Predicates.alwaysTrue();
        } else {
            final ValueExpr expr = Algebra.parseValueExpr(selector.stringValue(), null,
                    Namespaces.DEFAULT.uriMap());
            final List<String> vars = ImmutableList.copyOf(Algebra.extractVariables(expr, false));
            if (vars.size() > 1) {
                throw new IllegalArgumentException("At most one variable among ?i, ?l, ?b, ?v "
                        + "allowed in selector, got " + vars);
            } else if (vars.size() == 0) {
                final Value r = Algebra.evaluateValueExpr(expr, new EmptyBindingSet());
                predicate = r instanceof Literal && ((Literal) r).booleanValue() ? null
                        : Predicates.alwaysFalse();
            } else {
                final String var = Iterables.getFirst(vars, null);
                final Class<? extends Value> type = var.equals("i") ? IRI.class
                        : var.equals("l") ? Literal.class
                                : var.equals("b") ? BNode.class : Value.class;
                predicate = v -> {
                    if (type.isInstance(v)) {
                        final BindingSet bindings = new ListBindingSet(vars, v);
                        final Value r = Algebra.evaluateValueExpr(expr, bindings);
                        return r instanceof Literal && ((Literal) r).booleanValue();
                    }
                    return false;
                };
            }
        }

        // Read all values from the specified file, handling two cases
        final Set<Value> set = Sets.newConcurrentHashSet();
        final long ts = System.currentTimeMillis();
        final String location = file.stringValue();
        if (Rio.getParserFormatForFileName("test." + IO.extractExtension(location)).isPresent()) {

            // Case 1: RDF file specified -> extract all RDF values out of it
            final RDFSource source = RDFSources.read(true, true, null, null, null, true, location);
            source.emit(new AbstractRDFHandler() {

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {
                    if (predicate.test(stmt.getSubject())) {
                        set.add(stmt.getSubject());
                    }
                    if (predicate.test(stmt.getPredicate())) {
                        set.add(stmt.getPredicate());
                    }
                    if (predicate.test(stmt.getObject())) {
                        set.add(stmt.getObject());
                    }
                    if (predicate.test(stmt.getContext())) {
                        set.add(stmt.getContext() != null ? stmt.getContext() : SESAME.NIL);
                    }
                }

            }, 1);

        } else {
            // Case 2: TSV file -> tokenize on \t, parse tokens, and accumulate parsable values
            try (BufferedReader in = new BufferedReader(
                    IO.utf8Reader(IO.buffer(IO.read(location))))) {
                in.lines().forEach(line -> {
                    for (final String token : line.split("\t")) {
                        Value value;
                        try {
                            value = Statements.parseValue(token, Namespaces.DEFAULT);
                        } catch (final Throwable ex) {
                            value = Statements.VALUE_FACTORY.createLiteral(token);
                        }
                        if (predicate.test(value)) {
                            set.add(value);
                        }
                    }
                });
            }
        }

        // Return the extracted & filtered set
        LOGGER.info("{} value(s) extracted from {} in {} ms", set.size(), location,
                System.currentTimeMillis() - ts);
        return set;
    }

}
