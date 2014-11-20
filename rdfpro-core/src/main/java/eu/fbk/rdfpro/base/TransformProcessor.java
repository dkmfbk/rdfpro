package eu.fbk.rdfpro.base;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.util.Util;

public class TransformProcessor extends RDFProcessor {

    private final Transformer transformer;

    static TransformProcessor doCreate(final String... args) {
        final Options options = Options.parse("+", args);
        final String spec = String.join(" ", options.getPositionalArgs(String.class));
        return new TransformProcessor(newSimpleTransformer(spec));
    }

    public TransformProcessor(final Transformer transformer) {
        this.transformer = Util.checkNotNull(transformer);
    }

    @Override
    public final RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        return Handlers.decouple(new Handler(Handlers.decouple(sink)));
    }

    public static Transformer newSimpleTransformer(final String spec) {
        return new RuleTransformer(Util.checkNotNull(spec));
    }

    public interface Transformer {

        void transform(Statement statement, RDFHandler handler) throws RDFHandlerException;

        public static Transformer forRules(final String rules) {
            return new RuleTransformer(rules);
        }

    }

    private static class RuleTransformer implements Transformer {

        private final ValueDecisor subjectDecisor;

        private final ValueDecisor predicateDecisor;

        private final ValueDecisor objectDecisor;

        private final ValueDecisor contextDecisor;

        private final ValueDecisor typeDecisor;

        public RuleTransformer(final String rules) {

            this.subjectDecisor = new ValueDecisor();
            this.predicateDecisor = new ValueDecisor();
            this.objectDecisor = new ValueDecisor();
            this.contextDecisor = new ValueDecisor();
            this.typeDecisor = new ValueDecisor();

            ValueDecisor decisor = null;
            int rank = 0;

            for (String token : rules.split("\\s+")) {
                try {
                    token = token.trim();
                    if (token.isEmpty()) {
                        continue;
                    }
                    if (token.length() == 1) {
                        final char ch = Character.toLowerCase(token.charAt(0));
                        if (ch == 's') {
                            decisor = this.subjectDecisor;
                        } else if (ch == 'p') {
                            decisor = this.predicateDecisor;
                        } else if (ch == 'o') {
                            decisor = this.objectDecisor;
                        } else if (ch == 'c') {
                            decisor = this.contextDecisor;
                        } else if (ch == 't') {
                            decisor = this.typeDecisor;
                        } else {
                            throw new IllegalArgumentException("Invalid modifier "
                                    + token.charAt(0));
                        }
                    } else {
                        final Decision decision = new Decision(token.charAt(0) == '+', rank++);
                        final String condition = token.substring(1);
                        decisor.configure(decision, condition);
                    }
                } catch (final Throwable ex) {
                    throw new IllegalArgumentException("Invalid rule specification '" + rules
                            + "': " + ex.getMessage(), ex);
                }
            }
        }

        @Override
        public void transform(final Statement statement, final RDFHandler handler)
                throws RDFHandlerException {

            final Resource s = statement.getSubject();
            final URI p = statement.getPredicate();
            final Value o = statement.getObject();
            final Resource c = statement.getContext();

            final Decision d1 = this.subjectDecisor.decide(s);
            final Decision d2 = this.predicateDecisor.decide(p);
            final Decision d3 = this.objectDecisor.decide(o);
            final Decision d4 = this.contextDecisor.decide(c);

            final Decision d12 = d1.rank < d2.rank ? d1 : d2;
            final Decision d34 = d3.rank < d4.rank ? d3 : d4;
            Decision d = d12.rank < d34.rank ? d12 : d34;

            if (p.equals(RDF.TYPE)) {
                final Decision d5 = this.typeDecisor.decide(o);
                d = d.rank < d5.rank ? d : d5;
            }

            if (d.include) {
                handler.handleStatement(statement);
            }
        }

        private static class ValueDecisor {

            private final StringDecisor uriDecisor = new StringDecisor();

            private final StringDecisor bnodeDecisor = new StringDecisor();

            private final StringDecisor literalDecisor = new StringDecisor();

            private final StringDecisor datatypeDecisor = new StringDecisor();

            private final StringDecisor languageDecisor = new StringDecisor();

            void configure(final Decision decision, final String condition) {

                if (condition.equals("u*")) {
                    this.uriDecisor.configure(decision);
                } else if (condition.equals("b*")) {
                    this.bnodeDecisor.configure(decision);
                } else if (condition.equals("l*")) {
                    this.literalDecisor.configure(decision);
                } else if (condition.equals("@*")) {
                    this.languageDecisor.configure(decision);
                } else if (condition.equals("^^*")) {
                    this.datatypeDecisor.configure(decision);
                } else if (condition.equals("*")) {
                    this.uriDecisor.configure(decision);
                    this.bnodeDecisor.configure(decision);
                    this.literalDecisor.configure(decision);
                } else if (condition.startsWith("@")) {
                    this.languageDecisor.configure(decision, condition.substring(1));
                } else if (condition.startsWith("^^")) {
                    final String string = condition.substring(2);
                    final String namespace = Statements.PREFIX_TO_NS_MAP.get(string);
                    if (namespace != null) {
                        this.datatypeDecisor.configure(decision, namespace);
                    } else {
                        final URI uri = (URI) Statements.parseValue(string);
                        this.datatypeDecisor.configure(decision, uri.stringValue());
                    }
                } else {
                    final String namespace = Statements.PREFIX_TO_NS_MAP.get(condition);
                    if (namespace != null) {
                        this.uriDecisor.configure(decision, namespace);
                    } else {
                        final Value value = Statements.parseValue(condition);
                        if (value instanceof URI) {
                            this.uriDecisor.configure(decision, value.stringValue());
                        } else if (value instanceof BNode) {
                            this.bnodeDecisor.configure(decision, ((BNode) value).getID());
                        } else if (value instanceof Literal) {
                            this.literalDecisor.configure(decision, ((Literal) value).getLabel());
                        }
                    }
                }
            }

            Decision decide(final Value value) {

                if (value instanceof URI) {
                    final URI uri = (URI) value;
                    final Decision d1 = this.uriDecisor.decide(uri.getNamespace());
                    final Decision d2 = this.uriDecisor.decide(uri.stringValue());
                    return d1.rank < d2.rank ? d1 : d2;

                } else if (value instanceof BNode) {
                    return this.bnodeDecisor.decide(((BNode) value).getID());

                } else if (value instanceof Literal) {
                    final Literal literal = (Literal) value;
                    Decision d = this.literalDecisor.decide(literal.getLabel());
                    if (literal.getDatatype() != null) {
                        final URI dt = literal.getDatatype();
                        final Decision d1 = this.datatypeDecisor.decide(dt.getNamespace());
                        final Decision d2 = this.datatypeDecisor.decide(dt.stringValue());
                        final Decision dd = d1.rank < d2.rank ? d1 : d2;
                        d = d.rank < dd.rank ? d : dd;
                    } else if (literal.getLanguage() != null) {
                        final Decision dl = this.languageDecisor.decide(literal.getLanguage());
                        d = d.rank < dl.rank ? d : dl;
                    }
                    return d;

                } else if (value == null) {
                    return decide(SESAME.NIL);
                }

                throw new Error("Unknown value type for " + value);
            }
        }

        private static class StringDecisor {

            private final Map<String, Decision> rules;

            private Decision decision; // default

            StringDecisor() {
                this.rules = new HashMap<String, Decision>();
                this.decision = Decision.DEFAULT;
            }

            void configure(final Decision decision, final String condition) {
                if (decision.rank < this.decision.rank) {
                    this.rules.put(condition, decision);
                }
            }

            void configure(final Decision decision) {

                if (this.decision == null || this.decision.rank > decision.rank) {
                    this.decision = decision;
                }
                for (final String key : this.rules.keySet()) {
                    if (this.rules.get(key).rank > this.decision.rank) {
                        this.rules.put(key, this.decision);
                    }
                }
            }

            Decision decide(final String string) {

                if (this.rules.isEmpty()) {
                    return this.decision;
                } else {
                    final Decision d = this.rules.get(string);
                    if (d != null) {
                        return d;
                    }
                    return this.decision;
                }
            }

        }

        private static class Decision {

            private static final Decision DEFAULT = new Decision(true, Integer.MAX_VALUE);

            final boolean include;

            final int rank;

            Decision(final boolean include, final int rank) {
                this.include = include;
                this.rank = rank;
            }

        }

    }

    private final class Handler extends HandlerWrapper {

        public Handler(final RDFHandler handler) {
            super(handler);
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            TransformProcessor.this.transformer.transform(statement, this.handler);
        }

    }

}
