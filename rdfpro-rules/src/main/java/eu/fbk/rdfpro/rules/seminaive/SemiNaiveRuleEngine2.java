package eu.fbk.rdfpro.rules.seminaive;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.rules.Rule;
import eu.fbk.rdfpro.rules.RuleEngine;
import eu.fbk.rdfpro.rules.Ruleset;
import eu.fbk.rdfpro.rules.model.QuadModel;
import eu.fbk.rdfpro.rules.util.StatementDeduplicator;
import eu.fbk.rdfpro.rules.util.StatementMatcher;
import eu.fbk.rdfpro.rules.util.StatementTemplate;
import eu.fbk.rdfpro.util.Statements;

public class SemiNaiveRuleEngine2 extends RuleEngine {

    public SemiNaiveRuleEngine2(final Ruleset ruleset) {
        super(ruleset);
    }

    @Override
    protected void doEval(final Collection<Statement> model) {
        // TODO Auto-generated method stub
        super.doEval(model);
    }

    @Override
    protected RDFHandler doEval(final RDFHandler handler) {
        // TODO Auto-generated method stub
        return super.doEval(handler);
    }

    private static abstract class Phase {

        private final boolean handlerSupported;

        private final boolean modelSupported;

        Phase(final boolean handlerSupported, final boolean modelSupported) {
            this.handlerSupported = handlerSupported;
            this.modelSupported = modelSupported;
        }

        public Phase normalize(final Function<Object, Object> normalizer) {
            return this;
        }

        public RDFHandler eval(final RDFHandler handler) {
            throw new Error();
        }

        public void eval(final QuadModel model) {
            throw new Error();
        }

        public final boolean isHandlerSupported() {
            return this.handlerSupported;
        }

        public final boolean isModelSupported() {
            return this.modelSupported;
        }

    }

    private static final class StreamPhase extends Phase {

        @Nullable
        private final StatementMatcher deleteMatcher;

        @Nullable
        private final StatementMatcher insertMatcher;

        private final Statement[] axioms;

        private final boolean fixpoint;

        private StreamPhase(@Nullable final StatementMatcher deleteMatcher,
                @Nullable final StatementMatcher insertMatcher, final Statement[] axioms,
                final boolean fixpoint) {

            // We only work in streaming
            super(true, false);

            // Store supplied structures
            this.deleteMatcher = deleteMatcher;
            this.insertMatcher = insertMatcher;
            this.axioms = axioms;
            this.fixpoint = fixpoint;
        }

        static StreamPhase create(final Iterable<Rule> rules) {

            // Allocate builders for the two matchers and an empty axiom list
            StatementMatcher.Builder db = null;
            StatementMatcher.Builder ib = null;
            final List<Statement> axioms = new ArrayList<>();
            boolean containsFixpointRule = false;
            boolean containsNonFixpointRule = false;

            // Populate matchers by iterating over supplied rules
            for (final Rule rule : rules) {
                assert rule.isSafe() && rule.isStreamable();
                containsFixpointRule |= rule.isFixpoint();
                containsNonFixpointRule |= !rule.isFixpoint();
                if (!rule.getWherePatterns().isEmpty()) {
                    final StatementPattern wp = rule.getWherePatterns().iterator().next();
                    for (final StatementPattern ip : rule.getInsertPatterns()) {
                        ib = ib != null ? ib : StatementMatcher.builder();
                        ib.addPattern(wp, new StatementTemplate(wp, ip));
                    }
                    if (!rule.getDeletePatterns().isEmpty()) {
                        db = db != null ? db : StatementMatcher.builder();
                        db.addPattern(wp);
                    }
                } else {
                    for (final StatementPattern ip : rule.getInsertPatterns()) {
                        final Value subj = ip.getSubjectVar().getValue();
                        final Value pred = ip.getPredicateVar().getValue();
                        final Value obj = ip.getObjectVar().getValue();
                        final Value ctx = ip.getContextVar() == null ? null : ip.getContextVar()
                                .getValue();
                        if (subj instanceof Resource && pred instanceof URI
                                && (ctx == null || ctx instanceof Resource)) {
                            axioms.add(Statements.VALUE_FACTORY.createStatement((Resource) subj,
                                    (URI) pred, obj, (Resource) ctx));
                        }
                    }
                }
            }
            assert containsFixpointRule ^ containsNonFixpointRule;

            // Create a new StreamPhase object, using null when there are no deletions or
            // insertions possible, and determining whether fixpoint evaluation should occur
            return new StreamPhase(db == null ? null : db.build(null), ib == null ? null
                    : ib.build(null), axioms.toArray(new Statement[axioms.size()]),
                    containsFixpointRule);
        }

        @Override
        public Phase normalize(final Function<Object, Object> normalizer) {

            // Normalize delete matchers and insert matchers with associated templates
            final Function<Object, Object> matcherNormalizer = (final Object object) -> {
                if (object instanceof StatementTemplate) {
                    return ((StatementTemplate) object).normalize(normalizer);
                } else {
                    return normalizer.apply(object);
                }
            };
            final StatementMatcher normalizedDeleteMatcher = this.deleteMatcher == null ? null
                    : this.deleteMatcher.normalize(matcherNormalizer);
            final StatementMatcher normalizedInsertMatcher = this.insertMatcher == null ? null
                    : this.insertMatcher.normalize(matcherNormalizer);

            // Normalize axioms
            final Statement[] normalizedAxioms = this.axioms.clone();
            for (int i = 0; i < normalizedAxioms.length; ++i) {
                final Statement s = normalizedAxioms[i];
                normalizedAxioms[i] = Statements.VALUE_FACTORY.createStatement(
                        (Resource) normalizer.apply(s.getSubject()),
                        (URI) normalizer.apply(s.getPredicate()),
                        (Value) normalizer.apply(s.getObject()),
                        (Resource) normalizer.apply(s.getContext()));
            }

            // Return a normalized copy of this phase object
            return new StreamPhase(normalizedDeleteMatcher, normalizedInsertMatcher,
                    normalizedAxioms, this.fixpoint);
        }

        @Override
        public RDFHandler eval(final RDFHandler handler) {
            return new AbstractRDFHandlerWrapper(handler) {

                private final StatementDeduplicator deduplicator = StatementDeduplicator
                        .newPartialDeduplicator(6, false, true);

                @Override
                public void startRDF() throws RDFHandlerException {

                    // Delegate
                    super.startRDF();

                    // Emit axioms
                    for (final Statement axiom : StreamPhase.this.axioms) {
                        process(axiom, true); // axioms are inferred
                    }
                }

                @Override
                public void handleStatement(final Statement stmt) throws RDFHandlerException {

                    // Delegate to recursive method process(), marking the statement as explicit
                    process(stmt, false);
                }

                private void process(final Statement stmt, final boolean inferred)
                        throws RDFHandlerException {

                    // If statement was already seen, discard it
                    if (!this.deduplicator.isNew(stmt)) {
                        return;
                    }

                    // If the statement was previously inferred and we are not doing fixpoint,
                    // emit it as it is as no further processing may occur
                    if (inferred && !StreamPhase.this.fixpoint) {
                        super.handleStatement(stmt);
                        return;
                    }

                    // Otherwise, emit the statement only if it does not match a delete pattern
                    if (StreamPhase.this.deleteMatcher == null
                            || !StreamPhase.this.deleteMatcher.match(stmt.getSubject(),
                                    stmt.getPredicate(), stmt.getObject(), stmt.getContext())) {
                        super.handleStatement(stmt);
                    }

                    // Apply insert part by looking up and applying insert templates
                    if (StreamPhase.this.insertMatcher != null) {
                        for (final StatementTemplate template : StreamPhase.this.insertMatcher
                                .map(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(),
                                        stmt.getContext(), StatementTemplate.class)) {
                            final Statement stmt2 = template.apply(stmt);
                            if (stmt2 != null) {
                                process(stmt2, true);
                            }
                        }
                    }
                }

            };
        }
    }

}
