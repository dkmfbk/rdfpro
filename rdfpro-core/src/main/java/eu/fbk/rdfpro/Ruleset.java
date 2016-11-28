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
package eu.fbk.rdfpro;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Algebra;
import eu.fbk.rdfpro.util.Environment;
import eu.fbk.rdfpro.util.IO;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.QuadModel;
import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.vocab.RR;

/**
 * A set of rules, with associated optional meta-vocabulary.
 */
public final class Ruleset {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ruleset.class);

    public static final Ruleset RHODF = Ruleset
            .fromRDF(Environment.getProperty("rdfpro.rules.rhodf"));

    public static final Ruleset RDFS = Ruleset
            .fromRDF(Environment.getProperty("rdfpro.rules.rdfs"));

    public static final Ruleset OWL2RL = Ruleset
            .fromRDF(Environment.getProperty("rdfpro.rules.owl2rl"));

    private final Set<Rule> rules;

    private final Set<IRI> metaVocabularyTerms;

    @Nullable
    private transient Map<IRI, Rule> ruleIndex;

    @Nullable
    private transient List<RuleSplit> ruleSplits;

    private transient int hash;

    @Nullable
    private transient Boolean deletePossible;

    @Nullable
    private transient Boolean insertPossible;

    @Nullable
    private transient BloomFilter<Integer>[] filters;

    /**
     * Creates a new ruleset with the rules and meta-vocabulary terms supplied.
     *
     * @param rules
     *            the rules
     * @param metaVocabularyTerms
     *            the meta-vocabulary terms
     */
    public Ruleset(final Iterable<Rule> rules, @Nullable final Iterable<IRI> metaVocabularyTerms) {

        this.rules = ImmutableSet.copyOf(Ordering.natural().sortedCopy(rules));
        this.metaVocabularyTerms = metaVocabularyTerms == null ? ImmutableSet.of() //
                : ImmutableSet.copyOf(Ordering.from(Statements.valueComparator())
                        .sortedCopy(metaVocabularyTerms));

        this.ruleIndex = null;
        this.ruleSplits = null;
        this.hash = 0;
        this.deletePossible = null;
        this.insertPossible = null;
        this.filters = null;
    }

    /**
     * Returns the rules in this ruleset.
     *
     * @return a set of rules sorted by phase index, fixpoint flag and rule IRI.
     */
    public Set<Rule> getRules() {
        return this.rules;
    }

    /**
     * Returns the rule with the ID specified, or null if there is no rule for that ID.
     *
     * @param ruleID
     *            the rule ID
     * @return the rule for the ID specified, or null if it does not exist
     */
    @Nullable
    public Rule getRule(final IRI ruleID) {
        if (this.ruleIndex == null) {
            final ImmutableMap.Builder<IRI, Rule> builder = ImmutableMap.builder();
            for (final Rule rule : this.rules) {
                builder.put(rule.getID(), rule);
            }
            this.ruleIndex = builder.build();
        }
        return this.ruleIndex.get(Objects.requireNonNull(ruleID));
    }

    /**
     * Returns the meta-vocabulary terms associated to this ruleset.
     *
     * @return a set of term IRIs, sorted by IRI
     */
    public Set<IRI> getMetaVocabularyTerms() {
        return this.metaVocabularyTerms;
    }

    /**
     * Returns true if the evaluation of the ruleset may cause some statements to be deleted. This
     * happens if the ruleset contains at least a rule with a non-null DELETE expression.
     *
     * @return true, if statement deletion is possible with this ruleset
     */
    public boolean isDeletePossible() {
        if (this.deletePossible == null) {
            boolean deletePossible = false;
            for (final Rule rule : this.rules) {
                if (rule.getDeleteExpr() != null) {
                    deletePossible = true;
                    break;
                }
            }
            this.deletePossible = deletePossible;
        }
        return this.deletePossible;
    }

    /**
     * Returs true if the evaluation of the ruleset may cause some statements to be inserted. This
     * happens if the ruleset contains at least a rule with a non-null INSERT expression.
     *
     * @return true, if statement insertion is possible with this ruleset
     */
    public boolean isInsertPossible() {
        if (this.insertPossible == null) {
            boolean insertPossible = false;
            for (final Rule rule : this.rules) {
                if (rule.getInsertExpr() != null) {
                    insertPossible = true;
                    break;
                }
            }
            this.insertPossible = insertPossible;
        }
        return this.insertPossible;
    }

    /**
     * Returns true if the supplied statement can be matched by a pattern in a WHERE or DELETE
     * expression of some rule in this ruleset. Matchable statements (as defined before) are able
     * to affect the rule evaluation process, whereas non-matchable statements can be safely
     * removed with no effect on rule evaluation (i.e., no effect on the sets of statements
     * deleted or inserted by the engine where evaluating this ruleset on some input data).
     *
     * @param stmt
     *            the statement to test
     * @return true, if the statement is matchable by some WHERE or DELETE rule expression
     */
    @SuppressWarnings("unchecked")
    public boolean isMatchable(final Statement stmt) {

        // Initialize bloom filters at first access
        if (this.filters == null) {
            synchronized (this) {
                if (this.filters == null) {
                    // Extract all statement patterns from WHERE and DELETE exprs
                    final List<StatementPattern> patterns = new ArrayList<>();
                    for (final Rule rule : this.rules) {
                        patterns.addAll(rule.getDeletePatterns());
                        patterns.addAll(rule.getWherePatterns());
                    }

                    // Look for wildcard pattern
                    BloomFilter<Integer>[] filters = new BloomFilter[] { null, null, null, null };
                    for (final StatementPattern p : patterns) {
                        if (!p.getSubjectVar().hasValue() && !p.getPredicateVar().hasValue()
                                && !p.getObjectVar().hasValue()
                                && (p.getContextVar() == null || !p.getContextVar().hasValue())) {
                            filters = new BloomFilter[0];
                            Ruleset.LOGGER.debug("Rules contain <?s ?p ?o ?c> pattern");
                            break;
                        }
                    }

                    // Initialize SPOC bloom filters; null value = no constant in that position
                    final int[] counts = new int[4];
                    for (int i = 0; i < filters.length; ++i) {
                        final Set<Integer> hashes = Sets.newHashSet();
                        for (final StatementPattern pattern : patterns) {
                            final List<Var> vars = pattern.getVarList();
                            if (i < vars.size() && vars.get(i).hasValue()) {
                                hashes.add(vars.get(i).getValue().hashCode());
                            }
                        }
                        counts[i] = hashes.size();
                        if (!hashes.isEmpty()) {
                            final BloomFilter<Integer> filter = BloomFilter
                                    .create(Funnels.integerFunnel(), hashes.size());
                            filters[i] = filter;
                            for (final Integer hash : hashes) {
                                filter.put(hash);
                            }

                        }
                    }

                    // Atomically store the constructed filter list
                    this.filters = filters;
                    if (Ruleset.LOGGER.isDebugEnabled()) {
                        Ruleset.LOGGER.debug(
                                "Number of constants in pattern components: "
                                        + "s = {}, p = {}, o = {}, c = {}",
                                counts[0], counts[1], counts[2], counts[3]);
                    }
                }
            }
        }

        // All statements matchable in case a wildcard <?s ?p ?o ?c> is present
        if (this.filters.length == 0) {
            return true;
        }

        // Otherwise, at least a component of the statement must match one of the filter constants
        final BloomFilter<Integer> subjFilter = this.filters[0];
        final BloomFilter<Integer> predFilter = this.filters[1];
        final BloomFilter<Integer> objFilter = this.filters[2];
        final BloomFilter<Integer> ctxFilter = this.filters[3];
        return subjFilter != null && subjFilter.mightContain(stmt.getSubject().hashCode()) //
                || predFilter != null && predFilter.mightContain(stmt.getPredicate().hashCode()) //
                || objFilter != null && objFilter.mightContain(stmt.getObject().hashCode()) //
                || ctxFilter != null && ctxFilter.mightContain(stmt.getContext() == null ? 0 : //
                        stmt.getContext().hashCode());
    }

    /**
     * Returns the ruleset with the ABox rules obtained from the rules of this ruleset and the
     * TBox data specified. The method split the DELETE, INSERT and WHERE expressions of rules
     * into TBox and ABox parts. Rules with only TBox parts are discarded. Rules with only ABox
     * parts are included as is in the returned ruleset. Rules with both ABox part (in DELETE
     * and/or INSERT expressions) and TBox part (in WHERE expression) are instead 'exploded' by
     * computing all the possible bindings of the TBox part w.r.t. the supplied TBox data, adding
     * to the resulting ruleset all the rules obtained by injecting those bindings. Note: this
     * method does not modify in any way the supplied TBox data. In general, it is necessary to
     * close it w.r.t. relevant inference rules (which can be the same rules of this ruleset)
     * before computing the ABox ruleset. In this case, the closure should be computed manually
     * before calling this method.
     *
     * @param tboxData
     *            the TBox data
     * @return the resulting ruleset
     */
    public Ruleset getABoxRuleset(final QuadModel tboxData) {

        // Split rules if necessary, caching the result
        if (this.ruleSplits == null) {
            final List<RuleSplit> splits = new ArrayList<>(this.rules.size());
            for (final Rule rule : this.rules) {
                splits.add(new RuleSplit(rule, this.metaVocabularyTerms));
            }
            this.ruleSplits = splits;
        }

        // Compute preprocessing rules that obtain bindings of TBox WHERE exprs
        final Map<IRI, List<BindingSet>> bindingsMap = new ConcurrentHashMap<>();
        final List<Runnable> queries = new ArrayList<>();
        int numABoxRules = 0;
        for (final RuleSplit split : this.ruleSplits) {
            if (split.aboxDeleteExpr != null || split.aboxInsertExpr != null) {
                ++numABoxRules;
                if (split.tboxWhereExpr != null) {
                    queries.add(new Runnable() {

                        @Override
                        public void run() {
                            final List<BindingSet> bindings = Lists.newArrayList(
                                    tboxData.evaluate(split.tboxWhereExpr, null, null));
                            bindingsMap.put(split.rule.getID(), bindings);
                        }

                    });
                }
            }
        }
        Environment.run(queries);

        // Compute the ABox rules using obtained bindings to explode the TBox WHERE parts
        final List<Rule> rules = new ArrayList<>();
        for (final RuleSplit split : this.ruleSplits) {
            if (split.aboxDeleteExpr != null || split.aboxInsertExpr != null) {
                final IRI id = split.rule.getID();
                final boolean fixpoint = split.rule.isFixpoint();
                final int phase = split.rule.getPhase();
                if (split.tboxWhereExpr == null) {
                    final IRI newID = Rule.newID(id.stringValue());
                    rules.add(new Rule(newID, fixpoint, phase, split.aboxDeleteExpr,
                            split.aboxInsertExpr, split.aboxWhereExpr));
                } else {
                    final Iterable<? extends BindingSet> list = bindingsMap.get(id);
                    if (list != null) {
                        for (final BindingSet b : list) {
                            final TupleExpr delete = Algebra.normalize(
                                    Algebra.rewrite(split.aboxDeleteExpr, b),
                                    Statements.VALUE_NORMALIZER);
                            final TupleExpr insert = Algebra.normalize(
                                    Algebra.rewrite(split.aboxInsertExpr, b),
                                    Statements.VALUE_NORMALIZER);
                            final TupleExpr where = Algebra.normalize(
                                    Algebra.rewrite(split.aboxWhereExpr, b),
                                    Statements.VALUE_NORMALIZER);
                            if (!Objects.equals(insert, where) || delete != null) {
                                final IRI newID = Rule.newID(id.stringValue());
                                rules.add(new Rule(newID, fixpoint, phase, delete, insert, where));
                            }
                        }
                    }
                }
            }
        }
        Ruleset.LOGGER.debug(
                "{} ABox rules derived from {} TBox quads and {} original rules "
                        + "({} with ABox components, {} with TBox & ABox components)",
                rules.size(), tboxData.size(), this.rules.size(), numABoxRules, queries.size());

        // Build and return the resulting ruleset
        return new Ruleset(rules, this.metaVocabularyTerms);
    }

    /**
     * Returns the ruleset obtained by rewriting the rules of this ruleset according to the GLOBAL
     * graph inference mode, using the global graph IRI specified. Meta-vocabulary terms are not
     * affected.
     *
     *
     * @param globalGraph
     *            the IRI of the global graph where to insert new quads; if null, quads will be
     *            inserted in the default graph {@code sesame:nil}
     * @return a ruleset with the rewritten rules and the same meta-vocabulary terms of this
     *         ruleset
     * @see Rule#rewriteGlobalGM(IRI)
     */
    public Ruleset rewriteGlobalGM(@Nullable final IRI globalGraph) {
        final List<Rule> rewrittenRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            rewrittenRules.add(rule.rewriteGlobalGM(globalGraph));
        }
        return new Ruleset(rewrittenRules, this.metaVocabularyTerms);
    }

    /**
     * Returns the ruleset obtained by rewriting the rules of this ruleset according to the
     * SEPARATE graph inference mode. Meta-vocabulary terms are not affected.
     *
     * @return a ruleset with the rewritten rules and the same meta-vocabulary terms of this
     *         ruleset
     * @see Rule#rewriteSeparateGM()
     */
    public Ruleset rewriteSeparateGM() {
        final List<Rule> rewrittenRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            rewrittenRules.add(rule.rewriteSeparateGM());
        }
        return new Ruleset(rewrittenRules, this.metaVocabularyTerms);
    }

    /**
     * Returns the ruleset obtained by rewriting the rules of this ruleset according to the STAR
     * graph inference mode, using the global graph IRI supplied. Meta-vocabulary terms are not
     * affected.
     *
     * @param globalGraph
     *            the IRI of the global graph whose quads are 'imported' in other graphs; if null,
     *            the default graph {@code sesame:nil} will be used
     * @return a ruleset with the rewritten rules and the same meta-vocabulary terms of this
     *         ruleset
     * @see Rule#rewriteStarGM(IRI)
     */
    public Ruleset rewriteStarGM(@Nullable final IRI globalGraph) {
        final List<Rule> rewrittenRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            rewrittenRules.add(rule.rewriteStarGM(globalGraph));
        }
        return new Ruleset(rewrittenRules, this.metaVocabularyTerms);
    }

    /**
     * Returns the ruleset obtained by replacing selected variables in the rules of this ruleset
     * with the constant values dictated by the supplied bindings. Meta-vocabulary terms are not
     * affected.
     *
     * @param bindings
     *            the variable = value bindings to use for rewriting rules; if null or empty, no
     *            rewriting will take place
     * @return a ruleset with the rewritten rules and the same meta-vocabulary terms of this
     *         ruleset
     * @see Rule#rewriteVariables(BindingSet)
     */
    public Ruleset rewriteVariables(@Nullable final BindingSet bindings) {
        if (bindings == null || bindings.size() == 0) {
            return this;
        }
        final List<Rule> rewrittenRules = new ArrayList<>();
        for (final Rule rule : this.rules) {
            rewrittenRules.add(rule.rewriteVariables(bindings));
        }
        return new Ruleset(rewrittenRules, this.metaVocabularyTerms);
    }

    /**
     * Returns the ruleset obtained by merging the rules in this ruleset with the same WHERE
     * expression, priority and fixpoint flag. Meta-vocabulary terms are not affected.
     *
     * @return a ruleset with the merged rules and the same meta-vocabulary terms of this ruleset
     * @see Rule#mergeSameWhereExpr(Iterable)
     */
    public Ruleset mergeSameWhereExpr() {
        final List<Rule> rules = Rule.mergeSameWhereExpr(this.rules);
        return rules.size() == this.rules.size() ? this
                : new Ruleset(rules, this.metaVocabularyTerms);
    }

    /**
     * {@inheritDoc} Two rulesets are equal if they have the same rules and meta-vocabulary terms.
     */
    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Ruleset)) {
            return false;
        }
        final Ruleset other = (Ruleset) object;
        return this.rules.equals(other.rules)
                && this.metaVocabularyTerms.equals(other.metaVocabularyTerms);
    }

    /**
     * {@inheritDoc} The returned hash code depends on all the rules and meta-vocabulary terms in
     * this ruleset.
     */
    @Override
    public int hashCode() {
        if (this.hash == 0) {
            this.hash = Objects.hash(this.rules, this.metaVocabularyTerms);
        }
        return this.hash;
    }

    /**
     * {@inheritDoc} The returned string lists, on multiple lines, all the meta-vocabulary terms
     * and rules in this ruleset.
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("META-VOCABULARY TERMS (").append(this.metaVocabularyTerms.size())
                .append("):");
        for (final IRI metaVocabularyTerm : this.metaVocabularyTerms) {
            builder.append("\n")
                    .append(Statements.formatValue(metaVocabularyTerm, Namespaces.DEFAULT));
        }
        builder.append("\n\nRULES (").append(this.rules.size()).append("):");
        for (final Rule rule : this.rules) {
            builder.append("\n").append(rule);
        }
        return builder.toString();
    }

    /**
     * Emits the RDF serialization of the ruleset. Emitted triples are placed in the default
     * graph.
     *
     * @param output
     *            the collection where to add emitted RDF statements, not null
     * @return the supplied collection
     */
    public <T extends Collection<? super Statement>> T toRDF(final T output) {

        // Emit meta-vocabulary terms
        final ValueFactory vf = Statements.VALUE_FACTORY;
        for (final IRI metaVocabularyTerm : this.metaVocabularyTerms) {
            vf.createStatement(metaVocabularyTerm, RDF.TYPE, RR.META_VOCABULARY_TERM);
        }

        // Emit rules
        for (final Rule rule : this.rules) {
            rule.toRDF(output);
        }
        return output;
    }

    /**
     * Parses a ruleset from the supplied RDF statements. The method extracts all the rules and
     * the meta-vocabulary terms defined by supplied statements, and collects them in a new
     * ruleset.
     *
     * @param model
     *            the RDF statements, not null
     * @return the parsed ruleset
     */
    public static Ruleset fromRDF(final Iterable<Statement> model) {

        // Parse meta-vocabulary terms
        final List<IRI> metaVocabularyTerms = new ArrayList<>();
        for (final Statement stmt : model) {
            if (stmt.getSubject() instanceof IRI && RDF.TYPE.equals(stmt.getPredicate())
                    && RR.META_VOCABULARY_TERM.equals(stmt.getObject())) {
                metaVocabularyTerms.add((IRI) stmt.getSubject());
            }
        }

        // Parse rules
        final List<Rule> rules = Rule.fromRDF(model);

        // Build resulting ruleset
        return new Ruleset(rules, metaVocabularyTerms);
    }

    /**
     * Parses a ruleset from the RDF located at the specified location. The method extracts all
     * the rules and the meta-vocabulary terms defined in the RDF, and collects them in a new
     * ruleset.
     *
     * @param location
     *            the location where to load RDF data, not null
     * @return the parsed ruleset
     */
    public static Ruleset fromRDF(final String location) {
        final String url = IO.extractURL(location).toString();
        final RDFSource rulesetSource = RDFSources.read(true, true, null, null, false, false,
                false, url);
        return Ruleset.fromRDF(rulesetSource);
    }

    /**
     * Merges multiple rulesets in a single ruleset. The method collects all the rules and
     * meta-vocabulary terms in the specified rulesets, and creates a new ruleset (if necessary)
     * containing the resulting rules and terms.
     *
     * @param rulesets
     *            the rulesets to merge
     * @return the merged ruleset (possibly one of the input rulesets)
     */
    public static Ruleset merge(final Ruleset... rulesets) {
        if (rulesets.length == 0) {
            return new Ruleset(Collections.emptyList(), Collections.emptyList());
        } else if (rulesets.length == 1) {
            return rulesets[0];
        } else {
            final List<IRI> metaVocabularyTerms = new ArrayList<>();
            final List<Rule> rules = new ArrayList<>();
            for (final Ruleset ruleset : rulesets) {
                metaVocabularyTerms.addAll(ruleset.getMetaVocabularyTerms());
                rules.addAll(ruleset.getRules());
            }
            return new Ruleset(rules, metaVocabularyTerms);
        }
    }

    private static final class RuleSplit {

        final Rule rule;

        @Nullable
        final TupleExpr tboxDeleteExpr;

        @Nullable
        final TupleExpr aboxDeleteExpr;

        @Nullable
        final TupleExpr tboxInsertExpr;

        @Nullable
        final TupleExpr aboxInsertExpr;

        @Nullable
        final TupleExpr tboxWhereExpr;

        @Nullable
        final TupleExpr aboxWhereExpr;

        RuleSplit(final Rule rule, final Set<IRI> terms) {
            try {
                final TupleExpr[] deleteExprs = Algebra.splitTupleExpr(rule.getDeleteExpr(), terms,
                        -1);
                final TupleExpr[] insertExprs = Algebra.splitTupleExpr(rule.getInsertExpr(), terms,
                        -1);
                final TupleExpr[] whereExprs = Algebra
                        .splitTupleExpr(Algebra.explodeFilters(rule.getWhereExpr()), terms, 1);

                this.rule = rule;
                this.tboxDeleteExpr = deleteExprs[0];
                this.aboxDeleteExpr = deleteExprs[1];
                this.tboxInsertExpr = insertExprs[0];
                this.aboxInsertExpr = insertExprs[1];
                this.tboxWhereExpr = whereExprs[0];
                this.aboxWhereExpr = whereExprs[1];

                Ruleset.LOGGER.trace("{}", this);

            } catch (final Throwable ex) {
                throw new IllegalArgumentException("Cannot split rule " + rule.getID(), ex);
            }
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder();
            builder.append("Splitting of rule ").append(this.rule.getID());
            this.toStringHelper(builder, "\n  DELETE original: ", this.rule.getDeleteExpr());
            this.toStringHelper(builder, "\n  DELETE tbox:     ", this.tboxDeleteExpr);
            this.toStringHelper(builder, "\n  DELETE abox:     ", this.aboxDeleteExpr);
            this.toStringHelper(builder, "\n  INSERT original: ", this.rule.getInsertExpr());
            this.toStringHelper(builder, "\n  INSERT tbox:     ", this.tboxInsertExpr);
            this.toStringHelper(builder, "\n  INSERT abox:     ", this.aboxInsertExpr);
            this.toStringHelper(builder, "\n  WHERE  original: ", this.rule.getWhereExpr());
            this.toStringHelper(builder, "\n  WHERE  tbox:     ", this.tboxWhereExpr);
            this.toStringHelper(builder, "\n  WHERE  abox:     ", this.aboxWhereExpr);
            return builder.toString();
        }

        private void toStringHelper(final StringBuilder builder, final String prefix,
                @Nullable final TupleExpr expr) {
            if (expr != null) {
                builder.append(prefix).append(Algebra.format(expr));
            }
        }

    }

}
