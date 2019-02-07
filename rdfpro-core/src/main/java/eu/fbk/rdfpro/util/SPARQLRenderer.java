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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FN;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.algebra.Add;
import org.eclipse.rdf4j.query.algebra.And;
import org.eclipse.rdf4j.query.algebra.ArbitraryLengthPath;
import org.eclipse.rdf4j.query.algebra.Avg;
import org.eclipse.rdf4j.query.algebra.BNodeGenerator;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Bound;
import org.eclipse.rdf4j.query.algebra.Clear;
import org.eclipse.rdf4j.query.algebra.Coalesce;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.Compare.CompareOp;
import org.eclipse.rdf4j.query.algebra.CompareAll;
import org.eclipse.rdf4j.query.algebra.CompareAny;
import org.eclipse.rdf4j.query.algebra.Copy;
import org.eclipse.rdf4j.query.algebra.Count;
import org.eclipse.rdf4j.query.algebra.Create;
import org.eclipse.rdf4j.query.algebra.Datatype;
import org.eclipse.rdf4j.query.algebra.DeleteData;
import org.eclipse.rdf4j.query.algebra.DescribeOperator;
import org.eclipse.rdf4j.query.algebra.Difference;
import org.eclipse.rdf4j.query.algebra.Distinct;
import org.eclipse.rdf4j.query.algebra.EmptySet;
import org.eclipse.rdf4j.query.algebra.Exists;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupConcat;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.eclipse.rdf4j.query.algebra.IRIFunction;
import org.eclipse.rdf4j.query.algebra.If;
import org.eclipse.rdf4j.query.algebra.In;
import org.eclipse.rdf4j.query.algebra.InsertData;
import org.eclipse.rdf4j.query.algebra.Intersection;
import org.eclipse.rdf4j.query.algebra.IsBNode;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.IsNumeric;
import org.eclipse.rdf4j.query.algebra.IsResource;
import org.eclipse.rdf4j.query.algebra.IsURI;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Label;
import org.eclipse.rdf4j.query.algebra.Lang;
import org.eclipse.rdf4j.query.algebra.LangMatches;
import org.eclipse.rdf4j.query.algebra.LeftJoin;
import org.eclipse.rdf4j.query.algebra.Like;
import org.eclipse.rdf4j.query.algebra.ListMemberOperator;
import org.eclipse.rdf4j.query.algebra.Load;
import org.eclipse.rdf4j.query.algebra.LocalName;
import org.eclipse.rdf4j.query.algebra.MathExpr;
import org.eclipse.rdf4j.query.algebra.MathExpr.MathOp;
import org.eclipse.rdf4j.query.algebra.Max;
import org.eclipse.rdf4j.query.algebra.Min;
import org.eclipse.rdf4j.query.algebra.Modify;
import org.eclipse.rdf4j.query.algebra.Move;
import org.eclipse.rdf4j.query.algebra.MultiProjection;
import org.eclipse.rdf4j.query.algebra.Namespace;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.Or;
import org.eclipse.rdf4j.query.algebra.Order;
import org.eclipse.rdf4j.query.algebra.OrderElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.QueryModelVisitor;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.Reduced;
import org.eclipse.rdf4j.query.algebra.Regex;
import org.eclipse.rdf4j.query.algebra.SameTerm;
import org.eclipse.rdf4j.query.algebra.Sample;
import org.eclipse.rdf4j.query.algebra.Service;
import org.eclipse.rdf4j.query.algebra.SingletonSet;
import org.eclipse.rdf4j.query.algebra.Slice;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.StatementPattern.Scope;
import org.eclipse.rdf4j.query.algebra.Str;
import org.eclipse.rdf4j.query.algebra.Sum;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.UnaryTupleOperator;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.ZeroLengthPath;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;

final class SPARQLRenderer {

    private static final Map<String, String> NAMES;

    static {
        final Map<String, String> names = new HashMap<>();
        names.put("RAND", "RAND");
        names.put("TZ", "TZ");
        names.put("NOW", "NOW");
        names.put("UUID", "UUID");
        names.put("STRUUID", "STRUUID");
        names.put("MD5", "MD5");
        names.put("SHA1", "SHA1");
        names.put("SHA256", "SHA256");
        names.put("SHA384", "SHA384");
        names.put("SHA512", "SHA512");
        names.put("STRLANG", "STRLANG");
        names.put("STRDT", "STRDT");
        names.put(FN.STRING_LENGTH.stringValue(), "STRLEN");
        names.put(FN.SUBSTRING.stringValue(), "SUBSTR");
        names.put(FN.UPPER_CASE.stringValue(), "UCASE");
        names.put(FN.LOWER_CASE.stringValue(), "LCASE");
        names.put(FN.STARTS_WITH.stringValue(), "STRSTARTS");
        names.put(FN.ENDS_WITH.stringValue(), "STRENDS");
        names.put(FN.CONTAINS.stringValue(), "CONTAINS");
        names.put(FN.SUBSTRING_BEFORE.stringValue(), "STRBEFORE");
        names.put(FN.SUBSTRING_AFTER.stringValue(), "STRAFTER");
        names.put(FN.ENCODE_FOR_URI.stringValue(), "ENCODE_FOR_URI");
        names.put(FN.CONCAT.stringValue(), "CONCAT");
        names.put(FN.NAMESPACE + "matches", "REGEX");
        names.put(FN.REPLACE.stringValue(), "REPLACE");
        names.put(FN.NUMERIC_ABS.stringValue(), "ABS");
        names.put(FN.NUMERIC_ROUND.stringValue(), "ROUND");
        names.put(FN.NUMERIC_CEIL.stringValue(), "CEIL");
        names.put(FN.NUMERIC_FLOOR.stringValue(), "FLOOR");
        names.put(FN.YEAR_FROM_DATETIME.stringValue(), "YEAR");
        names.put(FN.MONTH_FROM_DATETIME.stringValue(), "MONTH");
        names.put(FN.DAY_FROM_DATETIME.stringValue(), "DAY");
        names.put(FN.HOURS_FROM_DATETIME.stringValue(), "HOURS");
        names.put(FN.MINUTES_FROM_DATETIME.stringValue(), "MINUTES");
        names.put(FN.SECONDS_FROM_DATETIME.stringValue(), "SECONDS");
        names.put(FN.TIMEZONE_FROM_DATETIME.stringValue(), "TIMEZONE");
        NAMES = Collections.unmodifiableMap(names);

    }

    private final Map<String, String> prefixes;

    private final boolean forceSelect;

    public SPARQLRenderer(@Nullable final Map<String, String> prefixes,
            @Nullable final boolean forceSelect) {
        this.prefixes = prefixes != null ? prefixes : Collections.<String, String>emptyMap();
        this.forceSelect = forceSelect;
    }

    public String render(final TupleExpr expr, @Nullable final Dataset dataset) {
        final Rendering rendering = new Rendering(Algebra.normalizeVars(expr), dataset, true);
        final StringBuilder builder = new StringBuilder();
        boolean newline = false;
        if (!rendering.namespaces.isEmpty()) {
            final List<String> sortedNamespaces = new ArrayList<>(rendering.namespaces);
            Collections.sort(sortedNamespaces);
            for (final String namespace : sortedNamespaces) {
                final String prefix = this.prefixes.get(namespace);
                if ("bif".equals(prefix) && "http://www.openlinksw.com/schema/sparql/extensions#" //
                        .equals(namespace)) {
                    continue; // do not emit Virtuoso bif: binding, as Virtuoso will complain
                }
                builder.append("PREFIX ").append(prefix).append(": <");
                SPARQLRenderer.escape(namespace, builder);
                builder.append(">\n");
                newline = true;
            }
        }
        if (rendering.base != null) {
            builder.append("BASE <");
            SPARQLRenderer.escape(rendering.base, builder);
            builder.append(">\n");
            newline = true;
        }
        if (newline) {
            builder.append("\n");
        }
        builder.append(rendering.body);
        return builder.toString();
    }

    public String renderTupleExpr(final TupleExpr expr) {
        return new Rendering(Algebra.normalizeVars(expr), null, false).body;
    }

    // Helper functions

    private static void escape(final String label, final StringBuilder builder) {
        final int length = label.length();
        for (int i = 0; i < length; ++i) {
            final char c = label.charAt(i);
            if (c == '\\') {
                builder.append("\\\\");
            } else if (c == '"') {
                builder.append("\\\"");
            } else if (c == '\n') {
                builder.append("\\n");
            } else if (c == '\r') {
                builder.append("\\r");
            } else if (c == '\t') {
                builder.append("\\t");
            }
            // TODO: not accepted by Virtuoso :-(
            // else if (c >= 0x0 && c <= 0x8 || c == 0xB || c == 0xC || c >= 0xE && c <= 0x1F
            // || c >= 0x7F && c <= 0xFFFF) {
            // builder.append("\\u").append(
            // Strings.padStart(Integer.toHexString(c).toUpperCase(), 4, '0'));
            // } else if (c >= 0x10000 && c <= 0x10FFFF) {
            // builder.append("\\U").append(
            // Strings.padStart(Integer.toHexString(c).toUpperCase(), 8, '0'));
            // }
            else {
                builder.append(Character.toString(c));
            }
        }
    }

    private static String sanitize(final String string) {
        final int length = string.length();
        final StringBuilder builder = new StringBuilder(length + 10);
        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            if (Character.isLetter(ch) || ch == '_' || i > 0 && Character.isDigit(ch)) {
                builder.append(ch);
            } else {
                builder.append("_");
            }
        }
        return builder.toString();
    }

    private static <T> boolean equalOrNull(@Nullable final T first, @Nullable final T second) {
        return first != null && first.equals(second) || first == null && second == null;
    }

    private static <T> T defaultIfNull(@Nullable final T value, @Nullable final T defaultValue) {
        return value != null ? value : defaultValue;
    }

    private static List<StatementPattern> getBGP(final QueryModelNode n) {
        if (n instanceof StatementPattern) {
            return Collections.singletonList((StatementPattern) n);
        } else if (!(n instanceof Join)) {
            return null;
        }
        final Join j = (Join) n;
        final List<StatementPattern> l = SPARQLRenderer.getBGP(j.getLeftArg());
        final List<StatementPattern> r = SPARQLRenderer.getBGP(j.getRightArg());
        if (l == null || r == null) {
            return null;
        }
        if (l.isEmpty()) {
            return r;
        } else if (r.isEmpty()) {
            return l;
        } else if (!SPARQLRenderer.equalOrNull(l.get(0).getContextVar(),
                r.get(0).getContextVar())) {
            return null;
        } else {
            final List<StatementPattern> s = new ArrayList<StatementPattern>(l.size() + r.size());
            s.addAll(l);
            s.addAll(r);
            return s;
        }
    }

    private static int getVarRefs(final QueryModelNode node, final String name) {
        final AtomicInteger count = new AtomicInteger(0);
        node.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(final Var var) {
                if (var.getName().equals(name)) {
                    count.set(count.get() + 1);
                }
            }

        });
        return count.get();
    }

    private static ValueExpr getVarExpr(final QueryModelNode node, final String name) {
        final AtomicReference<ValueExpr> result = new AtomicReference<ValueExpr>(null);
        node.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            protected void meetNode(final QueryModelNode node) throws RuntimeException {
                if (result.get() == null) {
                    super.meetNode(node);
                }
            }

            @Override
            public void meet(final Var var) {
                if (var.getName().equals(name) && var.getValue() != null) {
                    result.set(new ValueConstant(var.getValue()));
                }
            }

            @Override
            public void meet(final ExtensionElem node) throws RuntimeException {
                if (node.getName().equals(name)) {
                    result.set(node.getExpr());
                } else {
                    super.meet(node);
                }
            }

        });
        return result.get();
    }

    private static void check(final boolean condition, final String errorMessage) {
        if (!condition) {
            throw new IllegalArgumentException(errorMessage);
        }
    }

    private static <T> List<T> list(final Iterable<? extends T> iterable) {
        final List<T> result = new ArrayList<>();
        for (final T element : iterable) {
            result.add(element);
        }
        return result;
    }

    private final class Rendering implements QueryModelVisitor<RuntimeException> {

        final TupleExpr root;

        @Nullable
        final Dataset dataset;

        final String body;

        String base;

        private final StringBuilder builder;

        private final Set<String> namespaces;

        private int indent;

        Rendering(final TupleExpr node, @Nullable final Dataset dataset, final boolean query) {
            this.root = new QueryRoot(Objects.requireNonNull(node));
            this.dataset = dataset;
            this.builder = new StringBuilder();
            this.namespaces = new HashSet<>();
            this.indent = 0;
            if (query) {
                this.emit(Query.create(this.root, this.dataset, SPARQLRenderer.this.forceSelect));
            } else {
                this.emit(node);
            }
            this.body = this.builder.toString();
            this.builder.setLength(0);
        }

        // BASIC RENDERING METHODS (STRING, VALUES, CONDITIONALS, NEWLINE AND BRACES, ERRORS)

        private Rendering emitIf(final boolean condition, final Object object) {
            if (condition) {
                this.emit(object);
            }
            return this;
        }

        private Rendering emit(final Iterable<?> values, final String separator) {
            boolean first = true;
            for (final Object value : values) {
                if (!first) {
                    this.emit(separator);
                }
                this.emit(value);
                first = false;
            }
            return this;
        }

        @SuppressWarnings("unchecked")
        private Rendering emit(final Object value) {
            if (value instanceof String) {
                return this.emit((String) value);
            } else if (value instanceof QueryModelNode) {
                this.emit((QueryModelNode) value);
            } else if (value instanceof BNode) {
                this.emit((BNode) value);
            } else if (value instanceof IRI) {
                this.emit((IRI) value);
            } else if (value instanceof Literal) {
                this.emit((Literal) value);
            } else if (value instanceof List<?>) {
                this.emit((List<StatementPattern>) value);
            } else if (value instanceof Query) {
                this.emit((Query) value);
            }
            return this;
        }

        private Rendering emit(final String string) {
            this.builder.append(string);
            return this;
        }

        private Rendering emit(final Literal literal) {
            if (XMLSchema.INTEGER.equals(literal.getDatatype())) {
                this.builder.append(literal.getLabel());
            } else {
                this.builder.append("\"");
                SPARQLRenderer.escape(literal.getLabel(), this.builder);
                this.builder.append("\"");
                if (literal.getLanguage().isPresent()) {
                    this.builder.append("@").append(literal.getLanguage().get());
                } else if (literal.getDatatype() != null) {
                    this.builder.append("^^");
                    this.emit(literal.getDatatype());
                }
            }
            return this;
        }

        private Rendering emit(final BNode bnode) {
            this.builder.append("_:").append(bnode.getID());
            return this;
        }

        private Rendering emit(final IRI iri) {
            if (iri.getNamespace().equals("http://www.openlinksw.com/schema/sparql/extensions#")) {
                this.builder.append("bif:").append(iri.getLocalName()); // for Virtuoso builtins
            } else {
                final String prefix = SPARQLRenderer.this.prefixes.get(iri.getNamespace());
                if (prefix != null) {
                    if (this.namespaces != null) {
                        this.namespaces.add(iri.getNamespace());
                    }
                    this.builder.append(prefix).append(':').append(iri.getLocalName());
                } else {
                    this.builder.append("<");
                    SPARQLRenderer.escape(iri.toString(), this.builder);
                    this.builder.append(">");
                }
            }
            return this;
        }

        private Rendering emit(final List<StatementPattern> bgp) {
            if (bgp.isEmpty()) {
                return this;
            }
            final Var c = bgp.get(0).getContextVar();
            if (c != null) {
                this.emit("GRAPH ").emit(c).emit(" ").openBrace();
            }
            StatementPattern l = null;
            for (final StatementPattern n : bgp) {
                final Var s = n.getSubjectVar();
                final Var p = n.getPredicateVar();
                final Var o = n.getObjectVar();
                if (l == null) {
                    this.emit(s).emit(" ").emit(p).emit(" ").emit(o); // s p o
                } else if (!l.getSubjectVar().equals(n.getSubjectVar())) {
                    this.emit(" .").newline().emit(s).emit(" ").emit(p).emit(" ").emit(o); // .\n
                                                                                           // s p
                                                                                           // o
                } else if (!l.getPredicateVar().equals(n.getPredicateVar())) {
                    this.emit(" ;").newline().emit("\t").emit(p).emit(" ").emit(o); // ;\n\t p o
                } else if (!l.getObjectVar().equals(n.getObjectVar())) {
                    this.emit(" , ").emit(o); // , o
                }
                l = n;
            }
            this.emit(" .");
            if (c != null) {
                this.closeBrace();
            }
            return this;
        }

        private Rendering emit(final Query query) {
            if (query.root != this.root) {
                this.openBrace();
            }
            if (query.form == Form.ASK) {
                this.emit("ASK");
            } else if (query.form == Form.CONSTRUCT) {
                this.emit("CONSTRUCT ").openBrace().emit(query.construct).closeBrace();
            } else if (query.form == Form.DESCRIBE) {
                this.emit("DESCRIBE");
                for (final ProjectionElem p : query.select) {
                    final ExtensionElem e = p.getSourceExpression();
                    this.emit(" ").emit(
                            e != null && e.getExpr() instanceof ValueConstant ? e.getExpr() : p);
                }
            } else if (query.form == Form.SELECT) {
                this.emit("SELECT");
                if (query.modifier != null) {
                    this.emit(" ").emit(query.modifier.toString().toUpperCase());
                }
                if (query.select.isEmpty()) {
                    int count = 0;
                    for (final String var : query.where.getBindingNames()) {
                        final ValueExpr expr = SPARQLRenderer.getVarExpr(query.where, var);
                        if (!var.startsWith("-")) {
                            if (expr == null) {
                                this.emit(" ?").emit(var);
                            } else {
                                this.emit(" (").emit(expr).emit(" AS ?").emit(var).emit(")");
                            }
                            ++count;
                        }
                    }
                    if (count == 0) {
                        this.emit(" *");
                    }
                } else {
                    this.emit(" ").emit(query.select, " ");
                }
            }
            if (query.from != null) {
                for (final IRI iri : query.from.getDefaultGraphs()) {
                    this.newline().emit("FROM ").emit(iri);
                }
                for (final IRI iri : query.from.getNamedGraphs()) {
                    this.newline().emit("FROM NAMED ").emit(iri);
                }
            }
            if (query.form != Form.DESCRIBE || !(query.where instanceof SingletonSet)) {
                this.newline().emit("WHERE ").openBrace().emit(query.where).closeBrace();
            }
            if (!query.groupBy.isEmpty()) {
                this.newline().emit("GROUP BY");
                for (final ProjectionElem n : query.groupBy) {
                    this.emit(" ?").emit(n.getTargetName());
                }
            }
            if (query.having != null) {
                this.newline().emit("HAVING (").emit(query.having).emit(")");
            }
            if (!query.orderBy.isEmpty()) {
                this.newline().emit("ORDER BY ").emit(query.orderBy, " ");
            }
            if (query.form != Form.ASK) {
                if (query.offset != null) {
                    this.newline().emit("OFFSET " + query.offset);
                }
                if (query.limit != null) {
                    this.newline().emit("LIMIT " + query.limit);
                    // newline().emit("LIMIT " + (query.limit + 1)); // TODO Virtuoso fix :-(
                }
            }
            if (query.root != this.root) {
                this.closeBrace();
            }
            return this;
        }

        private Rendering emit(final QueryModelNode n) {
            final QueryModelNode p = n.getParentNode();
            final boolean braces = n instanceof TupleExpr && p != null
                    && !(p instanceof TupleExpr);
            if (braces) {
                this.openBrace();
            }
            n.visit(this);
            if (braces) {
                this.closeBrace();
            }
            return this;
        }

        private Rendering emit(final QueryModelNode node, final boolean parenthesis) { // TODO
            if (parenthesis) {
                if (node instanceof TupleExpr) {
                    this.openBrace();
                } else {
                    this.emit("(");
                }
            }
            this.emit(node);
            if (parenthesis) {
                if (node instanceof TupleExpr) {
                    this.closeBrace();
                } else {
                    this.emit(")");
                }
            }
            return this;
        }

        private Rendering openBrace() {
            this.emit("{");
            ++this.indent;
            this.newline();
            return this;
        }

        private Rendering closeBrace() {
            --this.indent;
            this.newline();
            this.emit("}");
            return this;
        }

        private Rendering newline() {
            this.emit("\n");
            for (int i = 0; i < this.indent; ++i) {
                this.emit("\t");
            }
            return this;
        }

        private Rendering fail(final String message, final QueryModelNode node) {
            throw new IllegalArgumentException("SPARQL rendering failed. " + message
                    + (node == null ? "null" : node.getClass().getSimpleName() + "\n" + node));
        }

        // TupleExpr: root query nodes

        @Override
        public void meet(final OrderElem n) {
            this.emit(n.isAscending() ? "ASC(" : "DESC(").emit(n.getExpr()).emit(")");
        }

        @Override
        public void meet(final ProjectionElemList node) {
            this.emit(node.getElements(), " ");
        }

        @Override
        public void meet(final ProjectionElem n) {

            final String source = n.getSourceName();
            final String target = n.getTargetName();
            final ValueExpr expr = n.getSourceExpression() == null ? null
                    : n.getSourceExpression().getExpr();

            if (target.startsWith("-")) {
                if (expr != null) {
                    this.emit("(").emit(expr).emit(" AS ?").emit(SPARQLRenderer.sanitize(target))
                            .emit(")");
                }
            } else if (expr != null) {
                this.emit("(").emit(expr).emit(" AS ?").emit(target).emit(")");
            } else if (!SPARQLRenderer.equalOrNull(source, target)) {
                this.emit("(?").emit(source).emit(" AS ?").emit(target).emit(")");
            } else {
                this.emit("?").emit(target);
            }
        }

        @Override
        public void meet(final GroupElem n) {
            final ProjectionElem e = new ProjectionElem();
            e.setTargetName(n.getName());
            e.setSourceName(n.getName());
            e.setSourceExpression(new ExtensionElem(n.getOperator(), n.getName()));
            this.meet(e);
        }

        @Override
        public void meet(final DescribeOperator n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final QueryRoot n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Projection n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final MultiProjection n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Distinct n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Reduced n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Group n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Order n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        @Override
        public void meet(final Slice n) {
            this.emit(Query.create(n, null, SPARQLRenderer.this.forceSelect));
        }

        // TupleExpr: leaf nodes

        @Override
        public void meet(final EmptySet n) {
            final QueryModelNode p = n.getParentNode();
            if (p.getParentNode() != null && !(p.getParentNode() instanceof QueryRoot)) {
                throw new IllegalArgumentException(
                        "Cannot translate EmptySet inside the body of a query / update operation");
            }
            this.emit("CONSTRUCT {} WHERE {}");
        }

        @Override
        public void meet(final SingletonSet n) {
            // nothing to do: braces, if necessary, are emitted by parent
        }

        @Override
        public void meet(final BindingSetAssignment n) {

            final Set<String> names = n.getBindingNames();

            if (names.isEmpty()) {
                this.emit("VALUES {}");

            } else if (names.size() == 1) {
                final String name = names.iterator().next();
                this.emit("VALUES ?").emit(name).emit(" ").openBrace();
                boolean first = true;
                for (final BindingSet bindings : n.getBindingSets()) {
                    this.emitIf(!first, " ")
                            .emit(SPARQLRenderer.defaultIfNull(bindings.getValue(name), "UNDEF"));
                    first = false;
                }
                this.closeBrace();

            } else {
                this.emit("VALUES (?").emit(names, " ?").emit(") ").openBrace();
                boolean firstBinding = true;
                for (final BindingSet bindings : n.getBindingSets()) {
                    if (!firstBinding) {
                        this.newline();
                    }
                    this.emit("(");
                    boolean first = true;
                    for (final String name : names) {
                        this.emitIf(!first, " ").emit(
                                SPARQLRenderer.defaultIfNull(bindings.getValue(name), "UNDEF"));
                        first = false;
                    }
                    this.emit(")");
                    firstBinding = false;
                }
                this.closeBrace();
            }
        }

        @Override
        public void meet(final StatementPattern n) {
            this.emit(SPARQLRenderer.getBGP(n));
        }

        // TupleExpr: unary

        @Override
        public void meet(final Extension n) {
            this.emit(n.getArg());
            if (!(n.getArg() instanceof SingletonSet)) {
                this.newline();
            }
            boolean first = true;
            for (final ExtensionElem e : n.getElements()) {
                final ValueExpr expr = e.getExpr();
                if (!(expr instanceof Var) || !((Var) expr).getName().equals(e.getName())) {
                    if (!first) {
                        this.newline();
                    }
                    this.emit("BIND (").emit(expr).emit(" AS ?").emit(e.getName()).emit(")");
                    first = false;
                }
            }
        }

        @Override
        public void meet(final ExtensionElem n) {
            throw new Error("Should not be directly called");
        }

        @Override
        public void meet(final Filter n) {
            final ValueExpr cond = n.getCondition();
            final boolean nopar = cond instanceof Exists
                    || cond instanceof Not && ((Not) cond).getArg() instanceof Exists;
            this.emit(n.getArg());
            if (!(n.getArg() instanceof SingletonSet)) {
                this.newline();
            }
            this.emit("FILTER ").emit(cond, !nopar);
        }

        @Override
        public void meet(final Service n) {
            this.newline().emit("SERVICE ").emitIf(n.isSilent(), "SILENT ").openBrace()
                    .emit(n.getServiceExpr()).closeBrace().emit(" ").emit(n.getServiceRef());
        }

        // TupleExpr: binary

        @Override
        public void meet(final Join n) {
            final List<StatementPattern> bgp = SPARQLRenderer.getBGP(n);
            if (bgp != null) {
                this.emit(bgp);
            } else {
                final TupleExpr l = n.getLeftArg();
                final TupleExpr r = n.getRightArg();
                final boolean norpar = r instanceof Join || r instanceof StatementPattern
                        || r instanceof SingletonSet || r instanceof Service || r instanceof Union
                        || r instanceof BindingSetAssignment || r instanceof ArbitraryLengthPath;
                this.emit(l).newline().emit(r, !norpar);
            }
        }

        @Override
        public void meet(final LeftJoin n) {
            final TupleExpr l = n.getLeftArg();
            final TupleExpr r = n.getCondition() == null ? n.getRightArg() : //
                    new Filter(n.getRightArg(), n.getCondition());
            this.emit(l);
            if (!(l instanceof SingletonSet)) {
                this.newline();
            }
            this.emit("OPTIONAL ").emit(r, true);
        }

        @Override
        public void meet(final Union n) {
            final TupleExpr l = n.getLeftArg();
            final TupleExpr r = n.getRightArg();
            final ZeroLengthPath p = l instanceof ZeroLengthPath ? (ZeroLengthPath) l
                    : r instanceof ZeroLengthPath ? (ZeroLengthPath) r : null;
            if (p == null) {
                this.emit(l, !(l instanceof Union)).emit(" UNION ").emit(r, !(r instanceof Union));
            } else {
                final Var s = p.getSubjectVar();
                final Var o = p.getObjectVar();
                final Var c = p.getContextVar();
                if (c != null) {
                    this.emit("GRAPH ").emit(c).emit(" ").openBrace();
                }
                this.emit(s).emit(" ").emitPropertyPath(n, s, o).emit(" ").emit(o);
                if (c != null) {
                    this.closeBrace();
                }
            }
        }

        @Override
        public void meet(final Difference n) {
            final TupleExpr l = n.getLeftArg();
            final TupleExpr r = n.getRightArg();
            this.emit(l, true).emit(" MINUS ").emit(r, true);
        }

        // TupleExpr: paths

        @Override
        public void meet(final ArbitraryLengthPath n) {
            final Var s = n.getSubjectVar();
            final Var o = n.getObjectVar();
            final Var c = n.getContextVar();
            if (c != null) {
                this.emit("GRAPH ").emit(c).openBrace();
            }
            this.emit(s).emit(" ").emitPropertyPath(n, s, o).emit(" ").emit(o).emit(" .");
            if (c != null) {
                this.closeBrace();
            }
        }

        @Override
        public void meet(final ZeroLengthPath node) {
            throw new Error("Should not be directly called");
        }

        private Rendering emitPropertyPath(final TupleExpr node, final Var start, final Var end) {

            // Note: elt1 / elt2 and ^(complex exp) do not occur in RDF4J algebra

            final boolean parenthesis = !(node instanceof StatementPattern)
                    && (node.getParentNode() instanceof ArbitraryLengthPath
                            || node.getParentNode() instanceof Union);

            this.emitIf(parenthesis, "(");

            if (node instanceof StatementPattern) {
                // handles iri, ^iri
                final StatementPattern pattern = (StatementPattern) node;
                final boolean inverse = this.isInversePath(pattern, start, end);
                if (!pattern.getPredicateVar().hasValue()
                        || !pattern.getPredicateVar().isAnonymous()) {
                    this.fail("Unsupported path expression. Check node: ", node);
                }
                this.emitIf(inverse, "^").emit(pattern.getPredicateVar().getValue());

            } else if (node instanceof Join) {
                final Join j = (Join) node;
                final TupleExpr l = j.getLeftArg();
                final TupleExpr r = j.getRightArg();
                final StatementPattern s = l instanceof StatementPattern ? (StatementPattern) l
                        : r instanceof StatementPattern ? (StatementPattern) r : null;
                if (s == null) {
                    return this.fail("Cannot process property path", node);
                }
                final Var m = s.getSubjectVar().equals(start) || s.getSubjectVar().equals(end)
                        ? s.getObjectVar() : s.getSubjectVar();
                this.emitPropertyPath(l, start, m);
                this.emit("/");
                this.emitPropertyPath(r, m, end);

            } else if (node instanceof ArbitraryLengthPath) {
                // handles elt*, elt+
                final ArbitraryLengthPath path = (ArbitraryLengthPath) node;
                SPARQLRenderer.check(path.getMinLength() <= 1, "Invalid path length");
                this.emitPropertyPath(path.getPathExpression(), start, end)
                        .emit(path.getMinLength() == 0 ? "*" : "+");

            } else if (node instanceof Union) {
                // handles elt?, elt1|elt2|...
                final Union union = (Union) node;
                if (union.getLeftArg() instanceof ZeroLengthPath) {
                    this.emitPropertyPath(union.getRightArg(), start, end).emit("?");
                } else if (union.getRightArg() instanceof ZeroLengthPath) {
                    this.emitPropertyPath(union.getLeftArg(), start, end).emit("?");
                } else {
                    this.emitPropertyPath(union.getLeftArg(), start, end);
                    this.emit("|");
                    this.emitPropertyPath(union.getRightArg(), start, end);
                }

            } else if (node instanceof Filter) {
                // handles !iri, !(iri1,iri2,...) with possibly inverse properties
                final Filter filter = (Filter) node;

                SPARQLRenderer.check(filter.getArg() instanceof StatementPattern,
                        "Invalid path expression");
                final StatementPattern pattern = (StatementPattern) filter.getArg();
                final boolean inverse = this.isInversePath(pattern, start, end);
                SPARQLRenderer.check(
                        !pattern.getPredicateVar().hasValue()
                                && pattern.getPredicateVar().isAnonymous(),
                        "Invalid path expression");

                final Set<IRI> negatedProperties = new LinkedHashSet<>();
                this.extractNegatedProperties(filter.getCondition(), negatedProperties);

                if (negatedProperties.size() == 1) {
                    this.emit("!").emitIf(inverse, "^").emit(negatedProperties.iterator().next());

                } else {
                    this.emit("!(");
                    boolean first = true;
                    for (final IRI negatedProperty : negatedProperties) {
                        this.emitIf(!first, "|").emitIf(inverse, "^").emit(negatedProperty);
                        first = false;
                    }
                    this.emit(")");
                }

            } else {
                this.fail("Unsupported path expression node", node);
            }

            return this.emitIf(parenthesis, ")");
        }

        private void extractNegatedProperties(final ValueExpr condition,
                final Set<IRI> negatedProperties) {
            if (condition instanceof And) {
                final And and = (And) condition;
                this.extractNegatedProperties(and.getLeftArg(), negatedProperties);
                this.extractNegatedProperties(and.getRightArg(), negatedProperties);

            } else if (condition instanceof Compare) {
                final Compare compare = (Compare) condition;
                SPARQLRenderer.check(compare.getOperator() == CompareOp.NE,
                        "Invalid path expression");
                if (compare.getLeftArg() instanceof ValueConstant) {
                    SPARQLRenderer.check(compare.getRightArg() instanceof Var,
                            "Invalid path expression");
                    negatedProperties.add((IRI) ((ValueConstant) compare.getLeftArg()).getValue());
                } else if (compare.getRightArg() instanceof ValueConstant) {
                    SPARQLRenderer.check(compare.getLeftArg() instanceof Var,
                            "Invalid path expression");
                    negatedProperties
                            .add((IRI) ((ValueConstant) compare.getRightArg()).getValue());
                } else {
                    this.fail("Unsupported path expression. Check condition node: ", condition);
                }
            }
        }

        private boolean isInversePath(final StatementPattern node, final Var start,
                final Var end) {
            if (node.getSubjectVar().equals(start)) {
                SPARQLRenderer.check(node.getObjectVar().equals(end), "Invalid path expression");
                return false;
            } else if (node.getObjectVar().equals(start)) {
                SPARQLRenderer.check(node.getSubjectVar().equals(end), "Invalid path expression");
                return true;
            } else {
                this.fail("Unsupported path expression. Check node: ", node);
                return false;
            }
        }

        // TupleExpr: unsupported

        @Override
        public void meet(final Intersection n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        // === SPARQL UPDATE ===

        @Override
        public void meet(final Add add) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Clear clear) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Copy copy) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Create create) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final DeleteData deleteData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final InsertData insertData) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Load load) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Modify modify) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void meet(final Move move) {
            throw new UnsupportedOperationException();
        }

        // === VALUE EXPR ===

        // ValueExpr: variables and constants

        @Override
        public void meet(final ValueConstant n) {
            this.emit(n.getValue());
        }

        @Override
        public void meet(final Var n) {
            final String name = n.getName();
            if (n.getValue() != null) {
                this.emit(n.getValue());
            } else if (!n.isAnonymous()) {
                this.emit("?" + n.getName());
            } else {
                final ValueExpr expr = SPARQLRenderer.getVarExpr(this.root, n.getName());
                if (expr != null) {
                    this.emit(expr);
                } else if (SPARQLRenderer.getVarRefs(this.root, n.getName()) <= 1) {
                    this.emit("[]");
                } else {
                    this.emit("?").emit(SPARQLRenderer.sanitize(name));
                }
            }
        }

        // ValueExpr: comparison, math and boolean operators

        @Override
        public void meet(final Compare n) {
            final QueryModelNode p = n.getParentNode();
            final boolean par = p instanceof Not || p instanceof MathExpr;
            this.emitIf(par, "(").emit(n.getLeftArg()).emit(" ").emit(n.getOperator().getSymbol())
                    .emit(" ").emit(n.getRightArg()).emitIf(par, ")");
        }

        @Override
        public void meet(final ListMemberOperator n) {
            final QueryModelNode p = n.getParentNode();
            final boolean par = p instanceof Not || p instanceof MathExpr;
            final List<ValueExpr> args = n.getArguments();
            this.emitIf(par, "(").emit(args.get(0)).emit(" in (")
                    .emit(args.subList(1, args.size()), ", ").emit(")").emitIf(par, ")");
        }

        @Override
        public void meet(final MathExpr n) {
            final QueryModelNode p = n.getParentNode();
            final MathOp op = n.getOperator();
            final MathOp pop = p instanceof MathExpr ? ((MathExpr) p).getOperator() : null;
            final boolean r = p instanceof BinaryValueOperator
                    && n == ((BinaryValueOperator) p).getRightArg();
            final boolean par = p instanceof Not //
                    || (op == MathOp.PLUS || op == MathOp.MINUS) && (pop == MathOp.MINUS && r //
                            || pop == MathOp.DIVIDE || pop == MathOp.MULTIPLY)
                    || (op == MathOp.MULTIPLY || op == MathOp.DIVIDE) && pop == MathOp.DIVIDE && r;
            this.emitIf(par, "(").emit(n.getLeftArg()).emit(" ").emit(op.getSymbol()).emit(" ")
                    .emit(n.getRightArg()).emitIf(par, ")");
        }

        @Override
        public void meet(final And n) {
            final QueryModelNode p = n.getParentNode();
            final boolean needPar = p instanceof Not || p instanceof MathExpr
                    || p instanceof ListMemberOperator || p instanceof Compare;
            this.emitIf(needPar, "(").emit(n.getLeftArg()).emit(" && ").emit(n.getRightArg())
                    .emitIf(needPar, ")");
        }

        @Override
        public void meet(final Or n) {
            final QueryModelNode p = n.getParentNode();
            final boolean needPar = p instanceof Not || p instanceof And || p instanceof MathExpr
                    || p instanceof ListMemberOperator || p instanceof Compare;
            this.emitIf(needPar, "(").emit(n.getLeftArg()).emit(" || ").emit(n.getRightArg())
                    .emitIf(needPar, ")");
        }

        @Override
        public void meet(final Not n) {
            final String op = n.getArg() instanceof Exists ? "NOT " : "!";
            this.emit(op).emit(n.getArg());
        }

        // ValueExpr: aggregates

        @Override
        public void meet(final Count node) {
            this.emit("COUNT(").emitIf(node.isDistinct(), "DISTINCT ")
                    .emit(SPARQLRenderer.defaultIfNull(node.getArg(), "*")).emit(")");
        }

        @Override
        public void meet(final Sum node) {
            this.emit("SUM(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Min node) {
            this.emit("MIN(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Max node) {
            this.emit("MAX(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Avg node) {
            this.emit("AVG(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg()).emit(")");
        }

        @Override
        public void meet(final Sample node) {
            this.emit("SAMPLE(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg())
                    .emit(")");
        }

        @Override
        public void meet(final GroupConcat node) {
            this.emit("GROUP_CONCAT(").emitIf(node.isDistinct(), "DISTINCT ").emit(node.getArg());
            if (node.getSeparator() != null) {
                this.emit(" ; separator=").emit(node.getSeparator());
            }
            this.emit(")");
        }

        // ValueExpr: function calls

        @Override
        public void meet(final Str n) {
            this.emit("STR(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final Lang n) {
            this.emit("LANG(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final LangMatches n) {
            this.emit("LANGMATCHES(").emit(n.getLeftArg()).emit(", ").emit(n.getRightArg())
                    .emit(")");
        }

        @Override
        public void meet(final Datatype n) {
            this.emit("DATATYPE(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final Bound n) {
            this.emit("BOUND(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IRIFunction n) {
            this.emit("IRI(").emit(n.getArg()).emit(")");
            if (n.getBaseURI() != null) {
                this.base = n.getBaseURI();
            }
        }

        @Override
        public void meet(final BNodeGenerator n) {
            final ValueExpr expr = n.getNodeIdExpr();
            this.emit("BNODE(").emitIf(expr != null, expr).emit(")");
        }

        @Override
        public void meet(final FunctionCall n) {
            final String iri = n.getURI();
            String name = SPARQLRenderer.NAMES.get(iri);
            if (name == null && SPARQLRenderer.NAMES.values().contains(iri.toUpperCase())) {
                name = n.getURI().toUpperCase();
            }
            this.emit(name != null ? name : SimpleValueFactory.getInstance().createIRI(iri))
                    .emit("(").emit(n.getArgs(), ", ").emit(")");
        }

        @Override
        public void meet(final Coalesce n) {
            this.emit("COALESCE(").emit(n.getArguments(), ", ").emit(")");
        }

        @Override
        public void meet(final If n) {
            this.emit("IF(").emit(n.getCondition()).emit(", ").emit(n.getResult()).emit(", ")
                    .emit(n.getAlternative()).emit(")");
        }

        @Override
        public void meet(final SameTerm n) {
            this.emit("sameTerm(").emit(n.getLeftArg()).emit(", ").emit(n.getRightArg()).emit(")");
        }

        @Override
        public void meet(final IsURI n) {
            this.emit("isIRI(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IsBNode n) {
            this.emit("isBLANK(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IsLiteral n) {
            this.emit("isLITERAL(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final IsNumeric n) {
            this.emit("isNUMERIC(").emit(n.getArg()).emit(")");
        }

        @Override
        public void meet(final Regex n) {
            this.emit("REGEX(").emit(n.getArg()).emit(", ").emit(n.getPatternArg());
            if (n.getFlagsArg() != null) {
                this.emit(", ").emit(n.getFlagsArg());
            }
            this.emit(")");
        }

        @Override
        public void meet(final Exists node) {
            this.emit("EXISTS ").emit(node.getSubQuery());
        }

        // ValueExpr: unsupported nodes

        @Override
        public void meet(final IsResource n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final Label n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final Like n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final LocalName n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final Namespace n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final In n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final CompareAll n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        @Override
        public void meet(final CompareAny n) {
            this.fail("Not a SPARQL 1.1 node", n);
        }

        // OTHER

        @Override
        public void meetOther(final QueryModelNode n) {
            this.fail("Unknown node", n);
        }

    }

    private enum Form {
        SELECT, CONSTRUCT, ASK, DESCRIBE
    }

    private enum Modifier {
        DISTINCT, REDUCED
    }

    private static final class Query {

        final QueryModelNode root;

        final Form form;

        @Nullable
        final Modifier modifier;

        final List<ProjectionElem> select;

        @Nullable
        final TupleExpr construct;

        @Nullable
        final Dataset from;

        final TupleExpr where;

        final List<ProjectionElem> groupBy;

        @Nullable
        final ValueExpr having;

        final List<OrderElem> orderBy;

        @Nullable
        final Long offset;

        @Nullable
        final Long limit;

        static Query create(final TupleExpr rootNode, @Nullable final Dataset dataset,
                final boolean forceSelect) {

            Objects.requireNonNull(rootNode);

            // Handle special trivial case
            if (rootNode instanceof EmptySet) {
                return new Query(rootNode, Form.CONSTRUCT, null, null, rootNode, dataset, rootNode,
                        null, null, null, null, null);
            }

            // Local variables
            Form form = null;
            Modifier modifier = null;
            final List<ProjectionElem> select = new ArrayList<>();
            TupleExpr construct = null;
            TupleExpr where = null;
            final List<ProjectionElem> groupBy = new ArrayList<>();
            ValueExpr having = null;
            final List<OrderElem> orderBy = new ArrayList<>();
            Long offset = null;
            Long limit = null;

            final List<UnaryTupleOperator> nodes = Query.extractQueryNodes(rootNode, false);

            where = nodes.size() > 0 ? nodes.get(nodes.size() - 1).getArg() : rootNode;

            for (final UnaryTupleOperator node : nodes) {

                if (node instanceof DescribeOperator) {
                    form = Form.DESCRIBE;

                } else if (node instanceof Distinct) {
                    modifier = Modifier.DISTINCT;

                } else if (node instanceof Reduced) {
                    modifier = Modifier.REDUCED;

                } else if (node instanceof Projection) {
                    final Map<String, ExtensionElem> extensions = Query.extractExtensions(node);
                    final List<ProjectionElem> projections = ((Projection) node)
                            .getProjectionElemList().getElements();
                    final boolean isConstruct = projections.size() >= 3
                            && "subject".equals(projections.get(0).getTargetName())
                            && "predicate".equals(projections.get(1).getTargetName())
                            && "object".equals(projections.get(2).getTargetName())
                            && (projections.size() == 3 || projections.size() == 4
                                    && "context".equals(projections.get(3).getTargetName()));
                    if (isConstruct && !forceSelect) {
                        form = Form.CONSTRUCT;
                        construct = Query.extractConstructExpression(extensions,
                                Collections.singleton(((Projection) node) //
                                        .getProjectionElemList()));
                    } else {
                        form = form == null ? Form.SELECT : form;
                        for (final ProjectionElem projection : projections) {
                            final String variable = projection.getTargetName();
                            ExtensionElem extension = extensions.get(variable);
                            if (extension == null && projection.getSourceName() != null) {
                                extension = extensions.get(projection.getSourceName());
                            }
                            final ProjectionElem newProjection = new ProjectionElem();
                            newProjection.setTargetName(variable);
                            newProjection.setSourceExpression(extension);
                            newProjection.setSourceName(
                                    extension == null || !(extension.getExpr() instanceof Var)
                                            ? projection.getSourceName()
                                            : ((Var) extension.getExpr()).getName());
                            select.add(newProjection);
                        }
                    }

                } else if (node instanceof MultiProjection) {
                    form = Form.CONSTRUCT;
                    construct = Query.extractConstructExpression(Query.extractExtensions(node),
                            ((MultiProjection) node).getProjections());

                } else if (node instanceof Group) {
                    final Group group = (Group) node;
                    final Map<String, ExtensionElem> extensions = Query
                            .extractExtensions(group.getArg());
                    for (final String variableName : group.getGroupBindingNames()) {
                        final ExtensionElem extension = extensions.get(variableName);
                        final ProjectionElem projection = new ProjectionElem();
                        projection.setTargetName(variableName);
                        projection.setSourceExpression(extension);
                        projection.setSourceName(
                                extension == null || !(extension.getExpr() instanceof Var)
                                        ? variableName
                                        : ((Var) extension.getExpr()).getName());
                        groupBy.add(projection);
                    }

                } else if (node instanceof Order) {
                    orderBy.addAll(((Order) node).getElements());

                } else if (node instanceof Slice) {
                    final Slice slice = (Slice) node;
                    offset = slice.getOffset() < 0 ? null : slice.getOffset();
                    limit = slice.getLimit() < 0 ? null : slice.getLimit();
                    if (form == null && slice.getOffset() == 0 && slice.getLimit() == 1) {
                        if (forceSelect) {
                            form = Form.SELECT;
                            limit = 1L;
                            // limit = 2L; // TODO: workaround for Virtuoso
                        } else {
                            form = Form.ASK;
                        }
                    }

                } else if (node instanceof Filter) {
                    having = ((Filter) node).getCondition();
                }
            }

            form = SPARQLRenderer.defaultIfNull(form, Form.SELECT);
            if (form == Form.CONSTRUCT && construct == null) {
                construct = new SingletonSet();
            }

            return new Query(rootNode, form, modifier, select, construct, dataset, where, groupBy,
                    having, orderBy, offset, limit);
        }

        private static List<UnaryTupleOperator> extractQueryNodes(final TupleExpr rootNode,
                final boolean haltOnGroup) {
            final List<UnaryTupleOperator> nodes = new ArrayList<>();

            TupleExpr queryNode = rootNode;
            while (queryNode instanceof UnaryTupleOperator) {
                nodes.add((UnaryTupleOperator) queryNode);
                queryNode = ((UnaryTupleOperator) queryNode).getArg();
            }

            boolean describeFound = false;
            boolean modifierFound = false;
            boolean projectionFound = false;
            boolean groupFound = false;
            boolean orderFound = false;
            boolean sliceFound = false;
            boolean extensionFound = false;

            int index = 0;
            while (index < nodes.size()) {
                final UnaryTupleOperator node = nodes.get(index);
                if (node instanceof DescribeOperator && !describeFound) {
                    describeFound = true;

                } else if ((node instanceof Distinct || node instanceof Reduced) && !modifierFound
                        && !projectionFound) {
                    modifierFound = true;

                } else if ((node instanceof Projection || node instanceof MultiProjection)
                        && !projectionFound) {
                    projectionFound = true;

                } else if (node instanceof Group && !groupFound && !haltOnGroup) {
                    groupFound = true;

                } else if (node instanceof Order && !orderFound) {
                    orderFound = true;

                } else if (node instanceof Slice && !sliceFound) {
                    sliceFound = true;

                } else if (node instanceof Filter && !groupFound && !haltOnGroup) {
                    int i = index + 1;
                    for (; i < nodes.size() && nodes.get(i) instanceof Extension;) {
                        ++i;
                    }
                    if (i < nodes.size() && nodes.get(i) instanceof Group) {
                        groupFound = true;
                        index = i;
                    } else {
                        break;
                    }

                } else if (node instanceof Extension && !extensionFound) {
                    extensionFound = true;

                } else if (!(node instanceof QueryRoot) || index > 0) {
                    break;
                }
                ++index;
            }

            return nodes.subList(0, index);
        }

        private static Map<String, ExtensionElem> extractExtensions(final TupleExpr rootNode) {
            final Map<String, ExtensionElem> map = new HashMap<>();
            for (final UnaryTupleOperator node : Query.extractQueryNodes(rootNode, true)) {
                if (node instanceof Extension) {
                    for (final ExtensionElem elem : ((Extension) node).getElements()) {
                        final String variable = elem.getName();
                        final ValueExpr expression = elem.getExpr();
                        if (!(expression instanceof Var)
                                || !((Var) expression).getName().equals(variable)) {
                            map.put(variable, elem);
                        }
                    }
                }
            }
            return map;
        }

        private static TupleExpr extractConstructExpression(
                final Map<String, ExtensionElem> extensions,
                final Iterable<? extends ProjectionElemList> multiProjections) {
            TupleExpr expression = null;
            for (final ProjectionElemList projections : multiProjections) {
                final Var subj = Query.extractConstructVar(extensions,
                        projections.getElements().get(0));
                final Var pred = Query.extractConstructVar(extensions,
                        projections.getElements().get(1));
                final Var obj = Query.extractConstructVar(extensions,
                        projections.getElements().get(2));
                final Var ctx = projections.getElements().size() < 4 ? null
                        : Query.extractConstructVar(extensions, projections.getElements().get(3));
                final StatementPattern pattern = new StatementPattern(
                        ctx == null ? Scope.DEFAULT_CONTEXTS : Scope.NAMED_CONTEXTS, subj, pred,
                        obj, ctx);
                expression = expression == null ? pattern : new Join(expression, pattern);
            }
            return expression;
        }

        private static Var extractConstructVar(final Map<String, ExtensionElem> extensions,
                final ProjectionElem projection) {
            final ExtensionElem extension = extensions.get(projection.getSourceName());
            String name = projection.getSourceName();
            if (name.startsWith("-anon-")) {
                name += "-construct";
            }
            if (extension == null || extension.getExpr() instanceof BNodeGenerator) {
                final Var var = new Var(name);
                var.setAnonymous(name.startsWith("-anon-"));
                return var;
            } else if (extension.getExpr() instanceof ValueConstant) {
                final ValueConstant constant = (ValueConstant) extension.getExpr();
                return new Var(name, constant.getValue());
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported extension in construct query: " + extension);
            }
        }

        private Query(//
                final QueryModelNode root, //
                final Form form, //
                @Nullable final Modifier modifier, //
                @Nullable final Iterable<? extends ProjectionElem> selectList, //
                @Nullable final TupleExpr construct, //
                @Nullable final Dataset from, //
                final TupleExpr where, //
                @Nullable final Iterable<? extends ProjectionElem> groupBy, //
                @Nullable final ValueExpr having, //
                @Nullable final Iterable<? extends OrderElem> orderBy, //
                @Nullable final Long offset, //
                @Nullable final Long limit) {

            this.root = Objects.requireNonNull(root);
            this.form = Objects.requireNonNull(form);
            this.modifier = modifier;
            this.select = selectList == null ? Collections.emptyList()
                    : SPARQLRenderer.list(selectList);
            this.construct = construct;
            this.from = from;
            this.where = Objects.requireNonNull(where);
            this.groupBy = groupBy == null ? Collections.emptyList()
                    : SPARQLRenderer.list(groupBy);
            this.having = having;
            this.orderBy = orderBy == null ? Collections.emptyList()
                    : SPARQLRenderer.list(orderBy);
            this.offset = offset;
            this.limit = limit;
        }

    }

}
