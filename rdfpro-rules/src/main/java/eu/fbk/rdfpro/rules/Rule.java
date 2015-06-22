package eu.fbk.rdfpro.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Model;
import org.openrdf.model.Namespace;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.queryrender.sparql.SparqlTupleExprRenderer;

import eu.fbk.rdfpro.rules.util.Algebra;
import eu.fbk.rdfpro.rules.util.SPARQLRenderer;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

public final class Rule {

    private final URI id;

    @Nullable
    private final TupleExpr head;

    @Nullable
    private final TupleExpr body;

    public Rule(final URI id, @Nullable final TupleExpr head, @Nullable final TupleExpr body) {
        this.id = id;
        this.head = head;
        this.body = body;
    }

    public URI getID() {
        return this.id;
    }

    public TupleExpr getHead() {
        return this.head;
    }

    public TupleExpr getBody() {
        return this.body;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Rule)) {
            return false;
        }
        final Rule other = (Rule) object;
        return this.id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public String toString() {
        try {
            String head = "<empty>";
            String body = "<empty>";
            if (this.head != null) {
                head = new SPARQLRenderer(Namespaces.DEFAULT.prefixMap(), false)
                        .renderTupleExpr(this.head);
            }
            if (this.body != null) {
                body = new SPARQLRenderer(Namespaces.DEFAULT.prefixMap(), false)
                        .renderTupleExpr(this.body);
            }
            final String id = this.id instanceof BNode ? ((BNode) this.id).getID() : this.id
                    .getLocalName();
            return id + ": " + head.replaceAll("[\n\r\t ]+", " ") + " :- "
                    + body.replaceAll("[\n\r\t ]+", " ");
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public <T extends Collection<? super Statement>> T toRDF(final T output) {

        // Emit statements ID a rr:Rule; rr:head HEAD; rr:body BODY.
        final ValueFactory vf = Statements.VALUE_FACTORY;
        output.add(vf.createStatement(this.id, RDF.TYPE, RR.RULE));
        try {
            if (this.head != null) {
                output.add(vf.createStatement(this.id, RR.HEAD,
                        vf.createLiteral(new SparqlTupleExprRenderer().render(this.head))));
            }
            if (this.body != null) {
                output.add(vf.createStatement(this.id, RR.BODY,
                        vf.createLiteral(new SparqlTupleExprRenderer().render(this.body))));
            }
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
        return output;
    }

    public static List<Rule> fromRDF(final Iterable<Statement> model) {

        // Load namespaces from model metadata, reusing default prefix/ns mappings
        final Map<String, String> namespaces = new HashMap<>(Namespaces.DEFAULT.uriMap());
        if (model instanceof Model) {
            for (final Namespace namespace : ((Model) model).getNamespaces()) {
                namespaces.put(namespace.getPrefix(), namespace.getName());
            }
        }
        for (final Statement stmt : model) {
            if (stmt.getSubject() instanceof URI && stmt.getObject() instanceof Literal
                    && stmt.getPredicate().equals(RR.PREFIX_PROPERTY)) {
                namespaces.put(stmt.getObject().stringValue(), stmt.getSubject().stringValue());
            }
        }

        // Extract ids, heads and bodies from ruleset data
        final Set<URI> ids = new TreeSet<>(Statements.valueComparator());
        final Map<URI, TupleExpr> heads = new HashMap<>();
        final Map<URI, TupleExpr> bodies = new HashMap<>();
        for (final Statement stmt : model) {
            try {
                if (stmt.getSubject() instanceof URI) {
                    final URI pred = stmt.getPredicate();
                    final Map<URI, TupleExpr> map = pred.equals(RR.HEAD) ? heads : pred
                            .equals(RR.BODY) ? bodies : null;
                    if (map != null) {
                        ids.add((URI) stmt.getSubject());
                        map.put((URI) stmt.getSubject(), Algebra.parseTupleExpr(stmt.getObject()
                                .stringValue(), null, namespaces));
                    }
                }
            } catch (final Throwable ex) {
                throw new IllegalArgumentException(
                        "Could not parse rule head/body from statement: " + stmt, ex);
            }
        }

        // Generate the rules from parsed heads and bodies
        final List<Rule> rules = new ArrayList<>();
        for (final URI id : ids) {
            final TupleExpr head = heads.get(id);
            final TupleExpr body = bodies.get(id);
            rules.add(new Rule(id, head, body));
        }

        return rules;
    }

}
