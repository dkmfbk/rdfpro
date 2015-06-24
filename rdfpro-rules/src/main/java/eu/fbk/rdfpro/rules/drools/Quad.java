package eu.fbk.rdfpro.rules.drools;

import org.kie.api.definition.type.Position;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import eu.fbk.rdfpro.util.Statements;

public final class Quad {

    @Position(0)
    private final int subjectID;

    @Position(1)
    private final int predicateID;

    @Position(2)
    private final int objectID;

    @Position(3)
    private final int contextID;

    public Quad(final int subjectID, final int predicateID, final int objectID,
            final int contextID) {
        this.subjectID = subjectID;
        this.predicateID = predicateID;
        this.objectID = objectID;
        this.contextID = contextID;
    }

    public int getSubjectID() {
        return this.subjectID;
    }

    public int getPredicateID() {
        return this.predicateID;
    }

    public int getObjectID() {
        return this.objectID;
    }

    public int getContextID() {
        return this.contextID;
    }

    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Quad)) {
            return false;
        }
        final Quad other = (Quad) object;
        return this.subjectID == other.subjectID && this.predicateID == other.predicateID
                && this.objectID == other.objectID && this.contextID == other.contextID;
    }

    @Override
    public int hashCode() {
        return 7829 * this.subjectID + 1103 * this.predicateID + 137 * this.objectID
                + this.contextID;
    }

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append('(');
        builder.append(this.subjectID);
        builder.append(", ");
        builder.append(this.predicateID);
        builder.append(", ");
        builder.append(this.objectID);
        builder.append(", ");
        builder.append(this.contextID);
        builder.append(')');
        return builder.toString();
    }

    public Statement decode(final Dictionary dictionary) {
        return Statements.VALUE_FACTORY.createStatement( //
                (Resource) dictionary.decode(this.subjectID), //
                (URI) dictionary.decode(this.predicateID), //
                dictionary.decode(this.objectID), //
                (Resource) dictionary.decode(this.contextID));
    }

    public static Quad encode(final Dictionary dictionary, final Statement statement) {
        return new Quad( //
                dictionary.encode(statement.getSubject()), //
                dictionary.encode(statement.getPredicate()), //
                dictionary.encode(statement.getObject()), //
                dictionary.encode(statement.getContext()));
    }

    public static Quad encode(final Dictionary dictionary, final Resource subject,
            final URI predicate, final Value object, final Resource context) {
        return new Quad( //
                dictionary.encode(subject), //
                dictionary.encode(predicate), //
                dictionary.encode(object), //
                dictionary.encode(context));
    }

}