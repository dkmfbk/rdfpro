package eu.fbk.rdfpro.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.ValueFactoryBase;
import org.openrdf.model.util.URIUtil;
import org.openrdf.model.vocabulary.SESAME;

final class HashValueFactory extends ValueFactoryBase {

    public static final HashValueFactory INSTANCE = new HashValueFactory();

    private static final URI NIL = normalize(SESAME.NIL);

    private HashValueFactory() {
    }

    @Override
    public URI createURI(final String uri) {
        return new HashURI(uri);
    }

    @Override
    public URI createURI(final String namespace, final String localName) {
        final String uri = namespace + localName;
        if (URIUtil.isCorrectURISplit(namespace, localName)) {
            return new HashURI(uri, namespace, localName);
        } else {
            return new HashURI(uri);
        }
    }

    @Override
    public BNode createBNode(final String id) {
        return new HashBNode(id);
    }

    @Override
    public Literal createLiteral(final String label) {
        return new HashLiteral(label, null, null);
    }

    @Override
    public Literal createLiteral(final String label, final String language) {
        return new HashLiteral(label, language, null);
    }

    @Override
    public Literal createLiteral(final String label, final URI datatype) {
        return new HashLiteral(label, null, datatype);
    }

    @Override
    public Statement createStatement(final Resource subject, final URI predicate,
            final Value object) {
        return new HashStatement(normalize(subject), normalize(predicate), normalize(object), null);
    }

    @Override
    public Statement createStatement(final Resource subject, final URI predicate,
            final Value object, final Resource context) {
        return new HashStatement(normalize(subject), normalize(predicate), normalize(object),
                normalize(context));
    }

    public static Function<Value, Value> getValueNormalizer() {
        return new Function<Value, Value>() {

            @Override
            public Value apply(final Value value) {
                return normalize(value);
            }

        };
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T extends Value> T normalize(@Nullable final T value) {
        if (value instanceof HashValue) {
            return value;
        } else if (value instanceof URI) {
            return (T) new HashURI(value.stringValue());
        } else if (value instanceof BNode) {
            return (T) new HashBNode(((BNode) value).getID());
        } else if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            return (T) new HashLiteral(literal.getLabel(), literal.getLanguage(),
                    literal.getDatatype());
        } else {
            return null;
        }
    }

    public static <T extends Value> T normalize(@Nullable final T value,
            @Nullable final T valueIfNull) {
        if (value == null) {
            return valueIfNull;
        } else {
            return normalize(value);
        }
    }

    @Nullable
    public static Statement normalize(@Nullable final Statement statement) {
        if (statement instanceof HashStatement) {
            return statement;
        } else if (statement != null) {
            return new HashStatement(normalize(statement.getSubject()),
                    normalize(statement.getPredicate()), normalize(statement.getObject()),
                    normalize(statement.getContext()));
        } else {
            return null;
        }
    }

    @Nullable
    public static Hash hash(@Nullable final Value value) {
        if (value instanceof HashValue) {
            return ((HashValue) value).getHash();
        } else if (value != null) {
            return doHash(value);
        } else {
            return null;
        }
    }

    @Nullable
    public static Hash hash(@Nullable final Statement statement) {
        if (statement instanceof HashStatement) {
            return ((HashStatement) statement).getHash();
        } else if (statement != null) {
            return doHash(statement);
        } else {
            return null;
        }
    }

    private static Hash doHash(final Value value) {
        Hash hash;
        if (value instanceof URI) {
            hash = Hash.murmur3("\u0001", value.stringValue());
        } else if (value instanceof BNode) {
            hash = Hash.murmur3("\u0002", ((BNode) value).getID());
        } else {
            final Literal l = (Literal) value;
            if (l.getLanguage() != null) {
                hash = Hash.murmur3("\u0003", l.getLanguage(), l.getLabel());
            } else if (l.getDatatype() != null) {
                hash = Hash.murmur3("\u0004", l.getDatatype().stringValue(), l.getLabel());
            } else {
                hash = Hash.murmur3("\u0005", l.getLabel());
            }
        }
        if (hash.getLow() == 0) {
            hash = Hash.fromLongs(hash.getHigh(), 1L);
        }
        return hash;
    }

    private static Hash doHash(final Statement statement) {
        final Hash subjHash = hash(statement.getSubject());
        final Hash predHash = hash(statement.getPredicate());
        final Hash objHash = hash(statement.getObject());
        final Hash ctxHash = hash(normalize(statement.getContext(), NIL));
        return Hash.combine(subjHash, predHash, objHash, ctxHash);
    }

    private static abstract class HashValue implements Value {

        private static final long serialVersionUID = 1L;

        transient long hashLo;

        transient long hashHi;

        final void initHash() {
            if (this.hashLo == 0L) {
                final Hash hash = doHash(this);
                this.hashLo = hash.getLow();
                this.hashHi = hash.getHigh();
            }
        }

        final Hash getHash() {
            Hash hash;
            if (this.hashLo != 0L) {
                hash = Hash.fromLongs(this.hashLo, this.hashHi);
            } else {
                hash = doHash(this);
                this.hashLo = hash.getLow();
                this.hashHi = hash.getHigh();
            }
            return hash;
        }

    }

    private static abstract class HashResource extends HashValue implements Resource {

        private static final long serialVersionUID = 1L;

    }

    private static final class HashLiteral extends HashValue implements Literal {

        private static final long serialVersionUID = 1L;

        private final String label;

        @Nullable
        private final String language;

        @Nullable
        private final URI datatype;

        HashLiteral(final String label, final String language, final URI datatype) {
            this.label = label;
            this.language = language == null ? null : language.toLowerCase();
            this.datatype = datatype;
        }

        @Override
        public String getLabel() {
            return this.label;
        }

        @Override
        public String getLanguage() {
            return this.language;
        }

        @Override
        public URI getDatatype() {
            return this.datatype;
        }

        @Override
        public String stringValue() {
            return this.label;
        }

        @Override
        public boolean booleanValue() {
            return XMLDatatypeUtil.parseBoolean(this.label);
        }

        @Override
        public byte byteValue() {
            return XMLDatatypeUtil.parseByte(this.label);
        }

        @Override
        public short shortValue() {
            return XMLDatatypeUtil.parseShort(this.label);
        }

        @Override
        public int intValue() {
            return XMLDatatypeUtil.parseInt(this.label);
        }

        @Override
        public long longValue() {
            return XMLDatatypeUtil.parseLong(this.label);
        }

        @Override
        public float floatValue() {
            return XMLDatatypeUtil.parseFloat(this.label);
        }

        @Override
        public double doubleValue() {
            return XMLDatatypeUtil.parseDouble(this.label);
        }

        @Override
        public BigInteger integerValue() {
            return XMLDatatypeUtil.parseInteger(this.label);
        }

        @Override
        public BigDecimal decimalValue() {
            return XMLDatatypeUtil.parseDecimal(this.label);
        }

        @Override
        public XMLGregorianCalendar calendarValue() {
            return XMLDatatypeUtil.parseCalendar(this.label);
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            } else if (object instanceof HashLiteral) {
                final HashLiteral other = (HashLiteral) object;
                initHash();
                other.initHash();
                return this.hashHi == other.hashHi && this.hashLo == other.hashLo;
            } else if (object instanceof Literal) {
                final Literal other = (Literal) object;
                return this.label.equals(other.getLabel())
                        && Objects.equals(this.datatype, other.getDatatype())
                        && Objects.equals(this.language, other.getLanguage());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return this.label.hashCode();
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(this.label.length() * 2);
            sb.append('"');
            sb.append(this.label);
            sb.append('"');
            if (this.language != null) {
                sb.append('@');
                sb.append(this.language);
            }
            if (this.datatype != null) {
                sb.append("^^<");
                sb.append(this.datatype.toString());
                sb.append(">");
            }
            return sb.toString();
        }

    }

    private static final class HashBNode extends HashResource implements BNode {

        private static final long serialVersionUID = 1L;

        private final String id;

        HashBNode(final String id) {
            this.id = id;
        }

        @Override
        public String getID() {
            return this.id;
        }

        @Override
        public String stringValue() {
            return this.id;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            } else if (object instanceof HashBNode) {
                final HashBNode other = (HashBNode) object;
                initHash();
                other.initHash();
                return this.hashHi == other.hashHi && this.hashLo == other.hashLo;
            } else if (object instanceof BNode) {
                final BNode other = (BNode) object;
                return this.id.equals(other.getID());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public String toString() {
            return "_:" + this.id;
        }

    }

    private static final class HashURI extends HashResource implements URI {

        private static final long serialVersionUID = 1L;

        private final String uri;

        @Nullable
        private transient String namespace;

        @Nullable
        private transient String localName;

        HashURI(final String uri) {
            this.uri = uri;
            this.namespace = null;
            this.localName = null;
        }

        HashURI(final String uri, final String namespace, final String localName) {
            this.uri = uri;
            this.namespace = namespace;
            this.localName = localName;
        }

        private void splitURI() {
            final int index = URIUtil.getLocalNameIndex(this.uri);
            this.namespace = this.uri.substring(0, index);
            this.localName = this.uri.substring(index);
        }

        @Override
        public String getNamespace() {
            if (this.namespace == null) {
                splitURI();
            }
            return this.namespace;
        }

        @Override
        public String getLocalName() {
            if (this.localName == null) {
                splitURI();
            }
            return this.localName;
        }

        @Override
        public String stringValue() {
            return this.uri;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            } else if (object instanceof HashURI) {
                final HashURI other = (HashURI) object;
                initHash();
                other.initHash();
                return this.hashHi == other.hashHi && this.hashLo == other.hashLo;
            } else if (object instanceof URI) {
                return this.uri.equals(object.toString());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return this.uri.hashCode();
        }

        @Override
        public String toString() {
            return this.uri;
        }

    }

    private static final class HashStatement implements Statement {

        private static final long serialVersionUID = 1L;

        private final Resource subject;

        private final URI predicate;

        private final Value object;

        @Nullable
        private final Resource context;

        @Nullable
        private transient Hash hash;

        HashStatement(final Resource subject, final URI predicate, final Value object,
                @Nullable final Resource context) {
            this.subject = subject;
            this.predicate = predicate;
            this.object = object;
            this.context = context;
        }

        @Override
        public Resource getSubject() {
            return this.subject;
        }

        @Override
        public URI getPredicate() {
            return this.predicate;
        }

        @Override
        public Value getObject() {
            return this.object;
        }

        @Override
        public Resource getContext() {
            return this.context;
        }

        final Hash getHash() {
            if (this.hash == null) {
                this.hash = doHash(this);
            }
            return this.hash;
        }

        @Override
        public boolean equals(final Object object) {
            if (this == object) {
                return true;
            } else if (object instanceof HashStatement) {
                final Hash thisHash = getHash();
                final Hash otherHash = ((HashStatement) object).getHash();
                return thisHash.getHigh() == otherHash.getHigh()
                        && thisHash.getLow() == otherHash.getLow();
            } else if (object instanceof Statement) {
                final Statement other = (Statement) object;
                return this.object.equals(other.getObject())
                        && this.subject.equals(other.getSubject())
                        && this.predicate.equals(other.getPredicate());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return 961 * this.subject.hashCode() + 31 * this.predicate.hashCode()
                    + this.object.hashCode();
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder(256);
            builder.append("(");
            builder.append(this.subject);
            builder.append(", ");
            builder.append(this.predicate);
            builder.append(", ");
            builder.append(this.object);
            builder.append(")");
            if (this.context != null) {
                builder.append(super.toString());
                builder.append(" [").append(getContext()).append("]");
            }
            return builder.toString();
        }

    }

}
