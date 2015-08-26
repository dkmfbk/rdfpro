package eu.fbk.rdfpro.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryBase;
import org.openrdf.model.util.URIUtil;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;

final class HashValueFactory extends ValueFactoryBase {

    public static final HashValueFactory INSTANCE = new HashValueFactory();

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
    public Statement createStatement(final Resource subj, final URI pred, final Value obj) {
        return new StatementImpl(subj, pred, obj);
    }

    @Override
    public Statement createStatement(final Resource subj, final URI pred, final Value obj,
            final Resource ctx) {
        return ctx == null ? new StatementImpl(subj, pred, obj) //
                : new ContextStatementImpl(subj, pred, obj, ctx);
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

    public static Hash hash(final Value value) {
        if (value instanceof HashValue) {
            return ((HashValue) value).getHash();
        } else {
            return Statements.doHash(value);
        }
    }

    private static abstract class HashValue implements Value {

        private static final long serialVersionUID = 1L;

        transient long hashLo;

        transient long hashHi;

        transient int hashCode;

        final Hash getHash() {
            Hash hash;
            if (this.hashLo != 0L) {
                hash = Hash.fromLongs(this.hashHi, this.hashLo);
            } else {
                hash = Statements.doHash(this);
                this.hashHi = hash.getHigh();
                this.hashLo = hash.getLow();
            }
            return hash;
        }

        final void initHash() {
            if (this.hashLo == 0L) {
                final Hash hash = Statements.doHash(this);
                this.hashLo = hash.getLow();
                this.hashHi = hash.getHigh();
            }
        }

        final boolean sameHash(final HashValue other) {
            initHash();
            other.initHash();
            return this.hashLo == other.hashLo && this.hashHi == other.hashHi;
        }

    }

    private static abstract class HashResource extends HashValue implements Resource {

        private static final long serialVersionUID = 1L;

    }

    private static final class HashLiteral extends HashValue implements Literal {

        private static final long serialVersionUID = 1L;

        private final String label;

        @Nullable
        private final Object languageOrDatatype;

        HashLiteral(final String label, final String language, final URI datatype) {
            this.label = label;
            this.languageOrDatatype = language != null ? language : datatype;
        }

        @Override
        public String getLabel() {
            return this.label;
        }

        @Override
        public String getLanguage() {
            return this.languageOrDatatype instanceof String ? (String) this.languageOrDatatype
                    : null;
        }

        @Override
        public URI getDatatype() {
            if (this.languageOrDatatype instanceof URI) {
                return (URI) this.languageOrDatatype;
            } else if (this.languageOrDatatype instanceof String) {
                return RDF.LANGSTRING;
            } else {
                return XMLSchema.STRING;
            }
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
                return sameHash((HashLiteral) object);
            } else if (object instanceof Literal) {
                final Literal other = (Literal) object;
                return this.label.equals(other.getLabel())
                        && Objects.equals(getDatatype(), other.getDatatype())
                        && Objects.equals(getLanguage(), other.getLanguage());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (this.hashCode == 0) {
                int hashCode = this.label.hashCode();
                final String language = getLanguage();
                if (language != null) {
                    hashCode = 31 * hashCode + language.hashCode();
                }
                hashCode = 31 * hashCode + getDatatype().hashCode();
                this.hashCode = hashCode;
            }
            return this.hashCode;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(this.label.length() * 2);
            sb.append('"');
            sb.append(this.label);
            sb.append('"');
            final String language = getLanguage();
            if (language != null) {
                sb.append('@');
                sb.append(language);
            } else {
                final URI datatype = getDatatype();
                if (datatype != null && !datatype.equals(XMLSchema.STRING)) {
                    sb.append("^^<");
                    sb.append(datatype.stringValue());
                    sb.append(">");
                }
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
                return sameHash((HashBNode) object);
            } else if (object instanceof BNode) {
                final BNode other = (BNode) object;
                return this.id.equals(other.getID());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (this.hashCode == 0) {
                this.hashCode = this.id.hashCode();
            }
            return this.hashCode;
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
                return sameHash(other);
            } else if (object instanceof URI) {
                return this.uri.equals(((URI) object).stringValue());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (this.hashCode == 0) {
                this.hashCode = this.uri.hashCode();
            }
            return this.hashCode;
        }

        @Override
        public String toString() {
            return this.uri;
        }

    }

}
