package eu.fbk.rdfpro.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
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
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;

final class HashValueFactory extends ValueFactoryBase {

    public static final HashValueFactory INSTANCE = new HashValueFactory();

    private final Map<String, URI> w3cURIs;

    private HashValueFactory() {
        this.w3cURIs = new HashMap<>(1024);
        for (final URI uri : new URI[] { XMLSchema.DECIMAL, XMLSchema.INTEGER,
                XMLSchema.NON_POSITIVE_INTEGER, XMLSchema.NEGATIVE_INTEGER,
                XMLSchema.NON_NEGATIVE_INTEGER, XMLSchema.POSITIVE_INTEGER, XMLSchema.LONG,
                XMLSchema.INT, XMLSchema.SHORT, XMLSchema.BYTE, XMLSchema.UNSIGNED_LONG,
                XMLSchema.UNSIGNED_INT, XMLSchema.UNSIGNED_SHORT, XMLSchema.UNSIGNED_BYTE,
                XMLSchema.DOUBLE, XMLSchema.FLOAT, XMLSchema.BOOLEAN, XMLSchema.DATETIME,
                XMLSchema.DATE, XMLSchema.TIME, XMLSchema.GYEARMONTH, XMLSchema.GMONTHDAY,
                XMLSchema.GYEAR, XMLSchema.GMONTH, XMLSchema.GDAY, XMLSchema.DURATION,
                XMLSchema.DAYTIMEDURATION, XMLSchema.STRING, XMLSchema.BASE64BINARY,
                XMLSchema.HEXBINARY, XMLSchema.ANYURI, XMLSchema.QNAME, XMLSchema.NOTATION,
                XMLSchema.NORMALIZEDSTRING, XMLSchema.TOKEN, XMLSchema.LANGUAGE,
                XMLSchema.NMTOKEN, XMLSchema.NMTOKENS, XMLSchema.NAME, XMLSchema.NCNAME,
                XMLSchema.ID, XMLSchema.IDREF, XMLSchema.IDREFS, XMLSchema.ENTITY,
                XMLSchema.ENTITIES, RDF.TYPE, RDF.PROPERTY, RDF.XMLLITERAL, RDF.SUBJECT,
                RDF.PREDICATE, RDF.OBJECT, RDF.STATEMENT, RDF.BAG, RDF.ALT, RDF.SEQ, RDF.VALUE,
                RDF.LI, RDF.LIST, RDF.FIRST, RDF.REST, RDF.NIL, RDF.LANGSTRING, RDF.HTML,
                RDFS.RESOURCE, RDFS.LITERAL, RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.SUBPROPERTYOF,
                RDFS.DOMAIN, RDFS.RANGE, RDFS.COMMENT, RDFS.LABEL, RDFS.DATATYPE, RDFS.CONTAINER,
                RDFS.MEMBER, RDFS.ISDEFINEDBY, RDFS.SEEALSO, RDFS.CONTAINERMEMBERSHIPPROPERTY,
                OWL.CLASS, OWL.INDIVIDUAL, OWL.THING, OWL.NOTHING, OWL.EQUIVALENTCLASS,
                OWL.EQUIVALENTPROPERTY, OWL.SAMEAS, OWL.DIFFERENTFROM, OWL.ALLDIFFERENT,
                OWL.DISTINCTMEMBERS, OWL.OBJECTPROPERTY, OWL.DATATYPEPROPERTY, OWL.INVERSEOF,
                OWL.TRANSITIVEPROPERTY, OWL.SYMMETRICPROPERTY, OWL.FUNCTIONALPROPERTY,
                OWL.INVERSEFUNCTIONALPROPERTY, OWL.RESTRICTION, OWL.ONPROPERTY, OWL.ALLVALUESFROM,
                OWL.SOMEVALUESFROM, OWL.MINCARDINALITY, OWL.MAXCARDINALITY, OWL.CARDINALITY,
                OWL.ONTOLOGY, OWL.IMPORTS, OWL.INTERSECTIONOF, OWL.VERSIONINFO, OWL.VERSIONIRI,
                OWL.PRIORVERSION, OWL.BACKWARDCOMPATIBLEWITH, OWL.INCOMPATIBLEWITH,
                OWL.DEPRECATEDCLASS, OWL.DEPRECATEDPROPERTY, OWL.ANNOTATIONPROPERTY,
                OWL.ONTOLOGYPROPERTY, OWL.ONEOF, OWL.HASVALUE, OWL.DISJOINTWITH, OWL.UNIONOF,
                OWL.COMPLEMENTOF }) {
            final HashURI h = new HashURI(uri.stringValue());
            h.initHash();
            this.w3cURIs.put(uri.stringValue(), h);
        }
    }

    private boolean isPossibleW3CURI(final String uri) {
        return uri.length() > 33 && uri.charAt(12) == '3';
    }

    @Override
    public URI createURI(final String uri) {
        if (isPossibleW3CURI(uri)) {
            final URI u = this.w3cURIs.get(uri);
            if (u != null) {
                return u;
            }
        }
        return new HashURI(uri);
    }

    @Override
    public URI createURI(final String namespace, final String localName) {
        final String uri = namespace + localName;
        if (isPossibleW3CURI(uri)) {
            final URI u = this.w3cURIs.get(uri);
            if (u != null) {
                return u;
            }
        }
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
        return new HashLiteral(label, language.intern(), null);
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

    private static abstract class HashValue implements Value, Hashable {

        private static final long serialVersionUID = 1L;

        transient long hashLo;

        transient long hashHi;

        transient int hashCode;

        @Override
        public final Hash getHash() {
            Hash hash;
            if (this.hashLo != 0L) {
                hash = Hash.fromLongs(this.hashHi, this.hashLo);
            } else {
                hash = Statements.computeHash(this);
                this.hashHi = hash.getHigh();
                this.hashLo = hash.getLow();
            }
            return hash;
        }

        final boolean hasHash() {
            return this.hashLo != 0L;
        }

        final void initHash() {
            if (this.hashLo == 0L) {
                final Hash hash = Statements.computeHash(this);
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
            } else if (object instanceof HashLiteral && hasHash() && ((HashURI) object).hasHash()) {
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
            } else if (object instanceof HashBNode && hasHash() && ((HashURI) object).hasHash()) {
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
            } else if (object instanceof HashURI && hasHash() && ((HashURI) object).hasHash()) {
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
