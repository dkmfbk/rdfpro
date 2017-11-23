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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;
import javax.xml.datatype.XMLGregorianCalendar;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.AbstractValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

final class HashValueFactory extends AbstractValueFactory {

    public static final HashValueFactory INSTANCE = new HashValueFactory();

    private final Map<String, IRI> w3cIRIs;

    private HashValueFactory() {
        this.w3cIRIs = new HashMap<>(1024);
        for (final IRI iri : new IRI[] { XMLSchema.DECIMAL, XMLSchema.INTEGER,
                XMLSchema.NON_POSITIVE_INTEGER, XMLSchema.NEGATIVE_INTEGER,
                XMLSchema.NON_NEGATIVE_INTEGER, XMLSchema.POSITIVE_INTEGER, XMLSchema.LONG,
                XMLSchema.INT, XMLSchema.SHORT, XMLSchema.BYTE, XMLSchema.UNSIGNED_LONG,
                XMLSchema.UNSIGNED_INT, XMLSchema.UNSIGNED_SHORT, XMLSchema.UNSIGNED_BYTE,
                XMLSchema.DOUBLE, XMLSchema.FLOAT, XMLSchema.BOOLEAN, XMLSchema.DATETIME,
                XMLSchema.DATE, XMLSchema.TIME, XMLSchema.GYEARMONTH, XMLSchema.GMONTHDAY,
                XMLSchema.GYEAR, XMLSchema.GMONTH, XMLSchema.GDAY, XMLSchema.DURATION,
                XMLSchema.DAYTIMEDURATION, XMLSchema.STRING, XMLSchema.BASE64BINARY,
                XMLSchema.HEXBINARY, XMLSchema.ANYURI, XMLSchema.QNAME, XMLSchema.NOTATION,
                XMLSchema.NORMALIZEDSTRING, XMLSchema.TOKEN, XMLSchema.LANGUAGE, XMLSchema.NMTOKEN,
                XMLSchema.NMTOKENS, XMLSchema.NAME, XMLSchema.NCNAME, XMLSchema.ID,
                XMLSchema.IDREF, XMLSchema.IDREFS, XMLSchema.ENTITY, XMLSchema.ENTITIES, RDF.TYPE,
                RDF.PROPERTY, RDF.XMLLITERAL, RDF.SUBJECT, RDF.PREDICATE, RDF.OBJECT,
                RDF.STATEMENT, RDF.BAG, RDF.ALT, RDF.SEQ, RDF.VALUE, RDF.LI, RDF.LIST, RDF.FIRST,
                RDF.REST, RDF.NIL, RDF.LANGSTRING, RDF.HTML, RDFS.RESOURCE, RDFS.LITERAL,
                RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.SUBPROPERTYOF, RDFS.DOMAIN, RDFS.RANGE,
                RDFS.COMMENT, RDFS.LABEL, RDFS.DATATYPE, RDFS.CONTAINER, RDFS.MEMBER,
                RDFS.ISDEFINEDBY, RDFS.SEEALSO, RDFS.CONTAINERMEMBERSHIPPROPERTY, OWL.CLASS,
                OWL.INDIVIDUAL, OWL.THING, OWL.NOTHING, OWL.EQUIVALENTCLASS,
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
            final HashIRI h = new HashIRI(iri.stringValue());
            h.initHash();
            this.w3cIRIs.put(iri.stringValue(), h);
        }
    }

    private boolean isPossibleW3CIRI(final String iri) {
        return iri.length() > 33 && iri.charAt(12) == '3';
    }

    @Override
    public IRI createIRI(final String iri) {
        if (isPossibleW3CIRI(iri)) {
            final IRI i = this.w3cIRIs.get(iri);
            if (i != null) {
                return i;
            }
        }
        return new HashIRI(iri);
    }

    @Override
    public IRI createIRI(final String namespace, final String localName) {
        final String iri = namespace + localName;
        if (isPossibleW3CIRI(iri)) {
            final IRI i = this.w3cIRIs.get(iri);
            if (i != null) {
                return i;
            }
        }
        if (URIUtil.isCorrectURISplit(namespace, localName)) {
            return new HashIRI(iri, namespace, localName);
        } else {
            return new HashIRI(iri);
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
    public Literal createLiteral(final String label, final IRI datatype) {
        return new HashLiteral(label, null, datatype);
    }

    @Override
    public Statement createStatement(final Resource subj, final IRI pred, final Value obj) {
        return SimpleValueFactory.getInstance().createStatement(subj, pred, obj);
    }

    @Override
    public Statement createStatement(final Resource subj, final IRI pred, final Value obj,
            final Resource ctx) {
        return ctx == null ? SimpleValueFactory.getInstance().createStatement(subj, pred, obj)
                : SimpleValueFactory.getInstance().createStatement(subj, pred, obj, ctx);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public static <T extends Value> T normalize(@Nullable final T value) {
        if (value instanceof HashValue) {
            return value;
        } else if (value instanceof IRI) {
            return (T) new HashIRI(value.stringValue());
        } else if (value instanceof BNode) {
            return (T) new HashBNode(((BNode) value).getID());
        } else if (value instanceof Literal) {
            final Literal literal = (Literal) value;
            return (T) new HashLiteral(literal.getLabel(), literal.getLanguage().orElse(null),
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

        HashLiteral(final String label, final String language, final IRI datatype) {
            this.label = label;
            this.languageOrDatatype = language != null ? language
                    : XMLSchema.STRING.equals(datatype) ? null : datatype;
        }

        @Override
        public String getLabel() {
            return this.label;
        }

        @Override
        public Optional<String> getLanguage() {
            return this.languageOrDatatype instanceof String
                    ? Optional.of((String) this.languageOrDatatype) : Optional.empty();
        }

        @Override
        public IRI getDatatype() {
            if (this.languageOrDatatype instanceof IRI) {
                return (IRI) this.languageOrDatatype;
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
                final HashLiteral other = (HashLiteral) object;
                return hasHash() && other.hasHash() ? sameHash(other)
                        : this.label.equals(other.label) && Objects.equals(this.languageOrDatatype,
                                other.languageOrDatatype);
            } else if (object instanceof Literal) {
                final Literal other = (Literal) object;
                return this.label.equals(other.getLabel())
                        && (this.languageOrDatatype instanceof String
                                && this.languageOrDatatype.equals(other.getLanguage().orElse(null))
                                || this.languageOrDatatype instanceof IRI
                                        && this.languageOrDatatype.equals(other.getDatatype())
                                || other.getDatatype().equals(XMLSchema.STRING));
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (this.hashCode == 0) {
                final int hashCode = this.label.hashCode();
                // if (this.languageOrDatatype instanceof String) {
                // hashCode = 31 * hashCode + this.languageOrDatatype.hashCode();
                // }
                // hashCode = 31 * hashCode + getDatatype().hashCode();
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
            if (this.languageOrDatatype instanceof String) {
                sb.append('@');
                sb.append(this.languageOrDatatype);
            } else {
                sb.append("^^<");
                sb.append((this.languageOrDatatype instanceof IRI ? (IRI) this.languageOrDatatype
                        : XMLSchema.STRING).stringValue());
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
                return hasHash() && other.hasHash() ? sameHash(other) : this.id.equals(other.id);
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

    private static final class HashIRI extends HashResource implements IRI {

        private static final long serialVersionUID = 1L;

        private final String iri;

        @Nullable
        private transient String namespace;

        @Nullable
        private transient String localName;

        HashIRI(final String iri) {
            this.iri = iri;
            this.namespace = null;
            this.localName = null;
        }

        HashIRI(final String iri, final String namespace, final String localName) {
            this.iri = iri;
            this.namespace = namespace;
            this.localName = localName;
        }

        private void splitIRI() {
            final int index = URIUtil.getLocalNameIndex(this.iri);
            this.namespace = this.iri.substring(0, index);
            this.localName = this.iri.substring(index);
        }

        @Override
        public String getNamespace() {
            if (this.namespace == null) {
                splitIRI();
            }
            return this.namespace;
        }

        @Override
        public String getLocalName() {
            if (this.localName == null) {
                splitIRI();
            }
            return this.localName;
        }

        @Override
        public String stringValue() {
            return this.iri;
        }

        @Override
        public boolean equals(final Object object) {
            if (object == this) {
                return true;
            } else if (object instanceof HashIRI) {
                final HashIRI other = (HashIRI) object;
                return hasHash() && other.hasHash() ? sameHash(other) : this.iri.equals(other.iri);
            } else if (object instanceof IRI) {
                return this.iri.equals(((IRI) object).stringValue());
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            if (this.hashCode == 0) {
                this.hashCode = this.iri.hashCode();
            }
            return this.hashCode;
        }

        @Override
        public String toString() {
            return this.iri;
        }

    }

}
