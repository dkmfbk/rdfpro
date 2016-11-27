/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 *
 * Written in 2014 by Francesco Corcoglioniti with support by Marco Amadori, Michele Mostarda,
 * Alessio Palmero Aprosio and Marco Rospocher. Contact info on http://rdfpro.fbk.eu/
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.SESAME;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import eu.fbk.rdfpro.util.Statements;

final class ProcessorRDFS implements RDFProcessor {

    private static final Map<IRI, IRI> VOC;

    static {
        VOC = new IdentityHashMap<IRI, IRI>();
        for (final IRI iri : new IRI[] { RDF.TYPE, RDF.PROPERTY, RDF.XMLLITERAL, RDF.SUBJECT,
                RDF.PREDICATE, RDF.OBJECT, RDF.STATEMENT, RDF.BAG, RDF.ALT, RDF.SEQ, RDF.VALUE,
                RDF.LI, RDF.LIST, RDF.FIRST, RDF.REST, RDF.NIL, RDF.LANGSTRING, RDFS.RESOURCE,
                RDFS.LITERAL, RDFS.CLASS, RDFS.SUBCLASSOF, RDFS.SUBPROPERTYOF, RDFS.DOMAIN,
                RDFS.RANGE, RDFS.COMMENT, RDFS.LABEL, RDFS.DATATYPE, RDFS.CONTAINER, RDFS.MEMBER,
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
                OWL.COMPLEMENTOF, XMLSchema.DURATION, XMLSchema.DATETIME,
                XMLSchema.DAYTIMEDURATION, XMLSchema.TIME, XMLSchema.DATE, XMLSchema.GYEARMONTH,
                XMLSchema.GYEAR, XMLSchema.GMONTHDAY, XMLSchema.GDAY, XMLSchema.GMONTH,
                XMLSchema.STRING, XMLSchema.BOOLEAN, XMLSchema.BASE64BINARY, XMLSchema.HEXBINARY,
                XMLSchema.FLOAT, XMLSchema.DECIMAL, XMLSchema.DOUBLE, XMLSchema.ANYURI,
                XMLSchema.QNAME, XMLSchema.NOTATION, XMLSchema.NORMALIZEDSTRING, XMLSchema.TOKEN,
                XMLSchema.LANGUAGE, XMLSchema.NMTOKEN, XMLSchema.NMTOKENS, XMLSchema.NAME,
                XMLSchema.NCNAME, XMLSchema.ID, XMLSchema.IDREF, XMLSchema.IDREFS,
                XMLSchema.ENTITY, XMLSchema.ENTITIES, XMLSchema.INTEGER, XMLSchema.LONG,
                XMLSchema.INT, XMLSchema.SHORT, XMLSchema.BYTE, XMLSchema.NON_POSITIVE_INTEGER,
                XMLSchema.NEGATIVE_INTEGER, XMLSchema.NON_NEGATIVE_INTEGER,
                XMLSchema.POSITIVE_INTEGER, XMLSchema.UNSIGNED_LONG, XMLSchema.UNSIGNED_INT,
                XMLSchema.UNSIGNED_SHORT, XMLSchema.UNSIGNED_BYTE }) {
            ProcessorRDFS.VOC.put(iri, iri);
        }
    }

    private final Ruleset ruleset;

    private final TBox tbox;

    private final boolean dropBNodeTypes;

    private final boolean emitTBox;

    ProcessorRDFS(final RDFSource tbox, @Nullable final Resource tboxContext,
            final boolean decomposeOWLAxioms, final boolean dropBNodeTypes,
            final String... excludedRules) {

        final Map<Value, Value> interner = new HashMap<Value, Value>();
        for (final IRI iri : ProcessorRDFS.VOC.keySet()) {
            interner.put(iri, iri);
        }

        final Database database = new Database();
        tbox.forEach(new Consumer<Statement>() {

            @Override
            public void accept(final Statement t) {
                final Resource s = this.normalize(t.getSubject());
                final IRI p = this.normalize(t.getPredicate());
                final Value o = this.normalize(t.getObject());
                database.add(s, p, o);
            }

            @SuppressWarnings("unchecked")
            @Nullable
            private <T extends Value> T normalize(@Nullable T value) {
                final Value v = interner.get(value);
                if (v != null) {
                    return (T) v;
                }
                if (value instanceof Literal) {
                    final Literal lit = (Literal) value;
                    final IRI dt = lit.getDatatype();
                    if (dt != null) {
                        final IRI dtn = this.normalize(dt);
                        if (dtn != dt) {
                            value = (T) Statements.VALUE_FACTORY.createLiteral(lit.getLabel(),
                                    dtn);
                        }
                    }
                }
                interner.put(value, value);
                return value;
            }

        });
        database.commit();
        final Ruleset ruleset = excludedRules == null || excludedRules.length == 0
                ? Ruleset.DEFAULT : new Ruleset(excludedRules);

        new TBoxInferencer(decomposeOWLAxioms, ruleset, database).infer();

        this.tbox = new TBox(database, SESAME.NIL.equals(tboxContext) ? null : tboxContext);
        this.dropBNodeTypes = dropBNodeTypes;
        this.emitTBox = tboxContext != null;
        this.ruleset = ruleset;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(Objects.requireNonNull(handler));
    }

    private final class Handler extends AbstractRDFHandlerWrapper {

        private final Deduplicator deduplicator;

        private ThreadLocal<ABoxInferencer> inferencer;

        Handler(final RDFHandler handler) {

            super(handler);

            this.deduplicator = new Deduplicator();
            this.inferencer = new ThreadLocal<ABoxInferencer>() {

                @Override
                protected ABoxInferencer initialValue() {
                    return new ABoxInferencer(Handler.this.handler, ProcessorRDFS.this.ruleset,
                            ProcessorRDFS.this.tbox, Handler.this.deduplicator,
                            ProcessorRDFS.this.dropBNodeTypes);
                }

            };
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.inferencer.get().handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (ProcessorRDFS.this.emitTBox) {
                for (final Statement statement : ProcessorRDFS.this.tbox.statements) {
                    super.handleStatement(statement);
                }
            }
            this.handler.endRDF();
        }

    }

    private static final class TBox {

        private static final Resource[] EMPTY = new Resource[0];

        final List<Statement> statements;

        final Map<Resource, Resource> resources;

        final Map<Resource, Type> types;

        final Map<Resource, Property> properties;

        TBox(final Database database, @Nullable final Resource context) {

            final List<Statement> attributes = new ArrayList<Statement>();

            final Map<Resource, Resource> resources = new HashMap<Resource, Resource>();
            final Map<Resource, Type> types = new HashMap<Resource, Type>();
            final Map<Resource, Property> properties = new HashMap<Resource, Property>();

            final Statement[] statementArray = new Statement[database.size()];

            int index = 0;
            for (final Statement statement : database) {

                final Resource s = statement.getSubject();
                final IRI p = statement.getPredicate();
                final Value o = statement.getObject();

                statementArray[index++] = Objects.equals(context, statement.getContext())
                        ? statement
                        : context == null ? Statements.VALUE_FACTORY.createStatement(s, p, o)
                                : Statements.VALUE_FACTORY.createStatement(s, p, o, context);

                resources.put(s, s);
                resources.put(p, p);
                if (o instanceof Resource) {
                    resources.put((Resource) o, (Resource) o);
                }

                if (o instanceof Resource && (p.equals(RDFS.SUBCLASSOF) || p.equals(RDFS.DOMAIN)
                        || p.equals(RDFS.RANGE)
                        || p.equals(RDFS.SUBPROPERTYOF) && o instanceof IRI)) {
                    attributes.add(statement);
                }
            }

            Collections.sort(attributes, Sorter.INSTANCE);

            final int length = attributes.size();
            Resource subject = null;
            Resource[] parents = TBox.EMPTY;
            Resource[] domain = TBox.EMPTY;
            Resource[] range = TBox.EMPTY;
            boolean property = false;

            int i = 0;
            while (i < length) {
                final Statement t = attributes.get(i);
                final Resource s = t.getSubject();
                final IRI p = t.getPredicate();

                if (s != subject) {
                    if (subject != null) {
                        if (property) {
                            properties.put(subject, new Property(parents, domain, range));
                        } else {
                            types.put(subject, new Type(parents));
                        }
                    }
                    subject = s;
                    parents = TBox.EMPTY;
                    domain = TBox.EMPTY;
                    range = TBox.EMPTY;
                }

                final int start = i;
                for (++i; i < length; ++i) {
                    final Statement t2 = attributes.get(i);
                    if (t2.getSubject() != s || t2.getPredicate() != p) {
                        break;
                    }
                }

                final Resource[] array = new Resource[i - start];
                for (int j = start; j < i; ++j) {
                    array[j - start] = (Resource) attributes.get(j).getObject();
                }

                property = p != RDFS.SUBCLASSOF;
                if (p == RDFS.SUBCLASSOF || p == RDFS.SUBPROPERTYOF) {
                    parents = array;
                } else if (p == RDFS.DOMAIN) {
                    domain = array;
                } else if (p == RDFS.RANGE) {
                    range = array;
                }
            }

            this.statements = Arrays.asList(statementArray);
            this.resources = resources; // should use immutable maps here...
            this.types = types;
            this.properties = properties;
        }

        static final class Type {

            final Resource[] parents;

            Type(final Resource[] parents) {
                this.parents = parents;
            }

        }

        static final class Property {

            final Resource[] parents;

            final Resource[] domain;

            final Resource[] range;

            Property(final Resource[] parents, final Resource[] domain, final Resource[] range) {
                this.parents = parents;
                this.domain = domain;
                this.range = range;
            }

        }

        private static final class Sorter implements Comparator<Statement> {

            static Sorter INSTANCE = new Sorter();

            @Override
            public int compare(final Statement t1, final Statement t2) {
                int result = 0;
                if (t1 != t2) {
                    result = System.identityHashCode(t1.getSubject())
                            - System.identityHashCode(t2.getSubject());
                    if (result == 0) {
                        result = System.identityHashCode(t1.getPredicate())
                                - System.identityHashCode(t2.getPredicate());
                    }
                }
                return result;
            }

        }

    }

    private static final class TBoxInferencer {

        private final boolean decomposeOWLAxioms;

        private final Ruleset ruleset;

        private final Database db;

        private Iterable<Statement> delta; // previously added statements

        TBoxInferencer(final boolean decomposeOWLAxioms, final Ruleset ruleset,
                final Database database) {
            this.decomposeOWLAxioms = decomposeOWLAxioms;
            this.ruleset = ruleset;
            this.db = database;
        }

        void infer() {

            this.addAxioms();
            if (this.decomposeOWLAxioms) {
                this.decomposeOWLAxioms();
            }
            this.db.commit();

            this.delta = this.db;
            while (true) {
                this.evalRules();
                final List<Statement> added = this.db.commit();
                if (added.isEmpty()) {
                    break;
                }
                this.delta = added;
            }
        }

        private void addAxioms() {

            this.emit(RDF.TYPE, RDFS.DOMAIN, RDFS.RESOURCE);
            this.emit(RDFS.DOMAIN, RDFS.DOMAIN, RDF.PROPERTY);
            this.emit(RDFS.RANGE, RDFS.DOMAIN, RDF.PROPERTY);
            this.emit(RDFS.SUBPROPERTYOF, RDFS.DOMAIN, RDF.PROPERTY);
            this.emit(RDFS.SUBCLASSOF, RDFS.DOMAIN, RDFS.CLASS);
            this.emit(RDF.SUBJECT, RDFS.DOMAIN, RDF.STATEMENT);
            this.emit(RDF.PREDICATE, RDFS.DOMAIN, RDF.STATEMENT);
            this.emit(RDF.OBJECT, RDFS.DOMAIN, RDF.STATEMENT);
            this.emit(RDFS.MEMBER, RDFS.DOMAIN, RDFS.RESOURCE);
            this.emit(RDF.FIRST, RDFS.DOMAIN, RDF.LIST);
            this.emit(RDF.REST, RDFS.DOMAIN, RDF.LIST);
            this.emit(RDFS.SEEALSO, RDFS.DOMAIN, RDFS.RESOURCE);
            this.emit(RDFS.ISDEFINEDBY, RDFS.DOMAIN, RDFS.RESOURCE);
            this.emit(RDFS.COMMENT, RDFS.DOMAIN, RDFS.RESOURCE);
            this.emit(RDFS.LABEL, RDFS.DOMAIN, RDFS.RESOURCE);
            this.emit(RDF.VALUE, RDFS.DOMAIN, RDFS.RESOURCE);

            this.emit(RDF.TYPE, RDFS.RANGE, RDFS.CLASS);
            this.emit(RDFS.DOMAIN, RDFS.RANGE, RDFS.CLASS);
            this.emit(RDFS.RANGE, RDFS.RANGE, RDFS.CLASS);
            this.emit(RDFS.SUBPROPERTYOF, RDFS.RANGE, RDF.PROPERTY);
            this.emit(RDFS.SUBCLASSOF, RDFS.RANGE, RDFS.CLASS);
            this.emit(RDF.SUBJECT, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDF.PREDICATE, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDF.OBJECT, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDFS.MEMBER, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDF.FIRST, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDF.REST, RDFS.RANGE, RDF.LIST);
            this.emit(RDFS.SEEALSO, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDFS.ISDEFINEDBY, RDFS.RANGE, RDFS.RESOURCE);
            this.emit(RDFS.COMMENT, RDFS.RANGE, RDFS.LITERAL);
            this.emit(RDFS.LABEL, RDFS.RANGE, RDFS.LITERAL);
            this.emit(RDF.VALUE, RDFS.RANGE, RDFS.RESOURCE);

            this.emit(RDF.ALT, RDFS.SUBCLASSOF, RDFS.CONTAINER);
            this.emit(RDF.BAG, RDFS.SUBCLASSOF, RDFS.CONTAINER);
            this.emit(RDF.SEQ, RDFS.SUBCLASSOF, RDFS.CONTAINER);
            this.emit(RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY);

            this.emit(RDFS.ISDEFINEDBY, RDFS.SUBPROPERTYOF, RDFS.SEEALSO);
            this.emit(RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.CLASS);
        }

        private void decomposeOWLAxioms() {

            final Map<IRI, List<Resource>> subprops = new HashMap<IRI, List<Resource>>();
            final Map<IRI, List<Resource>> domains = new HashMap<IRI, List<Resource>>();
            final Map<IRI, List<Resource>> ranges = new HashMap<IRI, List<Resource>>();
            final List<IRI[]> inverses = new ArrayList<IRI[]>();

            final Map<Resource, Resource[]> nodes = new HashMap<Resource, Resource[]>();
            final Map<Resource, Resource> intersections = new HashMap<Resource, Resource>();
            final Map<Resource, Resource> unions = new HashMap<Resource, Resource>();

            for (final Statement t : this.db) {

                final Resource s = t.getSubject();
                final IRI p = t.getPredicate();
                final Value o = t.getObject();

                if (p == RDF.TYPE) {
                    if (o == OWL.CLASS || o == OWL.RESTRICTION) {
                        this.emit(s, RDF.TYPE, RDFS.CLASS);
                    } else if (o == OWL.ANNOTATIONPROPERTY || o == OWL.DATATYPEPROPERTY
                            || o == OWL.OBJECTPROPERTY) {
                        this.emit(s, RDF.TYPE, RDF.PROPERTY);
                    }

                } else if (p == OWL.EQUIVALENTCLASS) {
                    if (o instanceof Resource) {
                        this.emit(s, RDFS.SUBCLASSOF, o);
                        this.emit((Resource) o, RDFS.SUBCLASSOF, s);
                    }

                } else if (p == OWL.EQUIVALENTPROPERTY) {
                    if (s instanceof IRI && o instanceof IRI && !s.equals(o)) {
                        for (final IRI prop : new IRI[] { (IRI) s, (IRI) o }) {
                            final IRI other = prop == s ? (IRI) o : (IRI) s;
                            this.emit(prop, RDFS.SUBPROPERTYOF, other);
                            List<Resource> list = subprops.get(prop);
                            if (list == null) {
                                list = new ArrayList<>();
                                subprops.put(prop, list);
                            }
                            if (!list.contains(other)) {
                                list.add(other);
                            }
                        }
                    }

                } else if (p == RDFS.DOMAIN || p == RDFS.RANGE || p == RDFS.SUBPROPERTYOF) {
                    if (s instanceof IRI && (o instanceof IRI
                            || o instanceof Resource && p != RDFS.SUBPROPERTYOF)) {
                        final Map<IRI, List<Resource>> map = p == RDFS.DOMAIN ? domains
                                : p == RDFS.RANGE ? ranges : subprops;
                        List<Resource> list = map.get(s);
                        if (list == null) {
                            list = new ArrayList<>();
                            map.put((IRI) s, list);
                        }
                        list.add((Resource) o);
                    }

                } else if (p == OWL.INVERSEOF) {
                    if (s instanceof IRI && o instanceof IRI) {
                        inverses.add(new IRI[] { (IRI) s, (IRI) o });
                    }

                } else if (p == OWL.INTERSECTIONOF) {
                    if (o instanceof Resource) {
                        intersections.put(s, (Resource) o);
                    }

                } else if (p == OWL.UNIONOF) {
                    if (o instanceof Resource) {
                        unions.put(s, (Resource) o);
                    }

                } else if (p == RDF.FIRST || p == RDF.REST) {
                    if (o instanceof Resource) {
                        Resource[] node = nodes.get(s);
                        if (node == null) {
                            node = new Resource[2];
                            nodes.put(s, node);
                        }
                        node[p == RDF.FIRST ? 0 : 1] = (Resource) o;
                    }
                }
            }

            // p owl:inverseOf q, q rdfs:subPropertyOf* q1, q1 rdfs:domain C -> p rdfs:range C
            // p owl:inverseOf q, q rdfs:subPropertyOf* q1, q1 rdfs:range C -> p rdfs:domain C
            for (final IRI[] pair : inverses) {
                for (int i = 0; i < 2; ++i) {
                    final IRI property = pair[i];
                    final Set<IRI> others = new HashSet<IRI>();
                    others.add(pair[1 - i]);
                    boolean changed;
                    do {
                        changed = false;
                        for (final IRI other : others.toArray(new IRI[others.size()])) {
                            final List<Resource> parents = subprops.get(other);
                            if (parents != null) {
                                for (final Resource parent : parents) {
                                    if (others.add((IRI) parent)) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    } while (changed);
                    for (final IRI other : others) {
                        final List<Resource> otherDomains = domains.get(other);
                        if (otherDomains != null) {
                            for (final Resource domain : otherDomains) {
                                this.emit(property, RDFS.RANGE, domain);
                            }
                        }
                        final List<Resource> otherRanges = ranges.get(other);
                        if (otherRanges != null) {
                            for (final Resource range : otherRanges) {
                                this.emit(property, RDFS.DOMAIN, range);
                            }
                        }
                    }
                }
            }

            // C owl:unionOf list(Ci) -> Ci rdfs:subClassOf C
            for (final Map.Entry<Resource, Resource> entry : unions.entrySet()) {
                final Resource unionClass = entry.getKey();
                for (Resource[] node = nodes.get(entry.getValue()); node != null
                        && node[0] != null; node = nodes.get(node[1])) {
                    this.emit(node[0], RDFS.SUBCLASSOF, unionClass);
                }
            }

            // C owl:intersectionOf list(Ci) -> C rdfs:subClassOf Ci
            for (final Map.Entry<Resource, Resource> entry : intersections.entrySet()) {
                final Resource intersectionClass = entry.getKey();
                for (Resource[] node = nodes.get(entry.getValue()); node != null
                        && node[0] != null; node = nodes.get(node[1])) {
                    this.emit(intersectionClass, RDFS.SUBCLASSOF, node[0]);
                }
            }
        }

        private void evalRules() {

            final Map<Resource, List<Resource>> superClasses //
                    = new HashMap<Resource, List<Resource>>();
            final Map<Resource, List<Resource>> subClasses //
                    = new HashMap<Resource, List<Resource>>();

            final Map<IRI, List<IRI>> superProperties = new HashMap<IRI, List<IRI>>();
            final Map<IRI, List<IRI>> subProperties = new HashMap<IRI, List<IRI>>();

            for (final Statement t : this.delta) {

                final Resource s = t.getSubject();
                final IRI p = t.getPredicate();
                final Value o = t.getObject();

                // RDFS1: "..."^^d => d rdf:type rdfs:Datatype
                if (this.ruleset.rdfs1 && o instanceof Literal) {
                    final Literal l = (Literal) o;
                    final IRI dt = l.getDatatype();
                    if (dt != null) {
                        this.emit(dt, RDF.TYPE, RDFS.DATATYPE);
                    }
                }

                // RDFS4A: s p o => s rdf:type rdfs:Resource
                if (this.ruleset.rdfs4a) {
                    this.emit(s, RDF.TYPE, RDFS.RESOURCE);
                }

                // RDFS4B: s p o => o rdf:type rdfs:Resource
                if (this.ruleset.rdfs4b && o instanceof Resource) {
                    this.emit((Resource) o, RDF.TYPE, RDFS.RESOURCE);
                }

                // RDFD2: s p o => p rdf:type rdf:Property
                if (this.ruleset.rdfD2) {
                    this.emit(p, RDF.TYPE, RDF.PROPERTY);
                }

                if (p == RDF.TYPE) {
                    if (o == RDFS.CLASS) {
                        // RDFS8: c rdf:type rdfs:Class => c rdfs:subClassOf rdfs:Resource
                        if (this.ruleset.rdfs8) {
                            this.emit(s, RDFS.SUBCLASSOF, RDFS.RESOURCE);
                        }
                        // RDFS10: c rdf:type rdfs:Class => c rdfs:subClassOf c
                        if (this.ruleset.rdfs10) {
                            this.emit(s, RDFS.SUBCLASSOF, s);
                        }
                    } else if (o == RDF.PROPERTY) {
                        // RDFS6: p rdf:type rdf:Property => p rdfs:subPropertyOf p
                        if (this.ruleset.rdfs6) {
                            this.emit(s, RDFS.SUBPROPERTYOF, s);
                        }
                    } else if (o == RDFS.DATATYPE) {
                        // RDFS13: d rdf:type rdfs:Datatype => d rdfs:subClassOf rdfs:Literal
                        if (this.ruleset.rdfs13) {
                            this.emit(s, RDFS.SUBCLASSOF, RDFS.LITERAL);
                        }
                    } else if (o == RDFS.CONTAINERMEMBERSHIPPROPERTY) {
                        // RDFS12: p rdf:type rdfs:CMP => p rdfs:subPropertyOf rdfs:member
                        if (this.ruleset.rdfs12) {
                            this.emit(s, RDFS.SUBPROPERTYOF, RDFS.MEMBER);
                        }
                    }
                }

                // RDFS2: p rdfs:domain c ^ s p o => s rdf:type c
                if (this.ruleset.rdfs2) {
                    if (p == RDFS.DOMAIN && s instanceof IRI && o instanceof Resource) {
                        for (final Statement t2 : this.db.filter(null, (IRI) s, null)) {
                            this.emit(t2.getSubject(), RDF.TYPE, o);
                        }
                    }
                    for (final Statement t2 : this.db.filter(p, RDFS.DOMAIN, null)) {
                        if (t2.getObject() instanceof Resource) {
                            this.emit(s, RDF.TYPE, t2.getObject());
                        }
                    }
                }

                // RDFS3: p rdfs:range c ^ x p y => y rdf:type c
                if (this.ruleset.rdfs3) {
                    if (p == RDFS.RANGE && s instanceof IRI && o instanceof Resource) {
                        for (final Statement t2 : this.db.filter(null, (IRI) s, null)) {
                            if (t2.getObject() instanceof Resource) {
                                this.emit((Resource) t2.getObject(), RDF.TYPE, o);
                            }
                        }
                    }
                    if (o instanceof Resource) {
                        for (final Statement t2 : this.db.filter(p, RDFS.RANGE, null)) {
                            if (t2.getObject() instanceof Resource) {
                                this.emit((Resource) o, RDF.TYPE, t2.getObject());
                            }
                        }
                    }
                }

                // RDFS9: c1 rdfs:subClassOf c2 ^ s rdf:type c1 => s rdf:type c2
                // RDFS11: c1 rdfs:subClassOf c2 ^ c2 rdfs:subClassOf c3 => c1 rdfs:subClassOf
                // c3
                if (p == RDFS.SUBCLASSOF && o instanceof Resource) {
                    final Resource c1 = s;
                    final Resource c2 = (Resource) o;
                    if (this.ruleset.rdfs11) {
                        for (final Resource c0 : this.match(subClasses, null, RDFS.SUBCLASSOF, c1,
                                Resource.class)) {
                            this.emit(c0, RDFS.SUBCLASSOF, c2);
                        }
                        for (final Resource c3 : this.match(superClasses, c2, RDFS.SUBCLASSOF,
                                null, Resource.class)) {
                            this.emit(c1, RDFS.SUBCLASSOF, c3);
                        }
                    }
                    if (this.ruleset.rdfs9) {
                        for (final Statement t2 : this.db.filter(null, RDF.TYPE, c1)) {
                            this.emit(t2.getSubject(), RDF.TYPE, c2);
                        }
                    }
                }
                if (this.ruleset.rdfs9 && p == RDF.TYPE && o instanceof Resource) {
                    for (final Statement t2 : this.db.filter((Resource) o, RDFS.SUBCLASSOF,
                            null)) {
                        if (t2.getObject() instanceof Resource) {
                            this.emit(s, RDF.TYPE, t2.getObject());
                        }
                    }
                }

                // RDFS7: p1 rdfs:subPropertyOf p2 ^ s p1 o => s p2 o
                // RDFS5: p1 rdfs:subPropertyOf p2 ^ p2 rdfs:subPropertyOf p3
                // => p1 rdfs:subPropertyOf p3
                if (p == RDFS.SUBPROPERTYOF && s instanceof IRI && o instanceof IRI) {
                    final IRI p1 = (IRI) s;
                    final IRI p2 = (IRI) o;
                    if (this.ruleset.rdfs5) {
                        for (final IRI p0 : this.match(subProperties, null, RDFS.SUBPROPERTYOF, p1,
                                IRI.class)) {
                            this.emit(p0, RDFS.SUBPROPERTYOF, p2);
                        }
                        for (final IRI p3 : this.match(superProperties, p2, RDFS.SUBPROPERTYOF,
                                null, IRI.class)) {
                            this.emit(p1, RDFS.SUBPROPERTYOF, p3);
                        }
                    }
                    if (this.ruleset.rdfs7) {
                        for (final Statement t2 : this.db.filter(null, p1, null)) {
                            this.emit(t2.getSubject(), p2, t2.getObject());
                        }
                    }
                }
                if (this.ruleset.rdfs7) {
                    for (final Statement t2 : this.db.filter(p, RDFS.SUBPROPERTYOF, null)) {
                        if (t2.getObject() instanceof IRI) {
                            this.emit(s, (IRI) t2.getObject(), o);
                        }
                    }
                }
            }
        }

        private <T> List<T> match(final Map<T, List<T>> map, @Nullable final Resource subject,
                @Nullable final IRI predicate, @Nullable final Value object,
                final Class<T> clazz) {

            final T key = clazz.cast(subject != null ? subject : object);
            List<T> list = map.get(key);
            if (list == null) {
                list = new ArrayList<T>();
                for (final Statement t : this.db.filter(subject, predicate, object)) {
                    if (subject == null && clazz.isInstance(t.getSubject())) {
                        list.add(clazz.cast(t.getSubject()));
                    }
                    if (object == null && clazz.isInstance(t.getObject())) {
                        list.add(clazz.cast(t.getObject()));
                    }
                }
                map.put(key, list);
            }
            return list;
        }

        private void emit(final Resource subject, final IRI predicate, final Value object) {
            this.db.add(subject, predicate, object);
        }

    }

    private static final class ABoxInferencer {

        private static final int STATEMENTS_PER_BUCKET = 4;

        private final RDFHandler handler;

        private final Ruleset ruleset;

        private final TBox tbox;

        private final Deduplicator deduplicator;

        private final boolean dropBNodeTypes;

        private Resource context;

        private long bitmask;

        private final Statement[] matrix;

        private final Set<Statement> set;

        private final List<Statement> emitted;

        ABoxInferencer(final RDFHandler handler, final Ruleset ruleset, final TBox tbox,
                final Deduplicator deduplicator, final boolean dropBNodesTypes) {
            this.handler = handler;
            this.ruleset = ruleset;
            this.tbox = tbox;
            this.deduplicator = deduplicator;
            this.dropBNodeTypes = dropBNodesTypes;
            this.matrix = new Statement[64 * ABoxInferencer.STATEMENTS_PER_BUCKET];
            this.set = new HashSet<Statement>();
            this.emitted = new ArrayList<Statement>();
        }

        void handleStatement(final Statement statement) throws RDFHandlerException {

            this.bitmask = 0L;
            this.context = statement.getContext();
            this.emitted.clear();
            if (!this.set.isEmpty()) {
                this.set.clear();
            }

            final Resource s = statement.getSubject();
            final IRI p = statement.getPredicate();
            final Value o = statement.getObject();

            Resource s2 = this.tbox.resources.get(s);
            if (s2 == null) {
                s2 = s;
            }

            IRI p2 = (IRI) this.tbox.resources.get(p);
            if (p2 == null) {
                p2 = s2 == s && p.equals(s) ? (IRI) s : p;
            }

            Value o2 = this.tbox.resources.get(o);
            if (o2 == null) {
                o2 = s2 == s && o.equals(s) ? s : p2 == p && o.equals(p) ? p : o;
            }

            int index = 0;
            this.emit(s2, p2, o2, false);
            while (index < this.emitted.size()) {
                final Statement t = this.emitted.get(index);
                this.infer(t.getSubject(), t.getPredicate(), t.getObject());
                ++index;
            }

            for (final Statement t : this.emitted) {
                final boolean emit = !this.dropBNodeTypes || t.getPredicate() != RDF.TYPE
                        || !(t.getObject() instanceof BNode);
                if (emit) {
                    this.handler.handleStatement(t);
                }
            }
        }

        private void infer(final Resource subject, final IRI predicate, final Value object) {

            if (this.ruleset.rdfs1 && object instanceof Literal) {
                final Literal l = (Literal) object;
                final IRI dt = l.getDatatype();
                if (dt != null) {
                    this.emit(dt, RDF.TYPE, RDFS.DATATYPE, true);
                }
            }

            if (this.ruleset.rdfs4a) {
                this.emit(subject, RDF.TYPE, RDFS.RESOURCE, false);
            }
            if (this.ruleset.rdfs4b && object instanceof Resource) {
                this.emit((Resource) object, RDF.TYPE, RDFS.RESOURCE, predicate == RDF.TYPE);
            }
            if (this.ruleset.rdfD2) {
                this.emit(predicate, RDF.TYPE, RDF.PROPERTY, true);
            }

            if (this.ruleset.rdfs2 || this.ruleset.rdfs3 || this.ruleset.rdfs7) {
                final TBox.Property p = this.tbox.properties.get(predicate);
                if (p != null) {
                    if (this.ruleset.rdfs2) {
                        for (final Resource c : p.domain) {
                            this.emit(subject, RDF.TYPE, c, false);
                        }
                    }
                    if (this.ruleset.rdfs3 && object instanceof Resource) {
                        for (final Resource c : p.range) {
                            this.emit((Resource) object, RDF.TYPE, c, false);
                        }
                    }
                    if (this.ruleset.rdfs7) {
                        for (final Resource q : p.parents) {
                            this.emit(subject, (IRI) q, object, false);
                        }
                    }
                }
            }

            if (predicate == RDF.TYPE) {
                if (object == RDFS.CLASS) {
                    if (this.ruleset.rdfs8) {
                        this.emit(subject, RDFS.SUBCLASSOF, RDFS.RESOURCE, true);
                    }
                    if (this.ruleset.rdfs10) {
                        this.emit(subject, RDFS.SUBCLASSOF, subject, true);
                    }
                } else if (object == RDF.PROPERTY) {
                    if (this.ruleset.rdfs6) {
                        this.emit(subject, RDFS.SUBPROPERTYOF, subject, true);
                    }
                } else if (object == RDFS.DATATYPE) {
                    if (this.ruleset.rdfs13) {
                        this.emit(subject, RDFS.SUBCLASSOF, RDFS.LITERAL, true);
                    }
                } else if (object == RDFS.CONTAINERMEMBERSHIPPROPERTY) {
                    if (this.ruleset.rdfs12) {
                        this.emit(subject, RDFS.SUBPROPERTYOF, RDFS.MEMBER, true);
                    }
                }

                if (this.ruleset.rdfs9) {
                    final TBox.Type t = this.tbox.types.get(object);
                    if (t != null) {
                        for (final Resource c : t.parents) {
                            this.emit(subject, RDF.TYPE, c, false);
                        }
                    }
                }
            }
        }

        private void emit(final Resource subject, final IRI predicate, final Value object,
                final boolean buffer) {

            final int hash = System.identityHashCode(subject) * 3323
                    + System.identityHashCode(predicate) * 661 + System.identityHashCode(object);

            final int index = hash & 0x3F;
            final long mask = 1L << index;
            final int offset = index * ABoxInferencer.STATEMENTS_PER_BUCKET;

            Statement statement = null;

            if ((this.bitmask & mask) == 0L) {
                statement = this.create(subject, predicate, object);
                this.bitmask = this.bitmask | mask;
                this.matrix[offset] = statement;
                this.matrix[offset + 1] = null;
            } else {
                final int last = offset + ABoxInferencer.STATEMENTS_PER_BUCKET;
                for (int i = offset; i < last; ++i) {
                    final Statement s = this.matrix[i];
                    if (s == null) {
                        statement = this.create(subject, predicate, object);
                        this.matrix[i] = statement;
                        final int next = i + 1;
                        if (next < last) {
                            this.matrix[next] = null;
                        }
                        break;
                    } else if (s.getSubject() == subject && s.getPredicate() == predicate
                            && s.getObject() == object) {
                        return; // duplicate
                    }
                }
                if (statement == null) {
                    final Statement s = this.create(subject, predicate, object);
                    if (this.set.add(s)) {
                        statement = s;
                    }
                }
            }

            if (statement != null && !this.deduplicator.add(statement, buffer)) {
                this.emitted.add(statement);
            }
        }

        private Statement create(final Resource subject, final IRI predicate, final Value object) {
            return this.context == null
                    ? Statements.VALUE_FACTORY.createStatement(subject, predicate, object)
                    : Statements.VALUE_FACTORY.createStatement(subject, predicate, object,
                            this.context);
        }

    }

    private static final class Deduplicator {

        private static final int RECENT_BUFFER_SIZE = 4 * 1024;

        private static final int LOCK_COUNT = 32;

        private final Map<Statement, Statement> mainBuffer;

        private final Statement[] recentBuffer;

        private final Object[] locks;

        Deduplicator() {
            this.mainBuffer = new ConcurrentHashMap<Statement, Statement>();
            this.recentBuffer = new Statement[Deduplicator.RECENT_BUFFER_SIZE];
            this.locks = new Object[Deduplicator.LOCK_COUNT];
            for (int i = 0; i < Deduplicator.LOCK_COUNT; ++i) {
                this.locks[i] = new Object();
            }
        }

        // TODO

        boolean add(final Statement statement, final boolean buffer) {

            if (buffer || ProcessorRDFS.VOC.containsKey(statement.getPredicate())
                    && ProcessorRDFS.VOC.containsKey(statement.getObject())
                    && ProcessorRDFS.VOC.containsKey(statement.getSubject())) {
                if (this.mainBuffer.put(statement, statement) != null) {
                    return true; // duplicate
                }
            }

            final int hash = statement.hashCode() & 0x7FFFFFFF;
            final int index = hash % Deduplicator.RECENT_BUFFER_SIZE;
            final Object lock = this.locks[hash % Deduplicator.LOCK_COUNT];
            synchronized (lock) {
                final Statement old = this.recentBuffer[index];
                if (old != null && old.equals(statement)) {
                    return true; // duplicate
                }
                this.recentBuffer[index] = statement;
                return false; // possibly not a duplicate
            }
        }

    }

    private static final class Ruleset {

        static final Ruleset DEFAULT = new Ruleset();

        final boolean rdfD2;

        final boolean rdfs1;

        final boolean rdfs2;

        final boolean rdfs3;

        final boolean rdfs4a;

        final boolean rdfs4b;

        final boolean rdfs5;

        final boolean rdfs6;

        final boolean rdfs7;

        final boolean rdfs8;

        final boolean rdfs9;

        final boolean rdfs10;

        final boolean rdfs11;

        final boolean rdfs12;

        final boolean rdfs13;

        Ruleset(final String... excludedRules) {

            final Set<String> set = new HashSet<String>();
            for (final String rule : excludedRules) {
                set.add(rule.trim().toLowerCase());
            }

            this.rdfD2 = !set.remove("rdfd2");
            this.rdfs1 = !set.remove("rdfs1");
            this.rdfs2 = !set.remove("rdfs2");
            this.rdfs3 = !set.remove("rdfs3");
            this.rdfs4a = !set.remove("rdfs4a");
            this.rdfs4b = !set.remove("rdfs4b");
            this.rdfs5 = !set.remove("rdfs5");
            this.rdfs6 = !set.remove("rdfs6");
            this.rdfs7 = !set.remove("rdfs7");
            this.rdfs8 = !set.remove("rdfs8");
            this.rdfs9 = !set.remove("rdfs9");
            this.rdfs10 = !set.remove("rdfs10");
            this.rdfs11 = !set.remove("rdfs11");
            this.rdfs12 = !set.remove("rdfs12");
            this.rdfs13 = !set.remove("rdfs13");

            if (!set.isEmpty()) {
                throw new IllegalArgumentException("Unknown rule(s): " + String.join(", ", set));
            }
        }

    }

    private static final class Database implements Iterable<Statement> {

        private final Map<Value, Node> nodes;

        private final Set<Triple> triples;

        private final Set<Triple> pending;

        Database() {
            this.nodes = new IdentityHashMap<Value, Node>();
            this.triples = new HashSet<Triple>();
            this.pending = new HashSet<Triple>();
        }

        public void add(final Resource subj, final IRI pred, final Value obj) {
            final Triple triple = new Triple(subj, pred, obj);
            if (!this.triples.contains(triple)) {
                this.pending.add(triple);
            }
        }

        public List<Statement> commit() {
            final List<Statement> result = new ArrayList<Statement>(this.pending.size());
            for (final Triple triple : this.pending) {
                result.add(triple.getStatement());
                this.triples.add(triple);
                final Node subjNode = this.nodeFor(triple.subj, true);
                triple.nextBySubj = subjNode.nextBySubj;
                subjNode.nextBySubj = triple;
                ++subjNode.numSubj;
                final Node predNode = this.nodeFor(triple.pred, true);
                triple.nextByPred = predNode.nextByPred;
                predNode.nextByPred = triple;
                ++predNode.numPred;
                final Node objNode = this.nodeFor(triple.obj, true);
                triple.nextByObj = objNode.nextByObj;
                objNode.nextByObj = triple;
                ++objNode.numObj;
            }
            this.pending.clear();
            return result;
        }

        public Iterable<Statement> filter(@Nullable final Resource subj, @Nullable final IRI pred,
                @Nullable final Value obj) {

            Node node = null;
            Triple triple = null;
            int field = -1;
            int num = Integer.MAX_VALUE;

            if (subj != null) {
                final Node n = this.nodeFor(subj, false);
                if (n == null) {
                    return Collections.emptyList();
                }
                if (n.numSubj < num) {
                    node = n;
                    triple = n.nextBySubj;
                    field = 0;
                    num = n.numSubj;
                }
            }

            if (pred != null) {
                final Node n = this.nodeFor(pred, false);
                if (n == null) {
                    return Collections.emptyList();
                }
                if (n.numPred < num) {
                    node = n;
                    triple = n.nextByPred;
                    field = 1;
                    num = n.numPred;
                }
            }

            if (obj != null) {
                final Node n = this.nodeFor(obj, false);
                if (n == null) {
                    return Collections.emptyList();
                }
                if (n.numObj < num) {
                    node = n;
                    triple = n.nextByObj;
                    field = 2;
                    num = n.numObj;
                }
            }

            if (node == null) {
                return this;
            }

            final Triple t = triple;
            final int f = field;

            return new Iterable<Statement>() {

                @Override
                public Iterator<Statement> iterator() {
                    return new Iterator<Statement>() {

                        private Triple triple = t;

                        {
                            this.advance(false);
                        }

                        @Override
                        public boolean hasNext() {
                            return this.triple != null;
                        }

                        @Override
                        public Statement next() {
                            final Statement result = this.triple.getStatement();
                            this.advance(true);
                            return result;
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }

                        private void advance(boolean skipCurrent) {
                            while (this.triple != null) {
                                if (!skipCurrent && (subj == null || subj == this.triple.subj)
                                        && (pred == null || pred == this.triple.pred)
                                        && (obj == null || obj == this.triple.obj)) {
                                    return;
                                }
                                skipCurrent = false;
                                if (f == 0) {
                                    this.triple = this.triple.nextBySubj;
                                } else if (f == 1) {
                                    this.triple = this.triple.nextByPred;
                                } else {
                                    this.triple = this.triple.nextByObj;
                                }
                            }
                        }

                    };
                }

            };
        }

        @Override
        public Iterator<Statement> iterator() {
            final Iterator<Triple> iterator = this.triples.iterator();
            return new Iterator<Statement>() {

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Statement next() {
                    return iterator.next().getStatement();
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public int size() {
            return this.triples.size();
        }

        private Node nodeFor(final Value value, final boolean canCreate) {
            Node node = this.nodes.get(value);
            if (node == null && canCreate) {
                node = new Node();
                this.nodes.put(value, node);
            }
            return node;
        }

        private static final class Node {

            @Nullable
            Triple nextBySubj;

            @Nullable
            Triple nextByPred;

            @Nullable
            Triple nextByObj;

            int numSubj;

            int numPred;

            int numObj;

        }

        private static final class Triple {

            Resource subj;

            IRI pred;

            Value obj;

            @Nullable
            Statement statement;

            @Nullable
            Triple nextBySubj;

            @Nullable
            Triple nextByPred;

            @Nullable
            Triple nextByObj;

            Triple(final Resource subj, final IRI pred, final Value obj) {
                this.subj = subj;
                this.pred = pred;
                this.obj = obj;
            }

            public Statement getStatement() {
                if (this.statement == null) {
                    this.statement = Statements.VALUE_FACTORY.createStatement(this.subj, this.pred,
                            this.obj);
                }
                return this.statement;
            }

            @Override
            public boolean equals(final Object object) {
                if (object == this) {
                    return true;
                }
                if (!(object instanceof Triple)) {
                    return false;
                }
                final Triple other = (Triple) object;
                return this.subj == other.subj && this.pred == other.pred && this.obj == other.obj;
            }

            @Override
            public int hashCode() {
                return System.identityHashCode(this.subj) * 757
                        + System.identityHashCode(this.pred) * 37
                        + System.identityHashCode(this.obj);
            }

        }

    }

}
