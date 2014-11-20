/*
 * RDFpro - An extensible tool for building stream-oriented RDF processing libraries.
 * 
 * Written in 2014 by Francesco Corcoglioniti <francesco.corcoglioniti@gmail.com> with support by
 * Marco Rospocher, Marco Amadori and Michele Mostarda.
 * 
 * To the extent possible under law, the author has dedicated all copyright and related and
 * neighboring rights to this software to the public domain worldwide. This software is
 * distributed without any warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication along with this software.
 * If not, see <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package eu.fbk.rdfpro.base;

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

import javax.annotation.Nullable;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SESAME;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Handlers;
import eu.fbk.rdfpro.util.Handlers.HandlerWrapper;
import eu.fbk.rdfpro.util.Options;
import eu.fbk.rdfpro.util.Statements;

public final class RdfsProcessor extends RDFProcessor {

    private static final Map<URI, URI> VOC;

    static {
        VOC = new IdentityHashMap<URI, URI>();
        for (final URI uri : new URI[] { RDF.TYPE, RDF.PROPERTY, RDF.XMLLITERAL, RDF.SUBJECT,
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
            VOC.put(uri, uri);
        }
    }

    private final TBox tbox;

    @Nullable
    private final boolean emitTbox;

    private final Ruleset ruleset;

    static RdfsProcessor doCreate(final String... args) {
        final Options options = Options.parse("d|r!|C|c!|b!|w|+", args);

        final List<Statement> tbox = new ArrayList<Statement>();
        try {
            final String base = options.getOptionArg("b", String.class);
            final boolean rewriteBNodes = options.hasOption("w");
            final String[] fileSpecs = options.getPositionalArgs(String.class).toArray(
                    new String[0]);
            final RDFHandler handler = new ReadProcessor(rewriteBNodes, base, fileSpecs)
                    .getHandler(Handlers.collect(tbox, true));
            handler.startRDF();
            handler.endRDF();
        } catch (final RDFHandlerException ex) {
            throw new IllegalArgumentException("Cannot load TBox data: " + ex.getMessage(), ex);
        }
        final boolean decomposeOWLAxioms = options.hasOption("d");

        String[] rules = new String[0];
        if (options.hasOption("r")) {
            rules = options.getOptionArg("r", String.class).split(",");
        }

        URI context = null;
        if (options.hasOption("C")) {
            context = SESAME.NIL;
        } else if (options.hasOption("c")) {
            context = options.getOptionArg("c", URI.class);
        }

        return new RdfsProcessor(tbox, context, decomposeOWLAxioms, rules);
    }

    // empty rule array = all rules; use SESAME.NIL to emit in default context

    public RdfsProcessor(final Iterable<? extends Statement> tbox,
            @Nullable final Resource tboxContext, final boolean decomposeOWLAxioms,
            final String... enabledRules) {

        final Ruleset ruleset = enabledRules == null || enabledRules.length == 0 ? Ruleset.DEFAULT
                : new Ruleset(enabledRules);

        final Map<Value, Value> interner = new HashMap<Value, Value>();
        for (final URI uri : VOC.keySet()) {
            interner.put(uri, uri);
        }

        final Database database = new Database();
        for (final Statement t : tbox) {
            Resource s = (Resource) interner.get(t.getSubject());
            if (s == null) {
                s = t.getSubject();
                interner.put(s, s);
            }
            URI p = (URI) interner.get(t.getPredicate());
            if (p == null) {
                p = t.getPredicate();
                interner.put(p, p);
            }
            Value o = interner.get(t.getObject());
            if (o == null) {
                o = t.getObject();
                interner.put(o, o);
            }
            database.add(s, p, o);
        }
        database.commit();

        new TBoxInferencer(decomposeOWLAxioms, ruleset, database).infer();

        this.tbox = new TBox(database, SESAME.NIL.equals(tboxContext) ? null : tboxContext);
        this.emitTbox = tboxContext != null;
        this.ruleset = ruleset;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        final RDFHandler sink = handler != null ? handler : Handlers.nop();
        return new Handler(sink, this.ruleset, this.tbox, this.emitTbox);
    }

    private static final class Handler extends HandlerWrapper {

        private final Ruleset ruleset;

        private final TBox tbox;

        private final boolean emitTBox;

        private final Deduplicator deduplicator;

        private ThreadLocal<ABoxInferencer> inferencer;

        Handler(final RDFHandler handler, final Ruleset ruleset, final TBox tbox,
                final boolean emitTBox) {

            super(handler);

            this.ruleset = ruleset;
            this.tbox = tbox;
            this.emitTBox = emitTBox;
            this.deduplicator = new Deduplicator();
            this.inferencer = new ThreadLocal<ABoxInferencer>() {

                @Override
                protected ABoxInferencer initialValue() {
                    return new ABoxInferencer(Handler.this.handler, Handler.this.ruleset,
                            Handler.this.tbox, Handler.this.deduplicator);
                }

            };
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {
            this.inferencer.get().handleStatement(statement);
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (this.emitTBox) {
                for (final Statement statement : this.tbox.statements) {
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
                final URI p = statement.getPredicate();
                final Value o = statement.getObject();

                statementArray[index++] = Objects.equals(context, statement.getContext()) ? statement
                        : context == null ? Statements.VALUE_FACTORY.createStatement(s, p, o)
                                : Statements.VALUE_FACTORY.createStatement(s, p, o, context);

                resources.put(s, s);
                resources.put(p, p);
                if (o instanceof Resource) {
                    resources.put((Resource) o, (Resource) o);
                }

                if (o instanceof Resource
                        && (p.equals(RDFS.SUBCLASSOF) || p.equals(RDFS.DOMAIN)
                                || p.equals(RDFS.RANGE) || p.equals(RDFS.SUBPROPERTYOF)
                                && o instanceof URI)) {
                    attributes.add(statement);
                }
            }

            Collections.sort(attributes, Sorter.INSTANCE);

            final int length = attributes.size();
            Resource subject = null;
            Resource[] parents = EMPTY;
            Resource[] domain = EMPTY;
            Resource[] range = EMPTY;
            boolean property = false;

            int i = 0;
            while (i < length) {
                final Statement t = attributes.get(i);
                final Resource s = t.getSubject();
                final URI p = t.getPredicate();

                if (s != subject) {
                    if (subject != null) {
                        if (property) {
                            properties.put(subject, new Property(parents, domain, range));
                        } else {
                            types.put(subject, new Type(parents));
                        }
                    }
                    subject = s;
                    parents = EMPTY;
                    domain = EMPTY;
                    range = EMPTY;
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

            addAxioms();
            if (this.decomposeOWLAxioms) {
                decomposeOWLAxioms();
            }
            this.db.commit();

            this.delta = this.db;
            while (true) {
                evalRules();
                final List<Statement> added = this.db.commit();
                if (added.isEmpty()) {
                    break;
                }
                this.delta = added;
            }
        }

        private void addAxioms() {

            emit(RDF.TYPE, RDFS.DOMAIN, RDFS.RESOURCE);
            emit(RDFS.DOMAIN, RDFS.DOMAIN, RDF.PROPERTY);
            emit(RDFS.RANGE, RDFS.DOMAIN, RDF.PROPERTY);
            emit(RDFS.SUBPROPERTYOF, RDFS.DOMAIN, RDF.PROPERTY);
            emit(RDFS.SUBCLASSOF, RDFS.DOMAIN, RDFS.CLASS);
            emit(RDF.SUBJECT, RDFS.DOMAIN, RDF.STATEMENT);
            emit(RDF.PREDICATE, RDFS.DOMAIN, RDF.STATEMENT);
            emit(RDF.OBJECT, RDFS.DOMAIN, RDF.STATEMENT);
            emit(RDFS.MEMBER, RDFS.DOMAIN, RDFS.RESOURCE);
            emit(RDF.FIRST, RDFS.DOMAIN, RDF.LIST);
            emit(RDF.REST, RDFS.DOMAIN, RDF.LIST);
            emit(RDFS.SEEALSO, RDFS.DOMAIN, RDFS.RESOURCE);
            emit(RDFS.ISDEFINEDBY, RDFS.DOMAIN, RDFS.RESOURCE);
            emit(RDFS.COMMENT, RDFS.DOMAIN, RDFS.RESOURCE);
            emit(RDFS.LABEL, RDFS.DOMAIN, RDFS.RESOURCE);
            emit(RDF.VALUE, RDFS.DOMAIN, RDFS.RESOURCE);

            emit(RDF.TYPE, RDFS.RANGE, RDFS.CLASS);
            emit(RDFS.DOMAIN, RDFS.RANGE, RDFS.CLASS);
            emit(RDFS.RANGE, RDFS.RANGE, RDFS.CLASS);
            emit(RDFS.SUBPROPERTYOF, RDFS.RANGE, RDF.PROPERTY);
            emit(RDFS.SUBCLASSOF, RDFS.RANGE, RDFS.CLASS);
            emit(RDF.SUBJECT, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDF.PREDICATE, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDF.OBJECT, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDFS.MEMBER, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDF.FIRST, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDF.REST, RDFS.RANGE, RDF.LIST);
            emit(RDFS.SEEALSO, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDFS.ISDEFINEDBY, RDFS.RANGE, RDFS.RESOURCE);
            emit(RDFS.COMMENT, RDFS.RANGE, RDFS.LITERAL);
            emit(RDFS.LABEL, RDFS.RANGE, RDFS.LITERAL);
            emit(RDF.VALUE, RDFS.RANGE, RDFS.RESOURCE);

            emit(RDF.ALT, RDFS.SUBCLASSOF, RDFS.CONTAINER);
            emit(RDF.BAG, RDFS.SUBCLASSOF, RDFS.CONTAINER);
            emit(RDF.SEQ, RDFS.SUBCLASSOF, RDFS.CONTAINER);
            emit(RDFS.CONTAINERMEMBERSHIPPROPERTY, RDFS.SUBCLASSOF, RDF.PROPERTY);

            emit(RDFS.ISDEFINEDBY, RDFS.SUBPROPERTYOF, RDFS.SEEALSO);
            emit(RDFS.DATATYPE, RDFS.SUBCLASSOF, RDFS.CLASS);
        }

        private void decomposeOWLAxioms() {

            final Map<URI, List<Resource>> subprops = new HashMap<URI, List<Resource>>();
            final Map<URI, List<Resource>> domains = new HashMap<URI, List<Resource>>();
            final Map<URI, List<Resource>> ranges = new HashMap<URI, List<Resource>>();
            final List<URI[]> inverses = new ArrayList<URI[]>();

            final Map<Resource, Resource[]> nodes = new HashMap<Resource, Resource[]>();
            final Map<Resource, Resource> intersections = new HashMap<Resource, Resource>();
            final Map<Resource, Resource> unions = new HashMap<Resource, Resource>();

            for (final Statement t : this.db) {

                final Resource s = t.getSubject();
                final URI p = t.getPredicate();
                final Value o = t.getObject();

                if (p == RDF.TYPE) {
                    if (o == OWL.CLASS || o == OWL.RESTRICTION) {
                        emit(s, RDF.TYPE, RDFS.CLASS);
                    } else if (o == OWL.ANNOTATIONPROPERTY || o == OWL.DATATYPEPROPERTY
                            || o == OWL.OBJECTPROPERTY) {
                        emit(s, RDF.TYPE, RDF.PROPERTY);
                    }

                } else if (p == OWL.EQUIVALENTCLASS) {
                    if (o instanceof Resource) {
                        emit(s, RDFS.SUBCLASSOF, o);
                        emit((Resource) o, RDFS.SUBCLASSOF, s);
                    }

                } else if (p == OWL.EQUIVALENTPROPERTY) {
                    if (s instanceof URI && o instanceof URI) {
                        emit(s, RDFS.SUBPROPERTYOF, o);
                        emit((URI) o, RDFS.SUBPROPERTYOF, s);
                    }

                } else if (p == RDFS.DOMAIN || p == RDFS.RANGE || p == RDFS.SUBPROPERTYOF) {
                    if (s instanceof URI
                            && (o instanceof URI || o instanceof Resource
                                    && p != RDFS.SUBPROPERTYOF)) {
                        final Map<URI, List<Resource>> map = p == RDFS.DOMAIN ? domains
                                : p == RDFS.RANGE ? ranges : subprops;
                        List<Resource> list = map.get(s);
                        if (list == null) {
                            list = new ArrayList<Resource>();
                            map.put((URI) s, list);
                        }
                        list.add((Resource) o);
                    }

                } else if (p == OWL.INVERSEOF) {
                    if (s instanceof URI && o instanceof URI) {
                        inverses.add(new URI[] { (URI) s, (URI) o });
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
            for (final URI[] pair : inverses) {
                for (int i = 0; i < 2; ++i) {
                    final URI property = pair[i];
                    final Set<URI> others = new HashSet<URI>();
                    others.add(pair[1 - i]);
                    boolean changed;
                    do {
                        changed = false;
                        for (final URI other : others.toArray(new URI[others.size()])) {
                            final List<Resource> parents = subprops.get(other);
                            if (parents != null) {
                                for (final Resource parent : parents) {
                                    if (others.add((URI) parent)) {
                                        changed = true;
                                    }
                                }
                            }
                        }
                    } while (changed);
                    for (final URI other : others) {
                        final List<Resource> otherDomains = domains.get(other);
                        if (otherDomains != null) {
                            for (final Resource domain : otherDomains) {
                                emit(property, RDFS.RANGE, domain);
                            }
                        }
                        final List<Resource> otherRanges = ranges.get(other);
                        if (otherRanges != null) {
                            for (final Resource range : otherRanges) {
                                emit(property, RDFS.DOMAIN, range);
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
                    emit(node[0], RDFS.SUBCLASSOF, unionClass);
                }
            }

            // C owl:intersectionOf list(Ci) -> C rdfs:subClassOf Ci
            for (final Map.Entry<Resource, Resource> entry : unions.entrySet()) {
                final Resource intersectionClass = entry.getKey();
                for (Resource[] node = nodes.get(entry.getValue()); node != null
                        && node[0] != null; node = nodes.get(node[1])) {
                    emit(intersectionClass, RDFS.SUBCLASSOF, node[0]);
                }
            }
        }

        private void evalRules() {

            final Map<Resource, List<Resource>> superClasses //
            = new HashMap<Resource, List<Resource>>();
            final Map<Resource, List<Resource>> subClasses //
            = new HashMap<Resource, List<Resource>>();

            final Map<URI, List<URI>> superProperties = new HashMap<URI, List<URI>>();
            final Map<URI, List<URI>> subProperties = new HashMap<URI, List<URI>>();

            for (final Statement t : this.delta) {

                final Resource s = t.getSubject();
                final URI p = t.getPredicate();
                final Value o = t.getObject();

                // RDFS1: "..."^^d => d rdf:type rdfs:Datatype
                if (this.ruleset.rdfs1 && o instanceof Literal) {
                    final Literal l = (Literal) o;
                    final URI dt = l.getDatatype();
                    if (dt != null) {
                        emit(dt, RDF.TYPE, RDFS.DATATYPE);
                    }
                }

                // RDFS4A: s p o => s rdf:type rdfs:Resource
                if (this.ruleset.rdfs4a) {
                    emit(s, RDF.TYPE, RDFS.RESOURCE);
                }

                // RDFS4B: s p o => o rdf:type rdfs:Resource
                if (this.ruleset.rdfs4b && o instanceof Resource) {
                    emit((Resource) o, RDF.TYPE, RDFS.RESOURCE);
                }

                // RDFD2: s p o => p rdf:type rdf:Property
                if (this.ruleset.rdfD2) {
                    emit(p, RDF.TYPE, RDF.PROPERTY);
                }

                if (p == RDF.TYPE) {
                    if (o == RDFS.CLASS) {
                        // RDFS8: c rdf:type rdfs:Class => c rdfs:subClassOf rdfs:Resource
                        if (this.ruleset.rdfs8) {
                            emit(s, RDFS.SUBCLASSOF, RDFS.RESOURCE);
                        }
                        // RDFS10: c rdf:type rdfs:Class => c rdfs:subClassOf c
                        if (this.ruleset.rdfs10) {
                            emit(s, RDFS.SUBCLASSOF, s);
                        }
                    } else if (o == RDF.PROPERTY) {
                        // RDFS6: p rdf:type rdf:Property => p rdfs:subPropertyOf p
                        if (this.ruleset.rdfs6) {
                            emit(s, RDFS.SUBPROPERTYOF, s);
                        }
                    } else if (o == RDFS.DATATYPE) {
                        // RDFS13: d rdf:type rdfs:Datatype => d rdfs:subClassOf rdfs:Literal
                        if (this.ruleset.rdfs13) {
                            emit(s, RDFS.SUBCLASSOF, RDFS.LITERAL);
                        }
                    } else if (o == RDFS.CONTAINERMEMBERSHIPPROPERTY) {
                        // RDFS12: p rdf:type rdfs:CMP => p rdfs:subPropertyOf rdfs:member
                        if (this.ruleset.rdfs12) {
                            emit(s, RDFS.SUBPROPERTYOF, RDFS.MEMBER);
                        }
                    }
                }

                // RDFS2: p rdfs:domain c ^ s p o => s rdf:type c
                if (this.ruleset.rdfs2) {
                    if (p == RDFS.DOMAIN && s instanceof URI && o instanceof Resource) {
                        for (final Statement t2 : this.db.filter(null, (URI) s, null)) {
                            emit(t2.getSubject(), RDF.TYPE, o);
                        }
                    }
                    for (final Statement t2 : this.db.filter(p, RDFS.DOMAIN, null)) {
                        if (t2.getObject() instanceof Resource) {
                            emit(s, RDF.TYPE, t2.getObject());
                        }
                    }
                }

                // RDFS3: p rdfs:range c ^ x p y => y rdf:type c
                if (this.ruleset.rdfs3) {
                    if (p == RDFS.RANGE && s instanceof URI && o instanceof Resource) {
                        for (final Statement t2 : this.db.filter(null, (URI) s, null)) {
                            if (t2.getObject() instanceof Resource) {
                                emit((Resource) t2.getObject(), RDF.TYPE, o);
                            }
                        }
                    }
                    if (o instanceof Resource) {
                        for (final Statement t2 : this.db.filter(p, RDFS.RANGE, null)) {
                            if (t2.getObject() instanceof Resource) {
                                emit((Resource) o, RDF.TYPE, t2.getObject());
                            }
                        }
                    }
                }

                // RDFS9: c1 rdfs:subClassOf c2 ^ s rdf:type c1 => s rdf:type c2
                // RDFS11: c1 rdfs:subClassOf c2 ^ c2 rdfs:subClassOf c3 => c1 rdfs:subClassOf c3
                if (p == RDFS.SUBCLASSOF && o instanceof Resource) {
                    final Resource c1 = s;
                    final Resource c2 = (Resource) o;
                    if (this.ruleset.rdfs11) {
                        for (final Resource c0 : match(subClasses, null, RDFS.SUBCLASSOF, c1,
                                Resource.class)) {
                            emit(c0, RDFS.SUBCLASSOF, c2);
                        }
                        for (final Resource c3 : match(superClasses, c2, RDFS.SUBCLASSOF, null,
                                Resource.class)) {
                            emit(c1, RDFS.SUBCLASSOF, c3);
                        }
                    }
                    if (this.ruleset.rdfs9) {
                        for (final Statement t2 : this.db.filter(null, RDF.TYPE, c1)) {
                            emit(t2.getSubject(), RDF.TYPE, c2);
                        }
                    }
                }
                if (this.ruleset.rdfs9 && p == RDF.TYPE && o instanceof Resource) {
                    for (final Statement t2 : this.db.filter((Resource) o, RDFS.SUBCLASSOF, null)) {
                        if (t2.getObject() instanceof Resource) {
                            emit(s, RDF.TYPE, t2.getObject());
                        }
                    }
                }

                // RDFS7: p1 rdfs:subPropertyOf p2 ^ s p1 o => s p2 o
                // RDFS5: p1 rdfs:subPropertyOf p2 ^ p2 rdfs:subPropertyOf p3
                // => p1 rdfs:subPropertyOf p3
                if (p == RDFS.SUBPROPERTYOF && s instanceof URI && o instanceof URI) {
                    final URI p1 = (URI) s;
                    final URI p2 = (URI) o;
                    if (this.ruleset.rdfs5) {
                        for (final URI p0 : match(subProperties, null, RDFS.SUBPROPERTYOF, p1,
                                URI.class)) {
                            emit(p0, RDFS.SUBPROPERTYOF, p2);
                        }
                        for (final URI p3 : match(superProperties, p2, RDFS.SUBPROPERTYOF, null,
                                URI.class)) {
                            emit(p1, RDFS.SUBPROPERTYOF, p3);
                        }
                    }
                    if (this.ruleset.rdfs7) {
                        for (final Statement t2 : this.db.filter(null, p1, null)) {
                            emit(t2.getSubject(), p2, t2.getObject());
                        }
                    }
                }
                if (this.ruleset.rdfs7) {
                    for (final Statement t2 : this.db.filter(p, RDFS.SUBPROPERTYOF, null)) {
                        if (t2.getObject() instanceof URI) {
                            emit(s, (URI) t2.getObject(), o);
                        }
                    }
                }
            }
        }

        private <T> List<T> match(final Map<T, List<T>> map, @Nullable final Resource subject,
                @Nullable final URI predicate, @Nullable final Value object, final Class<T> clazz) {

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

        private void emit(final Resource subject, final URI predicate, final Value object) {
            this.db.add(subject, predicate, object);
        }

    }

    private static final class ABoxInferencer {

        private static final int STATEMENTS_PER_BUCKET = 4;

        private final RDFHandler handler;

        private final Ruleset ruleset;

        private final TBox tbox;

        private final Deduplicator deduplicator;

        private Resource context;

        private long bitmask;

        private final Statement[] matrix;

        private final Set<Statement> set;

        private final List<Statement> emitted;

        ABoxInferencer(final RDFHandler handler, final Ruleset ruleset, final TBox tbox,
                final Deduplicator deduplicator) {
            this.handler = handler;
            this.ruleset = ruleset;
            this.tbox = tbox;
            this.deduplicator = deduplicator;
            this.matrix = new Statement[64 * STATEMENTS_PER_BUCKET];
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
            final URI p = statement.getPredicate();
            final Value o = statement.getObject();

            Resource s2 = this.tbox.resources.get(s);
            if (s2 == null) {
                s2 = s;
            }

            URI p2 = (URI) this.tbox.resources.get(p);
            if (p2 == null) {
                p2 = s2 == s && p.equals(s) ? (URI) s : p;
            }

            Value o2 = this.tbox.resources.get(o);
            if (o2 == null) {
                o2 = s2 == s && o.equals(s) ? s : p2 == p && o.equals(p) ? p : o;
            }

            int index = 0;
            emit(s2, p2, o2, false);
            while (index < this.emitted.size()) {
                final Statement t = this.emitted.get(index);
                infer(t.getSubject(), t.getPredicate(), t.getObject());
                ++index;
            }

            for (final Statement t : this.emitted) {
                this.handler.handleStatement(t);
            }
        }

        private void infer(final Resource subject, final URI predicate, final Value object) {

            if (this.ruleset.rdfs1 && object instanceof Literal) {
                final Literal l = (Literal) object;
                final URI dt = l.getDatatype();
                if (dt != null) {
                    emit(dt, RDF.TYPE, RDFS.DATATYPE, true);
                }
            }

            if (this.ruleset.rdfs4a) {
                emit(subject, RDF.TYPE, RDFS.RESOURCE, false);
            }
            if (this.ruleset.rdfs4b && object instanceof Resource) {
                emit((Resource) object, RDF.TYPE, RDFS.RESOURCE, predicate == RDF.TYPE);
            }
            if (this.ruleset.rdfD2) {
                emit(predicate, RDF.TYPE, RDF.PROPERTY, true);
            }

            if (this.ruleset.rdfs2 || this.ruleset.rdfs3 || this.ruleset.rdfs7) {
                final TBox.Property p = this.tbox.properties.get(predicate);
                if (p != null) {
                    if (this.ruleset.rdfs2) {
                        for (final Resource c : p.domain) {
                            emit(subject, RDF.TYPE, c, false);
                        }
                    }
                    if (this.ruleset.rdfs3 && object instanceof Resource) {
                        for (final Resource c : p.range) {
                            emit((Resource) object, RDF.TYPE, c, false);
                        }
                    }
                    if (this.ruleset.rdfs7) {
                        for (final Resource q : p.parents) {
                            emit(subject, (URI) q, object, false);
                        }
                    }
                }
            }

            if (predicate == RDF.TYPE) {
                if (object == RDFS.CLASS) {
                    if (this.ruleset.rdfs8) {
                        emit(subject, RDFS.SUBCLASSOF, RDFS.RESOURCE, true);
                    }
                    if (this.ruleset.rdfs10) {
                        emit(subject, RDFS.SUBCLASSOF, subject, true);
                    }
                } else if (object == RDF.PROPERTY) {
                    if (this.ruleset.rdfs6) {
                        emit(subject, RDFS.SUBPROPERTYOF, subject, true);
                    }
                } else if (object == RDFS.DATATYPE) {
                    if (this.ruleset.rdfs13) {
                        emit(subject, RDFS.SUBCLASSOF, RDFS.LITERAL, true);
                    }
                } else if (object == RDFS.CONTAINERMEMBERSHIPPROPERTY) {
                    if (this.ruleset.rdfs12) {
                        emit(subject, RDFS.SUBPROPERTYOF, RDFS.MEMBER, true);
                    }
                }

                if (this.ruleset.rdfs9) {
                    final TBox.Type t = this.tbox.types.get(object);
                    if (t != null) {
                        for (final Resource c : t.parents) {
                            emit(subject, RDF.TYPE, c, false);
                        }
                    }
                }
            }
        }

        private void emit(final Resource subject, final URI predicate, final Value object,
                final boolean buffer) {

            final int hash = System.identityHashCode(subject) * 3323
                    + System.identityHashCode(predicate) * 661 + System.identityHashCode(object);

            final int index = hash & 0x3F;
            final long mask = 1L << index;
            final int offset = index * STATEMENTS_PER_BUCKET;

            Statement statement = null;

            if ((this.bitmask & mask) == 0L) {
                statement = create(subject, predicate, object);
                this.bitmask = this.bitmask | mask;
                this.matrix[offset] = statement;
                this.matrix[offset + 1] = null;
            } else {
                final int last = offset + STATEMENTS_PER_BUCKET;
                for (int i = offset; i < last; ++i) {
                    final Statement s = this.matrix[i];
                    if (s == null) {
                        statement = create(subject, predicate, object);
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
                    final Statement s = create(subject, predicate, object);
                    if (this.set.add(s)) {
                        statement = s;
                    }
                }
            }

            if (statement != null && !this.deduplicator.add(statement, buffer)) {
                this.emitted.add(statement);
            }
        }

        private Statement create(final Resource subject, final URI predicate, final Value object) {
            return this.context == null ? Statements.VALUE_FACTORY.createStatement(subject,
                    predicate, object) : Statements.VALUE_FACTORY.createStatement(subject,
                    predicate, object, this.context);
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
            this.recentBuffer = new Statement[RECENT_BUFFER_SIZE];
            this.locks = new Object[LOCK_COUNT];
            for (int i = 0; i < LOCK_COUNT; ++i) {
                this.locks[i] = new Object();
            }
        }

        boolean add(final Statement statement, final boolean buffer) {

            if (buffer || VOC.containsKey(statement.getPredicate())
                    && VOC.containsKey(statement.getObject())
                    && VOC.containsKey(statement.getSubject())) {
                if (this.mainBuffer.put(statement, statement) != null) {
                    return true; // duplicate
                }
            }

            final int hash = statement.hashCode() & 0x7FFFFFFF;
            final int index = hash % RECENT_BUFFER_SIZE;
            final Object lock = this.locks[hash % LOCK_COUNT];
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

        static final Ruleset DEFAULT = new Ruleset("rdfd2", "rdfs1", "rdfs2", "rdfs3", "rdfs4a",
                "rdfs4b", "rdfs5", "rdfs6", "rdfs7", "rdfs8", "rdfs9", "rdfs10", "rdfs11",
                "rdfs12", "rdfs13");

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

        Ruleset(final String... rules) {

            final Set<String> set = new HashSet<String>();
            for (final String rule : rules) {
                set.add(rule.trim().toLowerCase());
            }

            this.rdfD2 = set.remove("rdfd2");
            this.rdfs1 = set.remove("rdfs1");
            this.rdfs2 = set.remove("rdfs2");
            this.rdfs3 = set.remove("rdfs3");
            this.rdfs4a = set.remove("rdfs4a");
            this.rdfs4b = set.remove("rdfs4b");
            this.rdfs5 = set.remove("rdfs5");
            this.rdfs6 = set.remove("rdfs6");
            this.rdfs7 = set.remove("rdfs7");
            this.rdfs8 = set.remove("rdfs8");
            this.rdfs9 = set.remove("rdfs9");
            this.rdfs10 = set.remove("rdfs10");
            this.rdfs11 = set.remove("rdfs11");
            this.rdfs12 = set.remove("rdfs12");
            this.rdfs13 = set.remove("rdfs13");

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

        public void add(final Resource subj, final URI pred, final Value obj) {
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
                final Node subjNode = nodeFor(triple.subj, true);
                triple.nextBySubj = subjNode.nextBySubj;
                subjNode.nextBySubj = triple;
                ++subjNode.numSubj;
                final Node predNode = nodeFor(triple.pred, true);
                triple.nextByPred = predNode.nextByPred;
                predNode.nextByPred = triple;
                ++predNode.numPred;
                final Node objNode = nodeFor(triple.obj, true);
                triple.nextByObj = objNode.nextByObj;
                objNode.nextByObj = triple;
                ++objNode.numObj;
            }
            this.pending.clear();
            return result;
        }

        public Iterable<Statement> filter(@Nullable final Resource subj, @Nullable final URI pred,
                @Nullable final Value obj) {

            Node node = null;
            Triple triple = null;
            int field = -1;
            int num = Integer.MAX_VALUE;

            if (subj != null) {
                final Node n = nodeFor(subj, false);
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
                final Node n = nodeFor(pred, false);
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
                final Node n = nodeFor(obj, false);
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
                            advance(false);
                        }

                        @Override
                        public boolean hasNext() {
                            return this.triple != null;
                        }

                        @Override
                        public Statement next() {
                            final Statement result = this.triple.getStatement();
                            advance(true);
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

            URI pred;

            Value obj;

            @Nullable
            Statement statement;

            @Nullable
            Triple nextBySubj;

            @Nullable
            Triple nextByPred;

            @Nullable
            Triple nextByObj;

            Triple(final Resource subj, final URI pred, final Value obj) {
                this.subj = subj;
                this.pred = pred;
                this.obj = obj;
            }

            public Statement getStatement() {
                if (this.statement == null) {
                    this.statement = Statements.VALUE_FACTORY.createStatement(this.subj,
                            this.pred, this.obj);
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
