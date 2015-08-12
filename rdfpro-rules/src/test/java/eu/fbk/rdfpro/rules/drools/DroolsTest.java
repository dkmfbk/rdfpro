package eu.fbk.rdfpro.rules.drools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.core.spi.KnowledgeHelper;
import org.kie.api.KieServices;
import org.kie.api.definition.type.Position;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SESAME;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.RDFSources;

public final class DroolsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DroolsTest.class);

    @SuppressWarnings("unchecked")
    public static void main(final String... args) {

        // args should be the list of filenames to process

        final List<Quad> quads = new ArrayList<>();
        for (final Statement stmt : RDFSources.read(false, true, null, null, args)) {
            final int ctx = stmt.getContext() != null && !stmt.getContext().equals(SESAME.NIL) ? Mapper
                    .map(stmt.getContext()) : 0;
                    quads.add(new Quad(Mapper.map(stmt.getSubject()), Mapper.map(stmt.getPredicate()),
                            Mapper.map(stmt.getObject()), ctx));
                    // final Resource ctx = stmt.getContext() != null ? stmt.getContext() : SESAME.NIL;
                    // quads.add(new Quad(stmt.getSubject(), stmt.getPredicate(), stmt.getObject(), ctx));
        }

        final KieServices kie = KieServices.Factory.get();
        final KieContainer container = kie.getKieClasspathContainer();

        final int n = 10;
        final long tss = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            final KieSession session = container.newKieSession();

            long ts = System.currentTimeMillis();
            session.execute(kie.getCommands().newInsertElements(quads));
            LOGGER.info("{} quads asserted in {} ms", session.getFactCount(),
                    System.currentTimeMillis() - ts);

            ts = System.currentTimeMillis();
            session.fireAllRules();
            session.fireAllRules();
            LOGGER.info("{} quads derived in {} ms", session.getFactCount(),
                    System.currentTimeMillis() - ts);

            session.dispose();
        }
        System.out.println((System.currentTimeMillis() - tss) / n);
    }

    public static final class Quad {

        @Position(0)
        private final int subject;

        @Position(1)
        private final int predicate;

        @Position(2)
        private final int object;

        @Position(3)
        private final int context;

        public Quad(final int subject, final int predicate, final int object) {
            this(subject, predicate, object, 0);
        }

        public Quad(final int subject, final int predicate, final int object, final int context) {
            this.subject = subject;
            this.predicate = predicate;
            this.object = object;
            this.context = context;
        }

        public int getSubject() {
            return this.subject;
        }

        public int getPredicate() {
            return this.predicate;
        }

        public int getObject() {
            return this.object;
        }

        public int getContext() {
            return this.context;
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
            return this.subject == other.subject && this.predicate == other.predicate
                    && this.object == other.object && this.context == other.context;
        }

        @Override
        public int hashCode() {
            return 7829 * this.subject + 1103 * this.predicate + 137 * this.object + this.context;
        }

        // @Position(0)
        // private final Resource subject;
        //
        // @Position(1)
        // private final URI predicate;
        //
        // @Position(2)
        // private final Value object;
        //
        // @Position(3)
        // private final Resource context;
        //
        // private final int hash;
        //
        // public Quad(final Value subject, final Value predicate, final Value object) {
        // this(subject, predicate, object, SESAME.NIL);
        // }
        //
        // public Quad(final Value subject, final Value predicate, final Value object, final Value
        // context) {
        // // int subjectHash;
        // // if (subject instanceof URI) {
        // // subjectHash = subject.stringValue().hashCode();
        // // this.subject = new URIImpl2(subject.stringValue());
        // // } else {
        // // subjectHash = subject.hashCode();
        // // this.subject = (Resource) subject;
        // // }
        // this.hash = 7829 * subject.hashCode() + 1103 * predicate.hashCode() + 137
        // * object.hashCode() + context.hashCode();
        // this.subject = (Resource) subject;
        // this.predicate = (URI) predicate;
        // this.object = object;
        // this.context = (Resource) context;
        // // System.out.println("Created " + this);
        // }
        //
        // public Resource getSubject() {
        // return this.subject;
        // }
        //
        // public URI getPredicate() {
        // return this.predicate;
        // }
        //
        // public Value getObject() {
        // return this.object;
        // }
        //
        // public Resource getContext() {
        // return this.context;
        // }
        //
        // @Override
        // public boolean equals(final Object object) {
        // if (object == this) {
        // return true;
        // }
        // if (!(object instanceof Quad)) {
        // return false;
        // }
        // final Quad other = (Quad) object;
        // return this.subject.equals(other.subject) && this.predicate.equals(other.predicate)
        // && this.object.equals(other.object) && this.context.equals(other.context);
        // }
        //
        // @Override
        // public int hashCode() {
        // return this.hash;
        // }

        @Override
        public String toString() {
            return "<" + this.subject + ", " + this.predicate + ", " + this.object + ", "
                    + this.context + ">";
        }

    }

    public static final class Mapper {

        private static Map<Value, Integer> map = new HashMap<>();

        private static int next = 20;

        static {
            map.put(RDF.TYPE, 1);
            map.put(RDF.PROPERTY, 2);
            map.put(RDFS.DOMAIN, 3);
            map.put(RDFS.RANGE, 4);
            map.put(RDFS.RESOURCE, 5);
            map.put(RDFS.SUBPROPERTYOF, 6);
            map.put(RDFS.CLASS, 7);
            map.put(RDFS.SUBCLASSOF, 8);
            map.put(RDFS.CONTAINERMEMBERSHIPPROPERTY, 9);
            map.put(RDFS.MEMBER, 10);
            map.put(RDFS.DATATYPE, 11);
            map.put(RDFS.LITERAL, 12);
        }

        public synchronized static int map(final Value value) {
            Integer result = map.get(value);
            if (result == null) {
                final int num = ++next;
                result = value instanceof Resource ? num : -num;
                map.put(value, result);
            }
            return result.intValue();
        }

    }

    public static final class Helper {

        // public static void ins(final Object drools, final Value s, final Value p, final Value
        // o,
        // final Value c) {
        // final KnowledgeHelper helper = (KnowledgeHelper) drools;
        // helper.insert(new Quad(s, p, o, c));
        // }

        public static void ins(final Object drools, final int s, final int p, final int o,
                final int c) {
            final KnowledgeHelper helper = (KnowledgeHelper) drools;
            helper.insert(new Quad(s, p, o, c));
        }

    }

    public static final class VOC {

        public static final int TYPE = Mapper.map(RDF.TYPE);

        public static final int PROPERTY = Mapper.map(RDF.PROPERTY);

        public static final int DOMAIN = Mapper.map(RDFS.DOMAIN);

        public static final int RANGE = Mapper.map(RDFS.RANGE);

        public static final int RESOURCE = Mapper.map(RDFS.RESOURCE);

        public static final int SUBPROPERTYOF = Mapper.map(RDFS.SUBPROPERTYOF);

        public static final int CLASS = Mapper.map(RDFS.CLASS);

        public static final int SUBCLASSOF = Mapper.map(RDFS.SUBCLASSOF);

        public static final int CONTAINERMEMBERSHIPPROPERTY = Mapper
                .map(RDFS.CONTAINERMEMBERSHIPPROPERTY);

        public static final int MEMBER = Mapper.map(RDFS.MEMBER);

        public static final int DATATYPE = Mapper.map(RDFS.DATATYPE);

        public static final int LITERAL = Mapper.map(RDFS.LITERAL);

        // public static final URI TYPE = RDF.TYPE;
        //
        // public static final URI PROPERTY = RDF.PROPERTY;
        //
        // public static final URI DOMAIN = RDFS.DOMAIN;
        //
        // public static final URI RANGE = RDFS.RANGE;
        //
        // public static final URI RESOURCE = RDFS.RESOURCE;
        //
        // public static final URI SUBPROPERTYOF = RDFS.SUBPROPERTYOF;
        //
        // public static final URI CLASS = RDFS.CLASS;
        //
        // public static final URI SUBCLASSOF = RDFS.SUBCLASSOF;
        //
        // public static final URI CONTAINERMEMBERSHIPPROPERTY =
        // RDFS.CONTAINERMEMBERSHIPPROPERTY;
        //
        // public static final URI MEMBER = RDFS.MEMBER;
        //
        // public static final URI DATATYPE = RDFS.DATATYPE;
        //
        // public static final URI LITERAL = RDFS.LITERAL;

    }

}
