package eu.fbk.rdfpro;

import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Hash;
import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

public class ProcessorPseudoClass implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorPseudoClass.class);

    private static final String DEFAULT_NAMESPACE = "pseudo:";

    private final String namespace;

    private final int minInstances;

    private final int maxClasses;

    ProcessorPseudoClass(@Nullable final String namespace, @Nullable final Integer minInstances,
            @Nullable final Integer maxClasses) {

        this.namespace = Strings.isNullOrEmpty(namespace) ? DEFAULT_NAMESPACE : namespace;
        this.minInstances = minInstances != null ? minInstances : 0;
        this.maxClasses = maxClasses != null ? maxClasses : Integer.MAX_VALUE;
    }

    @Override
    public int getExtraPasses() {
        return 1;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(handler);
    }

    private final class Handler extends AbstractRDFHandlerWrapper {

        private final Multimap<IRI, IRI> domains;

        private final Multimap<IRI, IRI> ranges;

        // TODO: use a more compact data structure
        
        private final List<Counter> counters;

        private final Map<Hash, PseudoClass> classes;

        // TODO: use 4 maps, one for each kind of pseudo-class, to reduce lock contention

        private long[] table;

        private int pass;

        Handler(final RDFHandler handler) {
            super(handler);
            this.domains = HashMultimap.create();
            this.ranges = HashMultimap.create();
            this.counters = Lists.newArrayList();
            this.classes = Maps.newConcurrentMap();
            this.table = new long[1024];
            this.pass = 0;
        }

        @Override
        public void startRDF() throws RDFHandlerException {

            // Increment the pass index
            this.pass++;

            // At the beginning of the second pass, select the pseudo-classes to emit
            if (this.pass == 2) {

                // First, select candidate classes according to 'minInstances' criterion
                List<Counter> candidates = Lists.newArrayList();
                for (final Counter counter : this.counters) {
                    if (counter.count >= ProcessorPseudoClass.this.minInstances) {
                        candidates.add(counter);
                    }
                }

                // Then, apply 'maxClasses' criterion
                if (candidates.size() > ProcessorPseudoClass.this.maxClasses) {
                    candidates.sort(Ordering.natural().reverse());
                    candidates = candidates.subList(0, ProcessorPseudoClass.this.maxClasses);
                }

                // Populate 'classes' map with remaining candidates
                for (final Counter candidate : candidates) {
                    this.classes.put(Hash.fromLongs(candidate.hashHi, candidate.hashLo),
                            new PseudoClass());
                }

                // Log selection
                LOGGER.debug("{}/{} pseudo-classes selected", this.classes.size(),
                        this.counters.size());
            }

            // At the beginning of the following passes (if any), reset the 'emitted' flags
            if (this.pass > 2) {
                for (final PseudoClass pc : this.classes.values()) {
                    pc.emitted = false;
                }
            }

            // Propagate only after first pass
            if (this.pass >= 2) {
                super.startRDF();
            }
        }

        @Override
        public void handleStatement(final Statement stmt) throws RDFHandlerException {

            // Extract subject, predicate, object and value of the statement
            final Resource s = stmt.getSubject();
            final IRI p = stmt.getPredicate();
            final Value o = stmt.getObject();
            final Resource c = stmt.getContext();

            // Handle differently first VS. following passes
            if (this.pass == 1) {

                // At first pass, count the instances of up to 4 pseudo-classes for the statement
                increment(p, null, false);
                if (o instanceof IRI) {
                    increment(p, (IRI) o, false);
                }
                if (o instanceof Resource) {
                    increment(p, null, true);
                    if (s instanceof IRI) {
                        increment(p, (IRI) s, true);
                    }
                }

                // Also keep track of all domain and range information
                final Multimap<IRI, IRI> map = p.equals(RDFS.DOMAIN) ? this.domains
                        : p.equals(RDFS.RANGE) ? this.ranges : null;
                if (map != null && s instanceof IRI && o instanceof IRI) {
                    synchronized (map) {
                        map.put((IRI) s, (IRI) o);
                    }
                }

            } else {

                // At following passes, emit TBox and ABox statements for up to 4 pseudo-classes
                emit(p, null, false, s, c);
                if (o instanceof IRI) {
                    emit(p, (IRI) o, false, s, c);
                }
                if (o instanceof Resource) {
                    emit(p, null, true, (Resource) o, c);
                    if (s instanceof IRI) {
                        emit(p, (IRI) s, true, (Resource) o, c);
                    }
                }

                // Also, emit the input statement unchanged
                super.handleStatement(stmt);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {

            // Propagate only after first pass
            if (this.pass >= 2) {
                super.handleNamespace(prefix, uri);
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {

            // Propagate only after first pass
            if (this.pass >= 2) {
                super.handleComment(comment);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {

            // Propagate only after first pass
            if (this.pass >= 2) {
                super.endRDF();
            }
        }

        void increment(final IRI property, @Nullable final IRI value, final boolean inverse) {

            // Compute hash and 31-bit key
            final Hash hash = hash(property, value, inverse);
            final long hi = hash.getHigh();
            final long lo = hash.getLow();
            final int key = (int) (hi & 0x7FFFFFFFL);

            // Lookup the counter for the hash, and increment its count
            synchronized (this) {
                int index = key % this.table.length;
                while (true) {
                    final long cell = this.table[index];
                    if (cell == 0L) {
                        final long offset = this.counters.size();
                        this.counters.add(new Counter(hi, lo));
                        this.table[index] = offset << 32 | 0x80000000L | key;
                        if (this.counters.size() > this.table.length / 2) {
                            rehash();
                        }
                        break;
                    } else {
                        final int cellKey = (int) (cell & 0x7FFFFFFFL);
                        if (key == cellKey) {
                            final int offset = (int) (cell >>> 32);
                            final Counter counter = this.counters.get(offset);
                            if (counter.hashHi == hi && counter.hashLo == lo) {
                                ++counter.count;
                                break;
                            }
                        }
                    }
                    index = (index + 1) % this.table.length;
                }
            }
        }

        void rehash() {
            this.table = new long[this.table.length * 2];
            final int numCounters = this.counters.size();
            for (int offset = 0; offset < numCounters; ++offset) {
                final Counter counter = this.counters.get(offset);
                final int key = (int) (counter.hashHi & 0x7FFFFFFFL);
                int index = key % this.table.length;
                while (true) {
                    if (this.table[index] == 0L) {
                        this.table[index] = (long) offset << 32 | 0x80000000L | key;
                        break;
                    }
                    index = (index + 1) % this.table.length;
                }
            }
        }

        void emit(final IRI property, @Nullable final IRI value, final boolean inverse,
                final Resource subject, @Nullable final Resource context) {

            // Compute the hash for the pseudo-class given by the property/value/inverse triple
            final Hash hash = hash(property, value, inverse);

            // Fetch the pseudo-class object for the hash
            final PseudoClass pc = this.classes.get(hash);
            if (pc == null) {
                return;
            }

            // Mint a IRI for the pseudo-class, if not done yet
            if (pc.iri == null) {
                pc.iri = mint(property, value, inverse);
            }

            // Emit TBox information about the pseudo-class, the first time it is encountered
            if (!pc.emitted) {
                synchronized (pc) {
                    if (!pc.emitted) {
                        emit(pc.iri, RDF.TYPE, OWL.CLASS, null);
                        emit(pc.iri, RDF.PREDICATE, property, null);
                        if (value != null) {
                            emit(pc.iri, inverse ? RDF.SUBJECT : RDF.OBJECT, value, null);
                        }
                        final PseudoClass parent = value == null ? null
                                : this.classes.get(hash(property, null, inverse));
                        if (parent != null) {
                            if (parent.iri == null) {
                                parent.iri = mint(property, null, inverse);
                            }
                            emit(pc.iri, RDFS.SUBCLASSOF, parent.iri, null);
                        } else {
                            for (final IRI p : (inverse ? this.ranges : this.domains)
                                    .get(property)) {
                                emit(pc.iri, RDFS.SUBCLASSOF, p, null);
                            }
                        }
                        pc.emitted = true;
                    }
                }
            }

            // Emit ABox information
            emit(subject, RDF.TYPE, pc.iri, context);
        }

        void emit(final Resource s, final IRI p, final IRI o, @Nullable final Resource c) {
            final ValueFactory vf = Statements.VALUE_FACTORY;
            this.handler.handleStatement(
                    c == null ? vf.createStatement(s, p, o) : vf.createStatement(s, p, o, c));
        }

        IRI mint(final IRI property, @Nullable final IRI value, final boolean inverse) {
            final StringBuilder sb = new StringBuilder(ProcessorPseudoClass.this.namespace);
            sb.append(inverse ? "is_" : "has_");
            mintHelper(sb, property);
            if (value != null) {
                sb.append(inverse ? "_of_" : "_");
                mintHelper(sb, value);
            }
            return Statements.VALUE_FACTORY.createIRI(sb.toString());
        }

        void mintHelper(final StringBuilder sb, final IRI iri) {
            final String ns = iri.getNamespace();
            String prefix = Namespaces.DEFAULT.prefixFor(ns);
            if (prefix == null) {
                prefix = Hash.murmur3(ns).toString().substring(0, 8);
            }
            sb.append(prefix);
            sb.append('.');
            sb.append(iri.getLocalName());
        }

        Hash hash(final IRI property, @Nullable final IRI value, final boolean inverse) {
            return Hash.murmur3(property.toString(), "\n", value == null ? "" : value.toString(),
                    "\n", inverse ? "i" : "d");
        }

    }

    private static final class Counter implements Comparable<Counter> {

        final long hashHi;

        final long hashLo;

        int count;

        Counter(final long hashHi, final long hashLo) {
            this.hashHi = hashHi;
            this.hashLo = hashLo;
            this.count = 1;
        }

        @Override
        public int compareTo(final Counter other) {
            int result = Integer.compare(this.count, other.count);
            if (result == 0) {
                result = Long.compare(this.hashHi, other.hashHi);
                if (result == 0) {
                    result = Long.compare(this.hashLo, other.hashLo);
                }
            }
            return result;
        }

    }

    private static final class PseudoClass {

        IRI iri;

        volatile boolean emitted;

    }

}
