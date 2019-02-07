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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Statements;

final class ProcessorSmush implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorSmush.class);

    private static final int BUFFER_SIZE = 262144;

    private final String[] rankedNamespaces;

    ProcessorSmush(final String... rankedNamespaces) {
        this.rankedNamespaces = rankedNamespaces.clone();
    }

    @Override
    public int getExtraPasses() {
        return 1;
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(Objects.requireNonNull(handler));
    }

    private final class Handler extends AbstractRDFHandlerWrapper {

        private int[] table;

        private int entries;

        private ByteBuffer[] buffers;

        private int endPointer;

        private boolean firstPass;

        private long numLookups;

        private long numTests;

        Handler(final RDFHandler handler) {

            super(handler);

            this.table = new int[1022]; // 511 entries, ~4K memory page
            this.entries = 0;
            this.buffers = new ByteBuffer[65536];
            this.endPointer = this.pointerFor(0, 4); // skip first word to avoid 0 pointers
            this.firstPass = true;
            this.numLookups = 0;
            this.numTests = 0;

            this.buffers[0] = ByteBuffer.allocate(ProcessorSmush.BUFFER_SIZE);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            if (!this.firstPass) {
                super.startRDF();
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            if (!this.firstPass) {
                super.handleComment(comment);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String iri)
                throws RDFHandlerException {
            if (!this.firstPass) {
                super.handleNamespace(prefix, iri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            final Resource s = statement.getSubject();
            final IRI p = statement.getPredicate();
            final Value o = statement.getObject();
            final Resource c = statement.getContext();

            final boolean isSameAs = p.equals(OWL.SAMEAS) && o instanceof Resource;

            if (!this.firstPass) {
                final Resource sn = this.rewrite(s);
                final Value on = o instanceof Literal ? o : this.rewrite((Resource) o);
                final Resource cn = c == null ? null : this.rewrite(c);
                if (isSameAs) {
                    if (sn != s) {
                        super.handleStatement(this.createStatement(sn, OWL.SAMEAS, s, cn));
                    }
                    if (on != o) {
                        super.handleStatement(
                                this.createStatement((Resource) on, OWL.SAMEAS, o, cn));
                    }
                } else {
                    final IRI pn = (IRI) this.rewrite(p);
                    if (sn == s && pn == p && on == o && cn == c) {
                        super.handleStatement(statement);
                    } else {
                        super.handleStatement(this.createStatement(sn, pn, on, cn));
                    }
                }
            } else if (isSameAs && !s.equals(o)) {
                synchronized (this) {
                    this.link(s, (Resource) o);
                }
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (!this.firstPass) {
                super.endRDF();
            } else {
                this.normalize();
                this.firstPass = false;
            }
        }

        @Override
        public void close() {
            super.close();
            this.table = null; // eagerly release memory
            this.buffers = null; // eagerly release memory
        }

        // LINKING, NORMALIZATION AND REWRITING METHODS

        private void link(final Resource resource1, final Resource resource2) {
            final int pointer1 = this.lookup(resource1, true);
            final int pointer2 = this.lookup(resource2, true);
            final int nextPointer1 = this.readNext(pointer1);
            for (int pointer = nextPointer1; pointer != pointer1; pointer = this
                    .readNext(pointer)) {
                if (pointer == pointer2) {
                    return; // already linked
                }
            }
            final int nextPointer2 = this.readNext(pointer2);
            this.writeNext(pointer1, nextPointer2);
            this.writeNext(pointer2, nextPointer1);
        }

        private void normalize() throws RDFHandlerException {
            final Comparator<Value> comparator = Statements
                    .valueComparator(ProcessorSmush.this.rankedNamespaces);
            int numClusters = 0;
            int numResources = 0;
            for (int j = 0; j < this.table.length; j += 2) {
                final int pointer = this.table[j];
                if (pointer == 0 || this.readNormalized(pointer)) {
                    continue;
                }
                final List<Resource> resources = new ArrayList<Resource>();
                Resource chosenResource = null;
                int chosenPointer = 0;
                // int chosenRank = Integer.MAX_VALUE;
                for (int p = pointer; p != pointer || chosenPointer == 0; p = this.readNext(p)) {
                    final Resource resource = this.readResource(p);
                    resources.add(resource);
                    if (chosenResource == null
                            || comparator.compare(resource, chosenResource) < 0) {
                        chosenResource = resource;
                        chosenPointer = p;
                    }
                    // if (resource instanceof BNode) {
                    // if (chosenResource == null
                    // || chosenResource instanceof BNode
                    // && resource.stringValue().length() < chosenResource.stringValue()
                    // .length()) {
                    // chosenResource = resource;
                    // chosenPointer = p;
                    // }
                    // } else if (resource instanceof IRI) {
                    // final String string = resource.stringValue();
                    // int rank = Integer.MAX_VALUE;
                    // for (int i = 0; i < ProcessorSmush.this.rankedNamespaces.length; ++i) {
                    // if (string.startsWith(ProcessorSmush.this.rankedNamespaces[i])) {
                    // rank = i;
                    // break;
                    // }
                    // }
                    // if (!(chosenResource instanceof IRI) || rank < chosenRank
                    // || rank == chosenRank
                    // && string.length() < chosenResource.stringValue().length()) {
                    // chosenResource = resource;
                    // chosenPointer = p;
                    // chosenRank = rank;
                    // }
                    // }
                }
                ++numClusters;
                numResources += resources.size();
                int p = pointer;
                do {
                    final int pn = this.readNext(p);
                    this.writeNext(p, chosenPointer);
                    this.writeNormalized(p, true);
                    p = pn;
                } while (p != pointer);
            }
            if (ProcessorSmush.LOGGER.isInfoEnabled()) {
                ProcessorSmush.LOGGER.info(String.format(
                        "owl:sameAs normalization: %d resource(s), %d cluster(s), "
                                + "%.3f collisions/lookup, %dMB buffered ",
                        numResources, numClusters,
                        (double) (this.numTests - this.numLookups) / this.numLookups,
                        this.bufferFor(this.endPointer) * (ProcessorSmush.BUFFER_SIZE / 1024)
                                / 1024));
            }
        }

        private Resource rewrite(final Resource resource) {
            final int pointer = this.lookup(resource, false);
            if (pointer == 0) {
                return resource;
            }
            final int nextPointer = this.readNext(pointer);
            return nextPointer == pointer ? resource : this.readResource(nextPointer);
        }

        private int lookup(final Resource resource, final boolean canAppend) {
            ++this.numLookups;
            final int hash = resource.hashCode();
            int offset = (hash & 0x7FFFFFFF) % (this.table.length / 2) * 2;
            while (true) {
                ++this.numTests;
                int pointer = this.table[offset];
                if (pointer == 0) {
                    if (!canAppend) {
                        return 0;
                    } else if (this.entries > this.table.length / 4) { // enforce load factor < .5
                        this.rehash();
                        return this.lookup(resource, canAppend); // repeat after rehashing
                    }
                    pointer = this.append(resource);
                    this.table[offset] = pointer;
                    this.table[offset + 1] = hash;
                    ++this.entries;
                    return pointer;
                }
                if (this.table[offset + 1] == hash && this.matchResource(pointer, resource)) {
                    return pointer;
                }
                offset += 2;
                if (offset >= this.table.length) {
                    offset = 0;
                }
            }
        }

        private void rehash() {
            final int newSize = this.table.length + 1;
            final int newTable[] = new int[2 * newSize];
            for (int oldOffset = 0; oldOffset < this.table.length; oldOffset += 2) {
                final int pointer = this.table[oldOffset];
                if (pointer != 0) {
                    final int hash = this.table[oldOffset + 1];
                    int newOffset = (hash & 0x7FFFFFFF) % newSize * 2;
                    while (newTable[newOffset] != 0) {
                        newOffset += 2;
                        if (newOffset >= newTable.length) {
                            newOffset = 0;
                        }
                    }
                    newTable[newOffset] = pointer;
                    newTable[newOffset + 1] = hash;
                }
            }
            this.table = newTable;
        }

        // POINTER MANAGEMENT METHODS

        private int pointerFor(final int buffer, final int offset) {
            return (buffer & 0xFFFF) << 16 | offset + 3 >> 2 & 0xFFFF;
        }

        private int bufferFor(final int pointer) {
            return pointer >> 16 & 0xFFFF;
        }

        private int offsetFor(final int pointer) {
            return (pointer & 0xFFFF) << 2;
        }

        // BUFFER MANIPULATION METHODS

        private int append(final Resource resource) {
            final String string = resource.stringValue();
            final int length = string.length();
            int bufferIndex = this.bufferFor(this.endPointer);
            int offset = this.offsetFor(this.endPointer);
            final ByteBuffer buffer;
            if (offset + 6 + length * 3 > ProcessorSmush.BUFFER_SIZE) {
                ++bufferIndex;
                buffer = ByteBuffer.allocate(ProcessorSmush.BUFFER_SIZE);
                this.buffers[bufferIndex] = buffer;
                this.endPointer = this.pointerFor(bufferIndex, 0);
                offset = 0;
            } else {
                buffer = this.buffers[bufferIndex];
            }
            buffer.putInt(offset, this.endPointer);
            offset += 4;
            buffer.putShort(offset,
                    (short) (length | (resource instanceof BNode ? 0x4000 : 0x0000)));
            offset += 2;
            for (int i = 0; i < length; ++i) {
                final char ch = string.charAt(i);
                if (ch > 0 && ch < 128) {
                    buffer.put(offset++, (byte) ch);
                } else {
                    buffer.put(offset++, (byte) 0);
                    buffer.putChar(offset, ch);
                    offset += 2;
                }
            }
            final int pointer = this.endPointer;
            this.endPointer = this.pointerFor(bufferIndex, offset);
            return pointer;
        }

        private Resource readResource(final int pointer) {
            final ByteBuffer buffer = this.buffers[this.bufferFor(pointer)];
            int offset = this.offsetFor(pointer) + 4;
            final int lengthAndFlags = buffer.getShort(offset);
            final int length = lengthAndFlags & 0x3FFF;
            final boolean bnode = (lengthAndFlags & 0x4000) != 0;
            final StringBuilder builder = new StringBuilder();
            offset += 2;
            while (builder.length() < length) {
                final byte b = buffer.get(offset++);
                if (b != 0) {
                    builder.append((char) b);
                } else {
                    builder.append(buffer.getChar(offset));
                    offset += 2;
                }
            }
            final String string = builder.toString();
            return bnode ? Statements.VALUE_FACTORY.createBNode(string) //
                    : Statements.VALUE_FACTORY.createIRI(string);
        }

        private boolean matchResource(final int pointer, final Resource resource) {
            final String string = resource.stringValue();
            final int length = string.length();
            final ByteBuffer buffer = this.buffers[this.bufferFor(pointer)];
            int offset = this.offsetFor(pointer) + 4;
            final int lengthAndFlags = buffer.getShort(offset);
            final boolean bnode = (lengthAndFlags & 0x4000) != 0;
            if (bnode && resource instanceof IRI || !bnode && resource instanceof BNode) {
                return false;
            }
            final int l = lengthAndFlags & 0x3FFF;
            if (l != length) {
                return false;
            }
            offset += 2;
            for (int i = 0; i < length; ++i) {
                final char c = string.charAt(i);
                if (c > 0 && c < 128) {
                    if (c != (char) buffer.get(offset++)) {
                        return false;
                    }
                } else {
                    if (buffer.get(offset++) != 0) {
                        return false;
                    }
                    if (c != buffer.getChar(offset)) {
                        return false;
                    }
                    offset += 2;
                }
            }
            return true;
        }

        private int readNext(final int pointer) {
            final ByteBuffer buffer = this.buffers[this.bufferFor(pointer)];
            final int offset = this.offsetFor(pointer);
            return buffer.getInt(offset);
        }

        private void writeNext(final int pointer, final int next) {
            final ByteBuffer buffer = this.buffers[this.bufferFor(pointer)];
            final int offset = this.offsetFor(pointer);
            buffer.putInt(offset, next);
        }

        private boolean readNormalized(final int pointer) {
            final ByteBuffer buffer = this.buffers[this.bufferFor(pointer)];
            final int offset = this.offsetFor(pointer) + 4;
            return (buffer.getShort(offset) & 0x8000) != 0;
        }

        private void writeNormalized(final int pointer, final boolean normalized) {
            final ByteBuffer buffer = this.buffers[this.bufferFor(pointer)];
            final int offset = this.offsetFor(pointer) + 4;
            buffer.putShort(offset,
                    (short) (buffer.getShort(offset) | (normalized ? 0x8000 : 0x0000)));
        }

        private Statement createStatement(final Resource subj, final IRI pred, final Value obj,
                @Nullable final Resource ctx) {
            return ctx == null ? Statements.VALUE_FACTORY.createStatement(subj, pred, obj) //
                    : Statements.VALUE_FACTORY.createStatement(subj, pred, obj, ctx);
        }

    }
}
