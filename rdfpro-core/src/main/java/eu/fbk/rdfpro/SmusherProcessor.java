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
package eu.fbk.rdfpro;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SmusherProcessor extends RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmusherProcessor.class);

    private final long bufferSize;

    private final String[] rankedNamespaces;

    public SmusherProcessor(final long bufferSize, final String... rankedNamespaces) {

        this.bufferSize = bufferSize;
        this.rankedNamespaces = rankedNamespaces.clone();
    }

    @Override
    public int getExtraPasses() {
        return 1;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler handler) {
        return new Handler(handler, this.bufferSize, this.rankedNamespaces);
    }

    private static final class Handler implements RDFHandler, Closeable {

        private static final int BUFFER_SIZE = 262144;

        private static final int MAX_TRIES = 1024 * 1024;

        private final RDFHandler handler;

        private final String[] rankedNamespaces;

        private int[] table;

        private ByteBuffer[] buffers;

        private int endPointer;

        private boolean firstPass;

        private long numLookups;

        private long numTests;

        public Handler(final RDFHandler handler, final long bufferSize,
                final String... rankedNamespaces) {

            final long tableSize = Math.min(bufferSize / 4 - 1, Integer.MAX_VALUE);

            this.handler = handler;
            this.rankedNamespaces = rankedNamespaces;
            this.table = new int[(int) tableSize]; // 16 bytes entry with 0.25 fill rate
            this.buffers = new ByteBuffer[65536];
            this.endPointer = pointerFor(0, 4); // skip first word to avoid 0 pointers
            this.firstPass = true;
            this.numLookups = 0;
            this.numTests = 0;

            this.buffers[0] = ByteBuffer.allocate(BUFFER_SIZE);
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            if (!this.firstPass) {
                this.handler.startRDF();
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            if (!this.firstPass) {
                this.handler.handleComment(comment);
            }
        }

        @Override
        public void handleNamespace(final String prefix, final String uri)
                throws RDFHandlerException {
            if (!this.firstPass) {
                this.handler.handleNamespace(prefix, uri);
            }
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            final Resource s = statement.getSubject();
            final URI p = statement.getPredicate();
            final Value o = statement.getObject();
            final Resource c = statement.getContext();

            final boolean isSameAs = p.equals(OWL.SAMEAS) && o instanceof Resource;

            if (!this.firstPass) {
                final Resource sn = rewrite(s);
                final Value on = o instanceof Literal ? o : rewrite((Resource) o);
                final Resource cn = c == null ? null : rewrite(c);
                if (isSameAs) {
                    if (sn != s) {
                        this.handler.handleStatement(createStatement(sn, OWL.SAMEAS, s, cn));
                    }
                    if (on != o) {
                        this.handler.handleStatement(createStatement((Resource) on, OWL.SAMEAS, o,
                                cn));
                    }
                } else {
                    final URI pn = (URI) rewrite(p);
                    if (sn == s && pn == p && on == o && cn == c) {
                        this.handler.handleStatement(statement);
                    } else {
                        this.handler.handleStatement(createStatement(sn, pn, on, cn));
                    }
                }
            } else if (isSameAs && !s.equals(o)) {
                synchronized (this) {
                    link(s, (Resource) o);
                }
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {
            if (!this.firstPass) {
                this.handler.endRDF();
            } else {
                normalize();
                this.firstPass = false;
            }
        }

        @Override
        public void close() throws IOException {
            Util.closeQuietly(this.handler);
            this.table = null; // eagerly release memory
            this.buffers = null; // eagerly release memory
        }

        // LINKING, NORMALIZATION AND REWRITING METHODS

        private void link(final Resource resource1, final Resource resource2) {
            final int pointer1 = lookup(resource1, true);
            final int pointer2 = lookup(resource2, true);
            final int nextPointer1 = readNext(pointer1);
            for (int pointer = nextPointer1; pointer != pointer1; pointer = readNext(pointer)) {
                if (pointer == pointer2) {
                    return; // already linked
                }
            }
            final int nextPointer2 = readNext(pointer2);
            writeNext(pointer1, nextPointer2);
            writeNext(pointer2, nextPointer1);
        }

        private void normalize() throws RDFHandlerException {
            int numClusters = 0;
            int numResources = 0;
            for (final int pointer : this.table) {
                if (pointer == 0 || readNormalized(pointer)) {
                    continue;
                }
                final List<Resource> resources = new ArrayList<Resource>();
                Resource chosenResource = null;
                int chosenPointer = 0;
                int chosenRank = Integer.MAX_VALUE;
                for (int p = pointer; p != pointer || chosenPointer == 0; p = readNext(p)) {
                    final Resource resource = readResource(p);
                    resources.add(resource);
                    if (resource instanceof BNode) {
                        if (chosenResource == null
                                || chosenResource instanceof BNode
                                && resource.stringValue().length() < chosenResource.stringValue()
                                        .length()) {
                            chosenResource = resource;
                            chosenPointer = p;
                        }
                    } else if (resource instanceof URI) {
                        final String string = resource.stringValue();
                        int rank = Integer.MAX_VALUE;
                        for (int i = 0; i < this.rankedNamespaces.length; ++i) {
                            if (string.startsWith(this.rankedNamespaces[i])) {
                                rank = i;
                                break;
                            }
                        }
                        if (!(chosenResource instanceof URI) || rank < chosenRank
                                || rank == chosenRank
                                && string.length() < chosenResource.stringValue().length()) {
                            chosenResource = resource;
                            chosenPointer = p;
                            chosenRank = rank;
                        }
                    }
                }
                ++numClusters;
                numResources += resources.size();
                int p = pointer;
                do {
                    final int pn = readNext(p);
                    writeNext(p, chosenPointer);
                    writeNormalized(p, true);
                    p = pn;
                } while (p != pointer);
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(String.format(
                        "owl:sameAs normalization: %d resource(s), %d cluster(s), "
                                + "%.3f collisions/lookup, %dMB buffered ", numResources,
                        numClusters, (double) (this.numTests - this.numLookups) / this.numLookups,
                        bufferFor(this.endPointer) * (BUFFER_SIZE / 1024) / 1024));
            }
        }

        private Resource rewrite(final Resource resource) {
            final int pointer = lookup(resource, false);
            if (pointer == 0) {
                return resource;
            }
            final int nextPointer = readNext(pointer);
            return nextPointer == pointer ? resource : readResource(nextPointer);
        }

        private int lookup(final Resource resource, final boolean canAppend) {
            ++this.numLookups;
            int index = Math.abs(resource.hashCode() % this.table.length);
            int delta = 0;
            for (int i = 0; i < MAX_TRIES; ++i) {
                ++this.numTests;
                int pointer = this.table[index];
                if (pointer == 0) {
                    if (!canAppend) {
                        return 0;
                    }
                    pointer = append(resource);
                    this.table[index] = pointer;
                    return pointer;
                } else if (matchResource(pointer, resource)) {
                    return pointer;
                }
                if (delta == 0) {
                    final String string = resource.stringValue();
                    final int length = string.length();
                    delta = length;
                    for (int j = 0; j < length; ++j) {
                        delta = delta << 5 ^ delta >> 27 ^ string.charAt(i);
                    }
                    if (delta == 0) {
                        delta = 1;
                    } else {
                        delta = Math.abs(delta % this.table.length);
                    }
                }
                index = (index + delta) % this.table.length;
            }
            throw new IllegalStateException("Lookup table too small (" + this.table.length + ")");
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
            int bufferIndex = bufferFor(this.endPointer);
            int offset = offsetFor(this.endPointer);
            final ByteBuffer buffer;
            if (offset + 6 + length * 3 > BUFFER_SIZE) {
                ++bufferIndex;
                buffer = ByteBuffer.allocate(BUFFER_SIZE);
                this.buffers[bufferIndex] = buffer;
                this.endPointer = pointerFor(bufferIndex, 0);
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
            this.endPointer = pointerFor(bufferIndex, offset);
            return pointer;
        }

        private Resource readResource(final int pointer) {
            final ByteBuffer buffer = this.buffers[bufferFor(pointer)];
            int offset = offsetFor(pointer) + 4;
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
            return bnode ? Util.FACTORY.createBNode(string) : Util.FACTORY.createURI(string);
        }

        private boolean matchResource(final int pointer, final Resource resource) {
            final String string = resource.stringValue();
            final int length = string.length();
            final ByteBuffer buffer = this.buffers[bufferFor(pointer)];
            int offset = offsetFor(pointer) + 4;
            final int lengthAndFlags = buffer.getShort(offset);
            final boolean bnode = (lengthAndFlags & 0x4000) != 0;
            if (bnode && resource instanceof URI || !bnode && resource instanceof BNode) {
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
            final ByteBuffer buffer = this.buffers[bufferFor(pointer)];
            final int offset = offsetFor(pointer);
            return buffer.getInt(offset);
        }

        private void writeNext(final int pointer, final int next) {
            final ByteBuffer buffer = this.buffers[bufferFor(pointer)];
            final int offset = offsetFor(pointer);
            buffer.putInt(offset, next);
        }

        private boolean readNormalized(final int pointer) {
            final ByteBuffer buffer = this.buffers[bufferFor(pointer)];
            final int offset = offsetFor(pointer) + 4;
            return (buffer.getShort(offset) & 0x8000) != 0;
        }

        private void writeNormalized(final int pointer, final boolean normalized) {
            final ByteBuffer buffer = this.buffers[bufferFor(pointer)];
            final int offset = offsetFor(pointer) + 4;
            buffer.putShort(offset, (short) (buffer.getShort(offset) | (normalized ? 0x8000
                    : 0x0000)));
        }

        private static Statement createStatement(final Resource subj, final URI pred,
                final Value obj, @Nullable final Resource ctx) {
            return ctx == null ? Util.FACTORY.createStatement(subj, pred, obj) : Util.FACTORY
                    .createStatement(subj, pred, obj, ctx);
        }

    }

}
