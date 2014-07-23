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
package eu.fbk.rdfpro.jsonld;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFWriterBase;

/**
 * An implementation of the RDFWriter interface that writes RDF documents in the JSON-LD format.
 * 
 * <p>
 * JSON-LD is a JSON-based format for serializing data in (a superset of) RDF as JSON and
 * interpreting JSON contents as RDF. See http://www.w3.org/TR/json-ld/ for the format
 * specification.
 * </p>
 * <p>
 * Similarly to RDF/XML, JSON-LD provides for several ways to encode the same triples. This
 * RDFWriter adopts a simple and consistent strategy whose output is a JSON-LD document consisting
 * of a sequence of resource blocks, one for each RDF resource of the configured root type (@see
 * {@link JSONLD#ROOT_TYPES}).
 * </p>
 */
public class JSONLDWriter extends RDFWriterBase {

    private static final int WINDOW = 32 * 1024;

    private final Writer writer;

    private final Map<String, String> prefixes; // namespace-to-prefix map

    private final Map<Resource, Map<Resource, JSONLDWriter.Node>> nodes; // context-to-id-to-node
                                                                         // map

    private JSONLDWriter.Node lrsHead; // head of least recently seen (LRS) linked list

    private JSONLDWriter.Node lrsTail; // tail of least recently seen (LRS) linked list

    private long counter; // statement counter;

    private int indent; // current indentation level

    private Resource emitContext; // context being currently emitted

    private Map<Resource, JSONLDWriter.Node> emitContextNodes; // id-to-node map for cur. context

    private Set<URI> rootTypes;

    /**
     * Creates a new JSONLDWriter that will write to the supplied OutputStream. The UTF-8
     * character encoding is used.
     * 
     * @param stream
     *            the OutputStream to write to
     */
    public JSONLDWriter(final OutputStream stream) {
        this(new OutputStreamWriter(stream, Charset.forName("UTF-8")));
    }

    /**
     * Creates a new JSONLDWriter that will write to the supplied Writer.
     * 
     * @param writer
     *            the Writer to write to
     */
    public JSONLDWriter(final Writer writer) {
        if (writer == null) {
            throw new NullPointerException("Null writer");
        }
        this.writer = writer;
        this.prefixes = new LinkedHashMap<String, String>();
        this.nodes = new HashMap<Resource, Map<Resource, JSONLDWriter.Node>>();
        this.lrsHead = null;
        this.lrsTail = null;
        this.counter = 0;
        this.indent = 1;
        this.rootTypes = null;
    }

    @Override
    public RDFFormat getRDFFormat() {
        return RDFFormat.JSONLD;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        this.rootTypes = getWriterConfig().get(JSONLD.ROOT_TYPES);
    }

    @Override
    public void handleComment(final String comment) throws RDFHandlerException {
        try {
            // comments cannot be emitted in JSONLD, but still we use them to flush output
            flush(true);
        } catch (final IOException ex) {
            throw new RDFHandlerException(ex);
        }
    }

    @Override
    public void handleNamespace(final String prefix, final String uri) throws RDFHandlerException {

        // add only if emission to writer not started yet
        if (this.emitContextNodes == null) {
            this.prefixes.put(uri, prefix);
        }
    }

    @Override
    public void handleStatement(final Statement statement) throws RDFHandlerException {

        // retrieve or create a node map for the statement context
        final Resource context = statement.getContext();
        Map<Resource, JSONLDWriter.Node> nodes = this.nodes.get(context);
        if (nodes == null) {
            nodes = new HashMap<Resource, JSONLDWriter.Node>();
            this.nodes.put(context, nodes);
        }

        // retrieve or create a node for the statement subject in the statement context
        final Resource subject = statement.getSubject();
        JSONLDWriter.Node node = nodes.get(subject);
        if (node != null) {
            detach(node);
        } else {
            node = new Node(subject, context);
            nodes.put(subject, node);
        }
        attach(node, this.lrsTail); // move node at the end of LRS list
        node.counter = this.counter++; // update LRS statement counter
        node.statements.add(statement);
        if (statement.getPredicate().equals(RDF.TYPE)
                && this.rootTypes.contains(statement.getObject())) {
            node.root = true;
        }

        try {
            flush(false); // emit nodes not seen in last WINDOW statement
        } catch (final IOException ex) {
            throw new RDFHandlerException(ex);
        }
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        try {
            flush(true);
            this.writer.append("]\n}");
            this.writer.flush();
        } catch (final IOException ex) {
            throw new RDFHandlerException(ex);
        }
    }

    private void flush(final boolean force) throws IOException {

        // Emit preamble of JSONLD document if necessary and select context
        if (this.emitContextNodes == null
                && (force || this.counter - this.lrsHead.counter >= WINDOW)) {
            this.writer.append("{\n\t\"@context\": {");
            if (!this.prefixes.isEmpty()) {
                String separator = "\n\t\t";
                final String[] sortedNamespaces = new String[this.prefixes.size()];
                this.prefixes.keySet().toArray(sortedNamespaces);
                Arrays.sort(sortedNamespaces);
                for (final String namespace : sortedNamespaces) {
                    final String prefix = this.prefixes.get(namespace);
                    this.writer.append(separator);
                    this.writer.append('\"');
                    emitString(prefix);
                    this.writer.append("\": \"");
                    emitString(namespace);
                    this.writer.append('\"');
                    separator = ",\n\t\t";
                }
            }
            this.writer.append("},\n\t\"@graph\": [");
        }

        // Emit all the nodes if force=true, otherwise limit to old nodes
        while (this.lrsHead != null && (force || this.counter - this.lrsHead.counter >= WINDOW)) {

            // detect change of context
            final boolean sameContext = Objects.equals(this.lrsHead.context, this.emitContext);

            // otherwise, close old context if necessary, and add required comma
            if (this.emitContextNodes == null) {
                this.emitContextNodes = this.nodes.get(this.lrsHead.context);
            } else {
                if (!sameContext && this.emitContext != null) {
                    this.writer.append("]\n\t}");
                    --this.indent;
                }
                this.writer.append(',');
                this.writer.append(' ');
            }

            // open new context if necessary
            if (!sameContext) {
                if (this.lrsHead.context != null) {
                    this.writer.append("{\n\t\t\"@id\": ");
                    emit(this.lrsHead.context, false);
                    this.writer.append(",\n\t\t\"@graph\": [");
                    ++this.indent;
                }
                this.emitContext = this.lrsHead.context;
                this.emitContextNodes = this.nodes.get(this.lrsHead.context);
            }

            // emit the node
            emitNode(this.emitContextNodes.get(this.lrsHead.id));
        }

        // if force=true, close the context if necessary
        if (force && this.emitContext != null) {
            this.writer.append("]\n\t}");
            --this.indent;
            this.emitContext = null;
        }
    }

    private void emit(final Value value, final boolean expand) throws IOException {

        if (value instanceof Literal) {
            emitLiteral((Literal) value);
        } else {
            final JSONLDWriter.Node node = expand ? this.emitContextNodes.get(value) : null;
            if (node != null && !node.root) {
                emitNode(node);
            } else {
                if (expand) {
                    this.writer.append("{\"@id\": ");
                }
                if (value instanceof BNode) {
                    emitBNode((BNode) value);
                } else if (value instanceof URI) {
                    emitURI((URI) value);
                }
                if (expand) {
                    this.writer.append('}');
                }
            }
        }
    }

    private void emitNode(final JSONLDWriter.Node node) throws IOException {

        this.emitContextNodes.remove(node.id);
        detach(node);

        ++this.indent;
        this.writer.append('{');
        emitNewline();
        this.writer.append("\"@id\": ");
        emit(node.id, false);

        boolean startProperty = true;
        boolean isTypeProperty = true;
        boolean insideArray = false;

        Collections.sort(node.statements, StatementComparator.INSTANCE);
        final int statementCount = node.statements.size();
        for (int i = 0; i < statementCount; ++i) {

            final Statement statement = node.statements.get(i);
            final URI property = statement.getPredicate();
            final boolean last = i == statementCount - 1
                    || !property.equals(node.statements.get(i + 1).getPredicate());

            if (startProperty) {
                this.writer.append(',');
                emitNewline();
                isTypeProperty = property.equals(RDF.TYPE);
                if (isTypeProperty) {
                    this.writer.append("\"@type\"");
                } else {
                    emit(property, false);
                }
                this.writer.append(": ");
                insideArray = !last;
                if (insideArray) {
                    this.writer.append('[');
                }
            } else {
                this.writer.append(", ");
            }

            emit(statement.getObject(), !isTypeProperty);

            startProperty = last;
            if (startProperty && insideArray) {
                this.writer.append(']');
            }
        }

        --this.indent;
        emitNewline();
        this.writer.append('}');
    }

    private void emitBNode(final BNode bnode) throws IOException {
        this.writer.append("\"_:");
        emitString(bnode.getID());
        this.writer.append('\"');
    }

    private void emitURI(final URI uri) throws IOException {
        final String prefix = this.prefixes.get(uri.getNamespace());
        this.writer.append('\"');
        if (prefix != null) {
            emitString(prefix);
            this.writer.append(':');
            emitString(uri.getLocalName());
        } else {
            emitString(uri.stringValue());
        }
        this.writer.append('\"');
    }

    private void emitLiteral(final Literal literal) throws IOException {
        final URI datatype = literal.getDatatype();
        if (datatype != null) {
            this.writer.append("{\"@type\": ");
            emit(datatype, false);
            this.writer.append(", \"@value\": \"");
        } else {
            final String language = literal.getLanguage();
            if (language != null) {
                this.writer.append("{\"@language\": \"");
                emitString(language);
                this.writer.append("\", \"@value\": \"");
            } else {
                this.writer.append("{\"@value\": \"");
            }
        }
        emitString(literal.getLabel());
        this.writer.append("\"}");
    }

    private void emitString(final String string) throws IOException {
        final int length = string.length();
        for (int i = 0; i < length; ++i) {
            final char ch = string.charAt(i);
            if (ch == '\"' || ch == '\\') {
                this.writer.append('\\').append(ch);
            } else if (Character.isISOControl(ch)) {
                if (ch == '\n') {
                    this.writer.append('\\').append('n');
                } else if (ch == '\r') {
                    this.writer.append('\\').append('r');
                } else if (ch == '\t') {
                    this.writer.append('\\').append('t');
                } else if (ch == '\b') {
                    this.writer.append('\\').append('b');
                } else if (ch == '\f') {
                    this.writer.append('\\').append('f');
                } else {
                    this.writer.append(String.format("\\u%04x", (int) ch));
                }
            } else {
                this.writer.append(ch);
            }
        }
    }

    private void emitNewline() throws IOException {
        this.writer.append('\n');
        for (int i = 0; i < this.indent; ++i) {
            this.writer.append('\t');
        }
    }

    private void detach(final JSONLDWriter.Node node) {
        final JSONLDWriter.Node prev = node.lrsPrev;
        final JSONLDWriter.Node next = node.lrsNext;
        if (prev != null) {
            prev.lrsNext = next;
        } else {
            this.lrsHead = next;
        }
        if (next != null) {
            next.lrsPrev = prev;
        } else {
            this.lrsTail = prev;
        }
    }

    private void attach(final JSONLDWriter.Node node, final JSONLDWriter.Node prev) {
        JSONLDWriter.Node next;
        if (prev == null) {
            next = this.lrsHead;
            this.lrsHead = node;
        } else {
            next = prev.lrsNext;
            prev.lrsNext = node;
        }
        if (next == null) {
            this.lrsTail = node;
        } else {
            next.lrsPrev = node;
        }
        node.lrsPrev = prev;
        node.lrsNext = next;
    }

    private static final class Node {

        final Resource id; // node identifier (statement subject)

        final Resource context; // node context

        final List<Statement> statements; // node statements

        long counter; // last recently seen (LRS) counter

        JSONLDWriter.Node lrsPrev; // pointer to prev node in LRS linked list

        JSONLDWriter.Node lrsNext; // pointer to next node in LRS linked list

        boolean root;

        Node(final Resource id, final Resource context) {
            this.id = id;
            this.context = context;
            this.statements = new ArrayList<Statement>();
        }

    }

    private static final class StatementComparator implements Comparator<Statement> {

        static final StatementComparator INSTANCE = new StatementComparator();

        @Override
        public int compare(final Statement first, final Statement second) {
            int result = compare(first.getPredicate(), second.getPredicate());
            if (result == 0) {
                result = compare(first.getObject(), second.getObject());
            }
            return result;
        }

        private int compare(final Value first, final Value second) {

            if (first instanceof Literal) {
                if (second instanceof Literal) {
                    int result = first.stringValue().compareTo(second.stringValue());
                    if (result == 0) {
                        final Literal firstLit = (Literal) first;
                        final Literal secondLit = (Literal) second;
                        final URI firstDt = firstLit.getDatatype();
                        final URI secondDt = secondLit.getDatatype();
                        result = firstDt == null ? secondDt == null ? 0 : -1
                                : secondDt == null ? 1 : firstDt.stringValue().compareTo(
                                        secondDt.stringValue());
                        if (result == 0) {
                            final String firstLang = firstLit.getLanguage();
                            final String secondLang = secondLit.getLanguage();
                            result = firstLang == null ? secondLang == null ? 0 : -1
                                    : secondLang == null ? 1 : firstLang.compareTo(secondLang);
                        }
                    }
                    return result;
                } else {
                    return -1;
                }

            } else if (first instanceof URI) {
                if (second instanceof URI) {
                    int result = first.stringValue().compareTo(second.stringValue());
                    if (result != 0) {
                        if (first.equals(RDF.TYPE)) { // rdf:type always first
                            result = -1;
                        } else if (second.equals(RDF.TYPE)) {
                            result = 1;
                        }
                    }
                    return result;
                } else if (second instanceof Literal) {
                    return 1;
                } else {
                    return -1;
                }

            } else if (first instanceof BNode) {
                if (second instanceof BNode) {
                    return first.stringValue().compareTo(second.stringValue());
                } else {
                    return 1;
                }
            }

            throw new IllegalArgumentException("Invalid arguments: " + first + ", " + second);
        }
    }

}