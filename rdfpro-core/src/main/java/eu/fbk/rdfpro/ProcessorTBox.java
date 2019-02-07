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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.util.Namespaces;
import eu.fbk.rdfpro.util.Statements;

final class ProcessorTBox implements RDFProcessor {

    static final RDFProcessor INSTANCE = new ProcessorTBox();

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorTBox.class);

    private ProcessorTBox() {
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return new Handler(Objects.requireNonNull(handler));
    }

    final static class Handler extends AbstractRDFHandlerWrapper {

        private static final int NUM_LOCKS = 128;

        private final Map<IRI, Term> terms;

        private final Object[] locks;

        Handler(@Nullable final RDFHandler handler) {
            super(handler);
            this.terms = new ConcurrentHashMap<>();
            this.locks = new Object[Handler.NUM_LOCKS];
            for (int i = 0; i < this.locks.length; ++i) {
                this.locks[i] = new Object();
            }
        }

        @Override
        public void startRDF() throws RDFHandlerException {
            super.startRDF();
            this.terms.clear();
            for (final IRI type : Statements.TBOX_CLASSES) {
                this.terms.put(type, new Term(true, false, true, false));
            }
            for (final IRI property : Statements.TBOX_PROPERTIES) {
                this.terms.put(property, new Term(true, true, true, false));
            }
        }

        @Override
        public void handleComment(final String comment) throws RDFHandlerException {
            // discarded
        }

        @Override
        public void handleStatement(final Statement statement) throws RDFHandlerException {

            final Resource s = statement.getSubject();
            final IRI p = statement.getPredicate();
            final Value o = statement.getObject();

            boolean emit = false;

            if (!p.equals(RDF.TYPE)) {
                synchronized (this.getLock(p)) {
                    Term term = this.terms.get(p);
                    if (term == null) {
                        term = new Term(false, true, false, true);
                        this.terms.put(p, term);
                    } else if (term.isLanguage) {
                        term.isUsed = true;
                        emit = true;
                    }
                }
            } else if (o instanceof IRI) {
                synchronized (this.getLock(o)) {
                    Term term = this.terms.get(o);
                    if (term == null) {
                        term = new Term(false, false, false, true);
                        this.terms.put((IRI) o, term);
                    } else if (term.isLanguage) {
                        term.isUsed = true;
                        emit = true;
                    }
                }
                if (s instanceof IRI) {
                    final boolean isType = o.equals(RDFS.CLASS) || o.equals(OWL.CLASS);
                    final boolean isProperty = o.equals(RDF.PROPERTY)
                            || o.equals(OWL.DATATYPEPROPERTY) || o.equals(OWL.OBJECTPROPERTY)
                            || o.equals(OWL.ANNOTATIONPROPERTY);
                    if (isType || isProperty) {
                        synchronized (this.getLock(s)) {
                            Term sterm = this.terms.get(s);
                            if (sterm == null) {
                                sterm = new Term(false, isProperty, true, true);
                                this.terms.put((IRI) s, sterm);
                            } else {
                                sterm.isDefined = true;
                            }
                        }
                    }
                }
            }

            if (emit) {
                super.handleStatement(statement);
            }
        }

        @Override
        public void endRDF() throws RDFHandlerException {

            if (ProcessorTBox.LOGGER.isInfoEnabled()) {

                int numTypes = 0;
                int numTypesDefined = 0;

                int numProperties = 0;
                int numPropertiesDefined = 0;

                final List<String> languageTypes = new ArrayList<String>();
                final List<String> languageProperties = new ArrayList<String>();
                final Set<String> undefinedVocabularies = new HashSet<String>();

                for (final Map.Entry<IRI, Term> entry : this.terms.entrySet()) {
                    final IRI iri = entry.getKey();
                    final Term term = entry.getValue();
                    if (term.isLanguage) {
                        if (term.isUsed) {
                            final String s = Statements.formatValue(iri, Namespaces.DEFAULT);
                            if (term.isProperty) {
                                languageProperties.add(s);
                            } else {
                                languageTypes.add(s);
                            }
                        }
                    } else {
                        if (term.isProperty) {
                            ++numProperties;
                            numPropertiesDefined += term.isDefined ? 1 : 0;
                        } else {
                            ++numTypes;
                            numTypesDefined += term.isDefined ? 1 : 0;
                        }
                        if (!term.isDefined) {
                            undefinedVocabularies.add(iri.getNamespace());
                        }
                    }
                }

                Collections.sort(languageTypes);
                Collections.sort(languageProperties);

                if (numTypes > 0) {
                    ProcessorTBox.LOGGER.info(
                            "Found " + numTypes + " classes (" + numTypesDefined + " defined)");
                }
                if (numProperties > 0) {
                    ProcessorTBox.LOGGER.info("Found " + numProperties + " properties ("
                            + numPropertiesDefined + " defined)");
                }
                if (!languageTypes.isEmpty()) {
                    ProcessorTBox.LOGGER
                            .info("Found language classes: " + String.join(" ", languageTypes));
                }
                if (!languageProperties.isEmpty()) {
                    ProcessorTBox.LOGGER.info(
                            "Found language properties: " + String.join(" ", languageProperties));
                }

                if (!undefinedVocabularies.isEmpty()) {
                    for (final String ns1 : new ArrayList<String>(undefinedVocabularies)) {
                        for (final String ns2 : undefinedVocabularies) {
                            if (ns1 != ns2 && ns1.startsWith(ns2)) {
                                undefinedVocabularies.remove(ns1);
                                break;
                            }
                        }
                    }
                    final StringBuilder builder = new StringBuilder(
                            "Found undefined vocabularies:");

                    final String[] sortedVocabularies = new String[undefinedVocabularies.size()];
                    undefinedVocabularies.toArray(sortedVocabularies);
                    Arrays.sort(sortedVocabularies);

                    for (final String ns : sortedVocabularies) {
                        builder.append("\n- ").append(ns);
                    }
                    ProcessorTBox.LOGGER.info(builder.toString());
                }
            }

            this.terms.clear();
            super.endRDF();
        }

        private Object getLock(final Value value) {
            final String s = value.stringValue(); // assume IRI with >= 3 chars
            final int length = s.length();
            final int index = s.charAt(length - 1) * 37 + s.charAt(length - 2);
            return this.locks[(index & 0x7FFFFFFF) % Handler.NUM_LOCKS];
        }

        private static class Term {

            boolean isLanguage;

            boolean isProperty;

            boolean isDefined;

            boolean isUsed;

            Term(final boolean isLanguage, final boolean isProperty, final boolean isDefined,
                    final boolean isUsed) {
                this.isLanguage = isLanguage;
                this.isProperty = isProperty;
                this.isDefined = isDefined;
                this.isUsed = isUsed;
            }

        }

    }

}
