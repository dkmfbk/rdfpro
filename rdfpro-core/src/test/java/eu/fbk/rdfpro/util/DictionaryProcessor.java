package eu.fbk.rdfpro.util;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.fbk.rdfpro.AbstractRDFHandlerWrapper;
import eu.fbk.rdfpro.RDFHandlers;
import eu.fbk.rdfpro.RDFProcessor;

// TODO: delete this class

public class DictionaryProcessor implements RDFProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryProcessor.class);

    static RDFProcessor create(final String name, final String... args)
            throws IOException, RDFHandlerException {
        return new DictionaryProcessor();
    }

    @Override
    public RDFHandler wrap(final RDFHandler handler) {
        return RDFHandlers.decouple(new AbstractRDFHandlerWrapper(handler) {

            private Dictionary dictionary;

            private AtomicInteger counter;

            @Override
            public void startRDF() throws RDFHandlerException {
                super.startRDF();
                this.dictionary = Dictionary.newMemoryDictionary();
                this.counter = new AtomicInteger();
            }

            @Override
            public void handleStatement(final Statement stmt) throws RDFHandlerException {
                final int sc = this.dictionary.encode(stmt.getSubject());
                final int pc = this.dictionary.encode(stmt.getPredicate());
                final int oc = this.dictionary.encode(stmt.getObject());
                final int cc = this.dictionary.encode(stmt.getContext());
                this.counter.addAndGet(sc + pc + oc + cc);
                super.handleStatement(stmt);
            }

            @Override
            public void endRDF() throws RDFHandlerException {
                LOGGER.info(this.counter + " - " + this.dictionary.toString());
                this.dictionary = null;
                super.endRDF();
            }

        });
    }

}
