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

import org.openrdf.rio.RDFHandler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

final class SequenceProcessor extends RDFProcessor {

    private final RDFProcessor[] processors;

    SequenceProcessor(final RDFProcessor... processors) {
        this.processors = processors.clone();
    }

    public List<RDFProcessor> getProcessors() {
        return Collections.unmodifiableList(Arrays.asList(processors));
    }

    @Override
    public int getExtraPasses() {
        int result = 0;
        for (final RDFProcessor processor : this.processors) {
            result += processor.getExtraPasses();
        }
        return result;
    }

    @Override
    public RDFHandler getHandler(final RDFHandler sink) {
        RDFHandler result = sink;
        for (int i = this.processors.length - 1; i >= 0; --i) {
            result = this.processors[i].getHandler(result);
        }
        return result;
    }

}
