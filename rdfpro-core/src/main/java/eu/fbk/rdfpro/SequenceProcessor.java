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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.openrdf.rio.RDFHandler;

import eu.fbk.rdfpro.util.Handlers;

final class SequenceProcessor extends RDFProcessor {

    private final RDFProcessor[] processors;

    private final int extraPasses;

    SequenceProcessor(final RDFProcessor... processors) {

        int extraPasses = 0;
        for (final RDFProcessor processor : processors) {
            extraPasses += processor.getExtraPasses();
        }

        this.processors = processors.clone();
        this.extraPasses = extraPasses;
    }

    public List<RDFProcessor> getProcessors() {
        return Collections.unmodifiableList(Arrays.asList(this.processors));
    }

    @Override
    public int getExtraPasses() {
        return this.extraPasses;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        RDFHandler result = handler != null ? handler : Handlers.nop();
        for (int i = this.processors.length - 1; i >= 0; --i) {
            result = this.processors[i].getHandler(result);
        }
        return result;
    }

}
