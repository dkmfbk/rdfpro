package eu.fbk.rdfpro.base;

import javax.annotation.Nullable;

import org.openrdf.rio.RDFHandler;

import eu.fbk.rdfpro.RDFProcessor;
import eu.fbk.rdfpro.util.Options;

public final class UniqueProcessor extends RDFProcessor {

    private final boolean mergeTriples;

    static UniqueProcessor doCreate(final String... args) {
        final Options options = Options.parse("m", args);
        return new UniqueProcessor(options.hasOption("m"));
    }

    public UniqueProcessor(final boolean mergeTriples) {
        this.mergeTriples = mergeTriples;
    }

    @Override
    public RDFHandler getHandler(@Nullable final RDFHandler handler) {
        return RDFProcessor.parallel(
                this.mergeTriples ? SetOperation.UNION_TRIPLES : SetOperation.UNION_QUADS,
                RDFProcessor.nop()).getHandler(handler);
    }

}
