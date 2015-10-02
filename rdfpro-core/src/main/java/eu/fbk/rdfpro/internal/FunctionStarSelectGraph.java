package eu.fbk.rdfpro.internal;

import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;

import eu.fbk.rdfpro.vocab.RR;

public class FunctionStarSelectGraph implements Function {

    @Override
    public String getURI() {
        return RR.STAR_SELECT_GRAPH.stringValue();
    }

    @Override
    public Value evaluate(final ValueFactory valueFactory, final Value... args)
            throws ValueExprEvaluationException {

        final Value global = args[0];

        Value result = null;
        for (int i = 1; i < args.length; ++i) {
            final Value graph = args[i];
            if (!graph.equals(global)) {
                if (result == null) {
                    result = graph;
                } else {
                    return RDF.NIL;
                }
            }
        }

        return result != null ? result : global;
    }

}
