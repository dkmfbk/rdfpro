package eu.fbk.rdfpro.internal;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import eu.fbk.rdfpro.vocab.RR;

public class FunctionNamespace implements Function {

    @Override
    public String getURI() {
        return RR.NAMESPACE_P.stringValue();
    }

    @Override
    public Value evaluate(final ValueFactory factory, final Value... args)
            throws ValueExprEvaluationException {

        if (args.length != 1) {
            throw new ValueExprEvaluationException(
                    "Expected unique IRI parameter, received " + args.length);
        }

        final IRI iri;
        try {
            iri = (IRI) args[0];
        } catch (final ClassCastException ex) {
            throw new ValueExprEvaluationException(
                    "Expected unique IRI parameter, received " + args[0]);
        }

        return factory.createLiteral(iri.getNamespace());
    }

}
