package eu.fbk.rdfpro.internal;

import com.google.common.base.Joiner;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import org.eclipse.rdf4j.query.algebra.evaluation.function.Function;

import eu.fbk.rdfpro.util.Statements;
import eu.fbk.rdfpro.vocab.RR;

public class FunctionSid implements Function {

    private static final String NAMESPACE = "sid:";

    @Override
    public String getURI() {
        return RR.SID.stringValue();
    }

    @Override
    public Value evaluate(final ValueFactory factory, final Value... args)
            throws ValueExprEvaluationException {
        try {
            final StringBuilder builder = new StringBuilder(NAMESPACE.length() + 22);
            builder.append(NAMESPACE);
            Statements.getHash(Statements.VALUE_FACTORY.createStatement((Resource) args[0],
                    (IRI) args[1], args[2])).toString(builder);
            return Statements.VALUE_FACTORY.createIRI(builder.toString());
        } catch (final Throwable ex) {
            throw new ValueExprEvaluationException(
                    "Could not compute statement ID for: " + Joiner.on(' ').join(args), ex);
        }
    }

}
