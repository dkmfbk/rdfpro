package eu.fbk.rdfpro.rules.seminaive;

import java.util.Collection;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;

import eu.fbk.rdfpro.rules.RuleEngine;
import eu.fbk.rdfpro.rules.Ruleset;
import eu.fbk.rdfpro.rules.model.QuadModel;

public class SemiNaiveRuleEngine2 extends RuleEngine {

    public SemiNaiveRuleEngine2(final Ruleset ruleset) {
        super(ruleset);
    }

    @Override
    protected void doEval(final Collection<Statement> model) {
        // TODO Auto-generated method stub
        super.doEval(model);
    }

    @Override
    protected RDFHandler doEval(final RDFHandler handler) {
        // TODO Auto-generated method stub
        return super.doEval(handler);
    }

    private static abstract class Phase {

        public RDFHandler eval(final RDFHandler handler) {
            throw new Error();
        }

        public void eval(final QuadModel model) {
            throw new Error();
        }

        public boolean isHandlerSupported() {
            return false;
        }

        public boolean isModelSupported() {
            return false;
        }

    }

    private static final class StreamPhase extends Phase {

        @Override
        public RDFHandler eval(final RDFHandler handler) {
            // TODO
            return null;
        }

        @Override
        public boolean isHandlerSupported() {
            return true;
        }

    }

}
