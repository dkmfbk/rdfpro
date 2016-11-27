package eu.fbk.rdfpro.util;

import java.util.Map;

import javax.annotation.Nullable;

import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.queryrender.QueryRenderer;

/**
 * Implementation of {@link QueryRenderer} based on RDFpro rendering capabilities.
 */
public class QueryRendererImpl implements QueryRenderer {

    @Nullable
    private final Map<String, String> prefixes;

    private final boolean forceSelect;

    public QueryRendererImpl(@Nullable final Map<String, String> prefixes,
            final boolean forceSelect) {
        this.prefixes = prefixes;
        this.forceSelect = forceSelect;
    }

    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.SPARQL;
    }

    @Override
    public String render(final ParsedQuery query) throws Exception {
        return Algebra.renderQuery(query.getTupleExpr(), query.getDataset(), this.prefixes,
                this.forceSelect);
    }

}
