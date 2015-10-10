package eu.fbk.rdfpro.util;

import java.util.Map;

import javax.annotation.Nullable;

import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.queryrender.QueryRenderer;

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
