package org.elasticsearch.search.children;

import java.util.List;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.search.child.TopChildrenQuery;
import org.elasticsearch.index.search.nested.BlockJoinQuery;
import org.elasticsearch.search.highlight.ESHighlighter;
import org.elasticsearch.search.highlight.SearchContextHighlight.Field;
import org.elasticsearch.search.internal.SearchContext;

public class ChildrenHighlighter extends ESHighlighter {

    public ChildrenHighlighter(SearchContext context, List<Field> fields) {
        super(context, fields);
    }

    @Override
    protected Query getParsedQuery() {
        Query query = context.parsedQuery().query();
        return extractChildQueries(new BooleanQuery(), query);
    }

    /**
     * 
     * @param cumulatorQuery
     *            a fake boolean query to gather all child queries in one
     * @param query
     * @return
     */
    private Query extractChildQueries(BooleanQuery cumulatorQuery, Query query) {
        if (query instanceof BooleanQuery) {
            for (BooleanClause clause : ((BooleanQuery) query).clauses()) {
                if (!clause.isProhibited()) {
                    extractChildQueries(cumulatorQuery, clause.getQuery());
                }
            }
        } else if (query instanceof TopChildrenQuery) {
            cumulatorQuery.add(((TopChildrenQuery) query).getChildQuery(), Occur.MUST);
        } else if (query instanceof BlockJoinQuery) {
            cumulatorQuery.add(((BlockJoinQuery) query).getChildQuery(), Occur.MUST);
        }
        return cumulatorQuery;
    }
}
