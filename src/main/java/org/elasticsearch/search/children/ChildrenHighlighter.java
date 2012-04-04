/*
 * Licensed to Nicolas Lalevee under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.children;

import java.util.List;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.search.child.TopChildrenQuery;
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
            // BlockJoinQuery not supported for now
            // } else if (query instanceof BlockJoinQuery) {
            // cumulatorQuery.add(((BlockJoinQuery) query).getChildQuery(), Occur.MUST);
        } else if (query instanceof FilteredQuery) {
            extractChildQueries(cumulatorQuery, ((FilteredQuery) query).getQuery());
        }
        return cumulatorQuery;
    }
}
