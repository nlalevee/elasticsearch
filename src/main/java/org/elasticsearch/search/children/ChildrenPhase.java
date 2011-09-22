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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.selector.UidFieldSelector;
import org.elasticsearch.index.search.child.TopChildrenQuery;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.highlight.ESHighlighter;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class ChildrenPhase implements FetchSubPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("children", new ChildrenHighlighterParseElement());
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.children() != null;
    }

    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        List<Integer> childIds = gatherChildIds(context, hitContext);
        int n = 0;
        ESHighlighter esHighlighter = null;
        if (context.children().fields() != null) {
            esHighlighter = new ChildrenHighlighter(context, context.children().fields());
        }
        ChildrenResult[] childrenResults = new ChildrenResult[Math.min(childIds.size(), context.children().size())];
        for (Integer childId : childIds) {
            if (context.children().size() <= n) {
                break;
            }
            Document doc;
            try {
                doc = hitContext.reader().document(childId, UidFieldSelector.INSTANCE);
            } catch (IOException e) {
                throw new FetchPhaseExecutionException(context, "Failed to fetch children doc id [" + childId + "]", e);
            }
            String uidField = doc.get(UidFieldMapper.NAME);
            Uid uid = Uid.createUid(uidField);
            Map<String, HighlightField> highlight = null;
            if (esHighlighter != null) {
                highlight = esHighlighter.highlight(hitContext, uid.type(), childId);
            }
            childrenResults[n++] = new ChildrenResult(uid.type(), uid.id(), highlight);
        }
        hitContext.hit().childrenResults(childrenResults);
    }

    private List<Integer> gatherChildIds(SearchContext context, HitContext hitContext) {
        if (!context.queryRewritten()) {
            try {
                context.updateRewriteQuery(context.searcher().rewrite(context.query()));
            } catch (IOException e) {
                throw new FetchPhaseExecutionException(context, "Failed to fetch children", e);
            }
        }

        List<Integer> childrenIds = Lists.newArrayList();
        gatherChildIds(childrenIds, hitContext.hit().docId(), context.query());
        return childrenIds;
    }

    private void gatherChildIds(List<Integer> childrenIds, int parentId, Query query) {
        if (query instanceof BooleanQuery) {
            for (BooleanClause clause : ((BooleanQuery) query).getClauses()) {
                if (!clause.isProhibited()) {
                    gatherChildIds(childrenIds, parentId, clause.getQuery());
                }
            }
        } else if (query instanceof TopChildrenQuery) {
            List<Integer> ids = ((TopChildrenQuery) query).getChildrendDocsByParent().get(parentId);
            if (ids != null) {
                childrenIds.addAll(ids);
            }
            // BlockJoinQuery not supported for now
            // } else if (query instanceof BlockJoinQuery) {
            // List<Integer> ids = ((BlockJoinQuery) query).getChildrendDocsByParent().get(parentId);
            // if (ids != null) {
            // childrenIds.addAll(ids);
            // }
        }
    }
}
