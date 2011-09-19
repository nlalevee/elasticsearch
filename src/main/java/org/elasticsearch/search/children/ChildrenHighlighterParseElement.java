/*
 * Licensed to Elastic Search and Shay Banon under one
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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.highlight.HighlighterParseElementBase;
import org.elasticsearch.search.highlight.SearchContextHighlight.Field;
import org.elasticsearch.search.internal.SearchContext;

/**
 * <pre>
 * children : {
 *     size : 2,
 *     highlight : {
 *        // same of top level highlight
 *     }
 * }
 * </pre>
 */
public class ChildrenHighlighterParseElement extends HighlighterParseElementBase implements SearchParseElement {

    private static final int DEFAULT_FETCH_SIZE = 10;

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token;
        String topLevelFieldName = null;
        int size = DEFAULT_FETCH_SIZE;
        List<Field> fields = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                topLevelFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("size".equals(topLevelFieldName)) {
                    size = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("highlight".equals(topLevelFieldName)) {
                    // FIXME doParse will eat all of the token stream, so if there is something after "highlight", we
                    // won't get to it
                    fields = doParse(parser, context);
                }
            }
        }

        context.children(new SearchContextChildren(size, fields));
    }
}
