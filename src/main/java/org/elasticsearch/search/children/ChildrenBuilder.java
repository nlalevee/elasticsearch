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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;

/**
 * A builder for children fetching.
 * 
 * @see org.elasticsearch.search.builder.SearchSourceBuilder#children()
 */
public class ChildrenBuilder implements ToXContent {

    private HighlightBuilder highlightBuilder;

    private Integer size;

    public ChildrenBuilder size(int size) {
        this.size = size;
        return this;
    }

    /**
     * Adds a field to be highlighted with default fragment size of 100 characters, and default number of fragments of
     * 5.
     * 
     * @param name
     *            The field to highlight
     */
    public ChildrenBuilder addHighlightedField(String name) {
        highlightBuilder().field(name);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and default number of fragments of
     * 5.
     * 
     * @param name
     *            The field to highlight
     * @param fragmentSize
     *            The size of a fragment in characters
     */
    public ChildrenBuilder addHighlightedField(String name, int fragmentSize) {
        highlightBuilder().field(name, fragmentSize);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), and a provided (maximum) number of
     * fragments.
     * 
     * @param name
     *            The field to highlight
     * @param fragmentSize
     *            The size of a fragment in characters
     * @param numberOfFragments
     *            The (maximum) number of fragments
     */
    public ChildrenBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments);
        return this;
    }

    /**
     * Adds a field to be highlighted with a provided fragment size (in characters), a provided (maximum) number of
     * fragments and an offset for the highlight.
     * 
     * @param name
     *            The field to highlight
     * @param fragmentSize
     *            The size of a fragment in characters
     * @param numberOfFragments
     *            The (maximum) number of fragments
     */
    public ChildrenBuilder addHighlightedField(String name, int fragmentSize, int numberOfFragments, int fragmentOffset) {
        highlightBuilder().field(name, fragmentSize, numberOfFragments, fragmentOffset);
        return this;
    }

    /**
     * Set a tag scheme that encapsulates a built in pre and post tags. The allows schemes are <tt>styled</tt> and
     * <tt>default</tt>.
     * 
     * @param schemaName
     *            The tag scheme name
     */
    public ChildrenBuilder setHighlighterTagsSchema(String schemaName) {
        highlightBuilder().tagsSchema(schemaName);
        return this;
    }

    /**
     * Explicitly set the pre tags that will be used for highlighting.
     */
    public ChildrenBuilder setHighlighterPreTags(String... preTags) {
        highlightBuilder().preTags(preTags);
        return this;
    }

    /**
     * Explicitly set the post tags that will be used for highlighting.
     */
    public ChildrenBuilder setHighlighterPostTags(String... postTags) {
        highlightBuilder().postTags(postTags);
        return this;
    }

    /**
     * The order of fragments per field. By default, ordered by the order in the highlighted text. Can be <tt>score</tt>
     * , which then it will be ordered by score of the fragments.
     */
    public ChildrenBuilder setHighlighterOrder(String order) {
        highlightBuilder().order(order);
        return this;
    }

    /**
     * The encoder to set for highlighting
     */
    public ChildrenBuilder setEncoder(String encoder) {
        highlightBuilder().encoder(encoder);
        return this;
    }

    public HighlightBuilder highlightBuilder() {
        if (highlightBuilder == null) {
            highlightBuilder = new HighlightBuilder();
        }
        return highlightBuilder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("children");
        if (size != null) {
            builder.field("size", size);
        }
        if (highlightBuilder != null) {
            highlightBuilder.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

}
