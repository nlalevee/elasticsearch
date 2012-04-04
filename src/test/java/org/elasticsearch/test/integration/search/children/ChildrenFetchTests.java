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
package org.elasticsearch.test.integration.search.children;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.topChildrenQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Map.Entry;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.children.ChildrenResult;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ChildrenFetchTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test public void testTopChildrenIdFetch() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("blog", jsonBuilder().startObject().startObject("blog").startObject("properties")
                        .startObject("title").field("type", "string").endObject()
                        .endObject().endObject().endObject())
                .addMapping("blog_tag", jsonBuilder().startObject().startObject("blog_tag").startObject("properties")
                        .startObject("tag").field("type", "string").endObject()
                        .endObject().startObject("_parent").field("type", "blog")
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "blog", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the parent/children fetching")
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "blog_tag", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("tag", "tag" + i)
                            .endObject())
                    .setParent(Integer.toString(i))
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(topChildrenQuery("blog_tag", fieldQuery("tag", "tag1")))
                .setChildrenSize(10)
                .execute().actionGet();

        System.out.println(search);

        assertThat(search.hits().totalHits(), equalTo(1l));
        assertThat(search.hits().hits().length, equalTo(1));
        assertThat(search.getFailedShards(), equalTo(0));
        assertThat(search.hits().hits()[0].childrenResults(), notNullValue());
        assertThat(search.hits().hits()[0].childrenResults().length, equalTo(1));
        assertThat(search.hits().hits()[0].childrenResults()[0].id(), equalTo("1"));
        assertThat(search.hits().hits()[0].childrenResults()[0].type(), equalTo("blog_tag"));
    }

    // not launched since BlockJoinQuery is not supported for now
    // @Test
    public void testNestedChildrenIdFetch() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("blog", jsonBuilder().startObject().startObject("blog").startObject("properties")
                        .startObject("title").field("type", "string").endObject()
                        .startObject("blog_tag").field("type", "nested").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "blog", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the parent/children fetching")
                            .field("blog_tag").startArray().startObject().field("tag", "tag" + i).endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(nestedQuery("blog_tag", fieldQuery("blog_tag.tag", "tag1")))
                .setChildrenSize(10)
                .execute().actionGet();

        System.out.println(search);

        assertThat(search.hits().totalHits(), equalTo(1l));
        assertThat(search.hits().hits().length, equalTo(1));
        assertThat(search.getFailedShards(), equalTo(0));
        assertThat(search.hits().hits()[0].childrenResults(), notNullValue());
        assertThat(search.hits().hits()[0].childrenResults().length, equalTo(1));
        assertThat(search.hits().hits()[0].childrenResults()[0].id(), equalTo("1"));
        assertThat(search.hits().hits()[0].childrenResults()[0].type(), equalTo("_nested"));
    }

    @Test public void testHighlighTopChildren() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("blog", jsonBuilder().startObject().startObject("blog").startObject("properties")
                        .startObject("title").field("type", "string").endObject()
                        .endObject().endObject().endObject())
                .addMapping("blog_tag", jsonBuilder().startObject().startObject("blog_tag").startObject("properties")
                        .startObject("tag").field("type", "string").endObject()
                        .endObject().startObject("_parent").field("type", "blog")
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "blog", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the parent/children fetching")
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "blog_tag", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("tag", "foo bar tag" + i + " stuff and other things")
                            .endObject())
                    .setParent(Integer.toString(i))
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(topChildrenQuery("blog_tag", fieldQuery("tag", "tag1")))
                .setChildrenSize(10).addChildrenHighlightedField("tag")
                .execute().actionGet();

        System.out.println(search);

        assertThat(search.hits().totalHits(), equalTo(1l));
        assertThat(search.hits().hits().length, equalTo(1));
        assertThat(search.getFailedShards(), equalTo(0));
        assertThat(search.hits().hits()[0].childrenResults(), notNullValue());
        assertThat(search.hits().hits()[0].childrenResults().length, equalTo(1));
        ChildrenResult childrenResult = search.hits().hits()[0].childrenResults()[0];
        assertThat(childrenResult.id(), equalTo("1"));
        assertThat(childrenResult.type(), equalTo("blog_tag"));
        assertThat(childrenResult.highlightFields(), notNullValue());
        assertThat(childrenResult.highlightFields().size(), equalTo(1));
        Entry<String, HighlightField> highlight = childrenResult.highlightFields().entrySet().iterator().next();
        assertThat(highlight.getKey(), equalTo("tag"));
        assertThat(highlight.getValue().fragments().length, equalTo(1));
        assertThat(highlight.getValue().fragments()[0], equalTo("foo bar <em>tag1</em> stuff and other things"));
    }

    // not launched since BlockJoinQuery is not supported for now
    // @Test
    public void testHighlighNestedChildren() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client.admin().indices().prepareCreate("test").setSettings(ImmutableSettings.settingsBuilder().put("number_of_shards", 2))
                .addMapping("blog", jsonBuilder().startObject().startObject("blog").startObject("properties")
                        .startObject("title").field("type", "string").endObject()
                        .startObject("blog_tag").field("type", "nested").endObject()
                        .endObject().endObject().endObject())
                .execute().actionGet();

        for (int i = 0; i < 5; i++) {
            client.prepareIndex("test", "blog", Integer.toString(i))
                    .setSource(XContentFactory.jsonBuilder().startObject()
                            .field("title", "This is a test on the parent/children fetching")
                            .field("blog_tag").startArray().startObject().field("tag", "foo bar tag" + i + " stuff and other things").endObject().endArray()
                            .endObject())
                    .setRefresh(true).execute().actionGet();
        }

        SearchResponse search = client.prepareSearch()
                .setQuery(nestedQuery("blog_tag", fieldQuery("blog_tag.tag", "tag1")))
                .setChildrenSize(10).addChildrenHighlightedField("tag")
                .execute().actionGet();

        System.out.println(search);

        assertThat(search.hits().totalHits(), equalTo(1l));
        assertThat(search.hits().hits().length, equalTo(1));
        assertThat(search.getFailedShards(), equalTo(0));
        assertThat(search.hits().hits()[0].childrenResults(), notNullValue());
        assertThat(search.hits().hits()[0].childrenResults().length, equalTo(1));
        ChildrenResult childrenResult = search.hits().hits()[0].childrenResults()[0];
        assertThat(childrenResult.id(), equalTo("1"));
        assertThat(childrenResult.type(), equalTo("_nested"));
        assertThat(childrenResult.highlightFields(), notNullValue());
        assertThat(childrenResult.highlightFields().size(), equalTo(1));
        Entry<String, HighlightField> highlight = childrenResult.highlightFields().entrySet().iterator().next();
        assertThat(highlight.getKey(), equalTo("tag"));
        assertThat(highlight.getValue().fragments().length, equalTo(1));
        assertThat(highlight.getValue().fragments()[0], equalTo("foo bar <em>tag1</em> stuff and other things"));
    }

}
