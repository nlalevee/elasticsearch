package org.elasticsearch.test.integration.search.children;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.topChildrenQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
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

    @Test public void testChildrenIdFetch() throws Exception {
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
    }

}
