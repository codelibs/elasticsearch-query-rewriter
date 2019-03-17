package org.codelibs.elasticsearch.searchopt;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.io.IOException;
import java.util.Map;

import org.codelibs.curl.CurlResponse;
import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.codelibs.elasticsearch.runner.net.EcrCurl;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.node.Node;

import junit.framework.TestCase;

public class SearchOptPluginTests extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.putList("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
            }
        }).build(newConfigs().clusterName("es-cl-run-" + System.currentTimeMillis())
                .pluginTypes("org.codelibs.elasticsearch.searchopt.SearchOptPlugin").numOfNode(3));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_plugin() throws Exception {
        final Node node = runner.node();
        final String index1 = "test_index1";
        final String index2 = "test_index2";
        final String type = "_doc";

        runner.createIndex(index1, Settings.builder().put(SearchOptimizer.MIN_TOTAL_SETTING.getKey(), 10)
                .put(SearchOptimizer.QUERY_NEW_INDEX_SETTING.getKey(), index2).build());
        runner.createIndex(index2, (Settings) null);
        runner.refresh();

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 =
                    runner.insert(index1, type, String.valueOf(i), "{\"msg\":\"test a" + i + " b" + (i % 10) + " c" + (i % 100) + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 =
                    runner.insert(index2, type, String.valueOf(i), "{\"msg\":\"test a" + i + " b" + (i % 5) + " c" + (i % 50) + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        assertSearchResult(1L, node, index1, "msg:a1");
        assertSearchResult(100L, node, index1, "msg:b1");
        assertSearchResult(10L, node, index1, "msg:c1");

        try (CurlResponse curlResponse = EcrCurl.put(node, "/" + index1 + "/_settings").header("Content-Type", "application/json")
                .body("{\"index.searchopt.min_total\":11}").execute()) {
            assertEquals(200, curlResponse.getHttpStatusCode());
        }

        assertSearchResult(1L, node, index1, "msg:a1");
        assertSearchResult(100L, node, index1, "msg:b1");
        assertSearchResult(20L, node, index1, "msg:c1");
    }

    private void assertSearchResult(final long expect, final Node node, final String index, final String query) throws IOException {
        try (CurlResponse curlResponse = EcrCurl.get(node, "/" + index + "/_search").param("q", query).execute()) {
            final Map<String, Object> contentMap = curlResponse.getContent(EcrCurl.jsonParser);
            assertEquals(expect, Long.parseLong(((Map<String, Object>) contentMap.get("hits")).get("total").toString()));
        }
    }
}
