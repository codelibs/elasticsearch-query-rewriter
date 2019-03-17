package org.codelibs.elasticsearch.searchopt;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
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
        final String index = "test_index";

        /*
        try (CurlResponse curlResponse = EcrCurl.get(node, "/" + index + "/_queryRewriter").execute()) {
            final String content = curlResponse.getContentAsString();
            assertNotNull(content);
            final Map<String, Object> contentMap = curlResponse.getContent(EcrCurl.jsonParser);
            assertEquals(index, contentMap.get("index"));
            assertTrue(contentMap.get("description").toString().startsWith("This is a elasticsearch-searchopt response:"));
        }

        try (CurlResponse curlResponse = EcrCurl.get(node, "/_queryRewriter").execute()) {
            final String content = curlResponse.getContentAsString();
            assertNotNull(content);
            final Map<String, Object> contentMap = curlResponse.getContent(EcrCurl.jsonParser);
            assertFalse(contentMap.containsKey("index"));
            assertTrue(contentMap.get("description").toString().startsWith("This is a elasticsearch-searchopt response:"));
        }
        */
    }
}
