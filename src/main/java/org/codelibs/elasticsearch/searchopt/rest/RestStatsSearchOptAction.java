package org.codelibs.elasticsearch.searchopt.rest;

import java.io.IOException;

import org.codelibs.elasticsearch.searchopt.SearchOptimizer;
import org.codelibs.elasticsearch.searchopt.stats.SearchStats;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;

public class RestStatsSearchOptAction extends BaseRestHandler {

    private final SearchOptimizer searchOptimizer;

    public RestStatsSearchOptAction(final Settings settings, final RestController controller, final SearchOptimizer searchOptimizer) {
        super(settings);
        this.searchOptimizer = searchOptimizer;

        controller.registerHandler(Method.GET, "/_searchopt/stats", this);
    }

    @Override
    public String getName() {
        return "searchopt_stats_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String pretty = request.param("pretty");

        return channel -> {
            final SearchStats stats = searchOptimizer.stats();

            final XContentBuilder builder = JsonXContent.contentBuilder();
            if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject();
            builder.startObject("_all");
            stats.toXContent(builder, null);
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

}
