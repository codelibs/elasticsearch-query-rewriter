package org.codelibs.elasticsearch.searchopt.rest;

import java.io.IOException;

import org.codelibs.elasticsearch.searchopt.SearchOptimizer;
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

public class RestClearSearchOptAction extends BaseRestHandler {

    private final SearchOptimizer searchOptimizer;

    public RestClearSearchOptAction(final Settings settings, final RestController controller, final SearchOptimizer searchOptimizer) {
        super(settings);
        this.searchOptimizer = searchOptimizer;

        controller.registerHandler(Method.POST, "/_searchopt/clear", this);
    }

    @Override
    public String getName() {
        return "searchopt_clear_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String pretty = request.param("pretty");

        return channel -> {
            searchOptimizer.clear();

            final XContentBuilder builder = JsonXContent.contentBuilder();
            if (pretty != null && !"false".equalsIgnoreCase(pretty)) {
                builder.prettyPrint().lfAtEnd();
            }
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };

    }

}
