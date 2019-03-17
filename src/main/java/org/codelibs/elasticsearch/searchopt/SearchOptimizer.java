package org.codelibs.elasticsearch.searchopt;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

public class SearchOptimizer {

    public static final Setting<Long> MIN_TOTAL_SETTING = Setting.longSetting("index.searchopt.min_total", 0L, 0L, Property.IndexScope);

    public static final Setting<String> QUERY_NEW_INDEX_SETTING =
            Setting.simpleString("index.searchopt.query.new_index", Property.IndexScope);

    private final ClusterService clusterService;

    public SearchOptimizer(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public <REQUEST extends ActionRequest, RESPONSE extends ActionResponse> void process(Task task, String action, REQUEST request,
            ActionListener<RESPONSE> listener, ActionFilterChain<REQUEST, RESPONSE> chain) {
        if (SearchAction.NAME.equals(action)) {
            processSearch(task, action, (SearchRequest) request, (ActionListener<SearchResponse>) listener,
                    (ActionFilterChain<SearchRequest, SearchResponse>) chain);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    private void processSearch(Task task, String action, SearchRequest request, ActionListener<SearchResponse> listener,
            ActionFilterChain<SearchRequest, SearchResponse> chain) {
        String[] indices = request.indices();
        if (indices.length != 1) {
            chain.proceed(task, action, request, listener);
            return;
        }

        final IndexMetaData indexMeta = clusterService.state().getMetaData().index(indices[0]);
        final Settings settings = indexMeta.getSettings();
        final long minTotal = MIN_TOTAL_SETTING.get(settings).longValue();
        if (minTotal == 0L) {
            chain.proceed(task, action, request, listener);
            return;
        }

        chain.proceed(task, action, request, ActionListener.wrap(res -> {
            if (res.getHits().getTotalHits() < minTotal) {
                boolean isUpdated = false;
                final String newIndex = QUERY_NEW_INDEX_SETTING.get(settings);
                if (newIndex != null) {
                    request.indices(newIndex);
                    isUpdated = true;
                }
                if (isUpdated) {
                    chain.proceed(task, action, request, listener);
                    return;
                }
            }
            listener.onResponse(res);
        }, listener::onFailure));
    }

}
