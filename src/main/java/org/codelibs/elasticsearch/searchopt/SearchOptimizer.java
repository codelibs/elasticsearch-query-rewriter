package org.codelibs.elasticsearch.searchopt;

import org.codelibs.elasticsearch.searchopt.stats.SearchStats;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

public class SearchOptimizer {

    public static final Setting<Long> MIN_TOTAL_SETTING =
            Setting.longSetting("index.searchopt.min_total", 0L, 0L, Property.Dynamic, Property.IndexScope);

    public static final Setting<String> QUERY_NEW_INDEX_SETTING =
            Setting.simpleString("index.searchopt.query.new_index", Property.Dynamic, Property.IndexScope);

    private Client client;

    private final ClusterService clusterService;

    private CounterMetric totalMetric = new CounterMetric();

    private CounterMetric optimizeMetric = new CounterMetric();

    private CounterMetric newIndexMetric = new CounterMetric();

    public SearchOptimizer(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    public SearchStats stats() {
        return new SearchStats(totalMetric.count(), optimizeMetric.count(), newIndexMetric.count());
    }

    public synchronized void clear() {
        totalMetric = new CounterMetric();
        optimizeMetric = new CounterMetric();
        newIndexMetric = new CounterMetric();
    }

    public <REQUEST extends ActionRequest, RESPONSE extends ActionResponse> void process(final Task task, final String action,
            final REQUEST request, final ActionListener<RESPONSE> listener, final ActionFilterChain<REQUEST, RESPONSE> chain) {
        if (SearchAction.NAME.equals(action)) {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> l = (ActionListener<SearchResponse>) listener;
            @SuppressWarnings("unchecked")
            ActionFilterChain<SearchRequest, SearchResponse> c = (ActionFilterChain<SearchRequest, SearchResponse>) chain;
            processSearch(task, action, (SearchRequest) request, l, c);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }

    private void processSearch(final Task task, final String action, final SearchRequest request,
            final ActionListener<SearchResponse> listener, final ActionFilterChain<SearchRequest, SearchResponse> chain) {
        final String[] indices = request.indices();
        if (indices.length != 1) {
            chain.proceed(task, action, request, listener);
            return;
        }

        final IndexMetaData indexMeta = clusterService.state().getMetaData().index(indices[0]);
        if (indexMeta == null) {
            chain.proceed(task, action, request, listener);
            return;
        }

        final Settings settings = indexMeta.getSettings();
        final long minTotal = MIN_TOTAL_SETTING.get(settings).longValue();
        if (minTotal == 0L) {
            chain.proceed(task, action, request, listener);
            return;
        }

        totalMetric.inc();
        chain.proceed(task, action, request, ActionListener.wrap(res -> {
            if (res.getHits().getTotalHits() < minTotal) {
                boolean isUpdated = false;
                final String newIndex = QUERY_NEW_INDEX_SETTING.get(settings);
                if (newIndex != null) {
                    request.indices(newIndex);
                    newIndexMetric.inc();
                    isUpdated = true;
                }
                if (isUpdated) {
                    optimizeMetric.inc();
                    client.search(request, listener);
                    return;
                }
            }
            listener.onResponse(res);
        }, listener::onFailure));
    }

}
