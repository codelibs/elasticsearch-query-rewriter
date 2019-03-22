package org.codelibs.elasticsearch.searchopt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codelibs.elasticsearch.searchopt.stats.SearchStats;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
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
        } else if (BulkAction.NAME.equals(action)) {
            BulkRequest bulkRequest = (BulkRequest) request;
            List<DocWriteRequest<?>> requests = new ArrayList<>();
            for (DocWriteRequest<?> req : bulkRequest.requests()) {
                if (req instanceof IndexRequest) {
                    IndexRequest indexRequest = (IndexRequest) req;
                    Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
                    // TODO ...
                    requests.add(req); // TODO replace with a new one if ...
                    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                        indexRequest.writeTo(new OutputStreamStreamOutput(out));
                        IndexRequest newRequest = new IndexRequest();
                        newRequest.readFrom(new InputStreamStreamInput(new ByteArrayInputStream(out.toByteArray())));
                        // TODO newRequest.index("newname");
                        requests.add(newRequest);
                    } catch (Exception e) {
                        // TODO ...
                    }
                } else if (req instanceof UpdateRequest) {
                    requests.add(req);
                } else if (req instanceof DeleteRequest) {
                    requests.add(req);
                }
            }
            bulkRequest.requests().clear();
            bulkRequest.requests().addAll(requests);
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
