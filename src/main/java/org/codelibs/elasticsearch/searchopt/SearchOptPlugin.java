package org.codelibs.elasticsearch.searchopt;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.codelibs.elasticsearch.searchopt.action.SearchOptActionFilter;
import org.codelibs.elasticsearch.searchopt.rest.RestClearSearchOptAction;
import org.codelibs.elasticsearch.searchopt.rest.RestStatsSearchOptAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

public class SearchOptPlugin extends Plugin implements ActionPlugin {

    private final SearchOptActionFilter actionFilter = new SearchOptActionFilter();

    private SearchOptimizer searchOptimizer;

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(SearchOptimizer.MIN_TOTAL_SETTING, //
                SearchOptimizer.QUERY_NEW_INDEX_SETTING);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return Arrays.asList(actionFilter);
    }

    @Override
    public Collection<Object> createComponents(final Client client, final ClusterService clusterService, final ThreadPool threadPool,
            final ResourceWatcherService resourceWatcherService, final ScriptService scriptService, final NamedXContentRegistry xContentRegistry,
            final Environment environment, final NodeEnvironment nodeEnvironment, final NamedWriteableRegistry namedWriteableRegistry) {
        searchOptimizer = new SearchOptimizer(client,clusterService);
        actionFilter.setSearchOptimizer(searchOptimizer);
        return Arrays.asList(searchOptimizer);
    }

    @Override
    public List<RestHandler> getRestHandlers(final Settings settings, final RestController restController, final ClusterSettings clusterSettings,
            final IndexScopedSettings indexScopedSettings, final SettingsFilter settingsFilter, final IndexNameExpressionResolver indexNameExpressionResolver,
            final Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(new RestClearSearchOptAction(settings, restController, searchOptimizer), //
                new RestStatsSearchOptAction(settings, restController, searchOptimizer));
    }
}
