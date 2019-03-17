package org.codelibs.elasticsearch.searchopt;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.codelibs.elasticsearch.searchopt.action.SearchOptActionFilter;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

public class SearchOptPlugin extends Plugin implements ActionPlugin {

    private SearchOptActionFilter actionFilter = new SearchOptActionFilter();

    @Override
    public List<ActionFilter> getActionFilters() {
        return Arrays.asList(actionFilter);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService, ScriptService scriptService, NamedXContentRegistry xContentRegistry,
            Environment environment, NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        SearchOptimizer searchOptimizer = new SearchOptimizer(clusterService);
        actionFilter.setSearchOptimizer(searchOptimizer);
        return Arrays.asList(searchOptimizer);
    }
}
