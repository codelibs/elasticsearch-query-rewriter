package org.codelibs.elasticsearch.searchopt.action;

import org.codelibs.elasticsearch.searchopt.SearchOptimizer;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

public class SearchOptActionFilter implements ActionFilter {

    private SearchOptimizer searchOptimizer;

    @Override
    public int order() {
        return 10;
    }

    @Override
    public <REQUEST extends ActionRequest, RESPONSE extends ActionResponse> void apply(Task task, String action, REQUEST request,
            ActionListener<RESPONSE> listener, ActionFilterChain<REQUEST, RESPONSE> chain) {
        if (searchOptimizer == null) {
            chain.proceed(task, action, request, listener);
            return;
        }

        searchOptimizer.process(task, action, request, listener, chain);
    }

    public void setSearchOptimizer(SearchOptimizer searchOptimizer) {
        this.searchOptimizer = searchOptimizer;
    }

}
