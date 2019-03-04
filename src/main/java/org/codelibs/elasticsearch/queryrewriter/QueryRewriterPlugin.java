package org.codelibs.elasticsearch.queryrewriter;

import java.util.Collections;
import java.util.List;

import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

public class QueryRewriterPlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionFilter> getActionFilters() {
        return Collections.emptyList();
    }
}
