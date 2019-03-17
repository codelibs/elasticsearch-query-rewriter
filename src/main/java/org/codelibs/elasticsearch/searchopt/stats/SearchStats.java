package org.codelibs.elasticsearch.searchopt.stats;

import java.io.IOException;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class SearchStats implements Streamable, ToXContent {

    private long total;

    private long optimized;

    private long newIndex;

    public SearchStats() {
    }

    public SearchStats(final long total, final long optimized, final long newIndex) {
        this.total = total;
        this.optimized = optimized;
        this.newIndex = newIndex;
    }

    @Override
    public void readFrom(final StreamInput in) throws IOException {
        total = in.readVLong();
        optimized = in.readVLong();
        newIndex = in.readVLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(optimized);
        out.writeVLong(newIndex);
    }

    private static final ParseField SEARCH_OPT_STATS = new ParseField("search_opt");

    private static final ParseField TOTAL = new ParseField("total");

    private static final ParseField OPTIMIZED = new ParseField("optimized");

    private static final ParseField NEW_INDEX = new ParseField("new_index");

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(SEARCH_OPT_STATS.getPreferredName());
        builder.field(TOTAL.getPreferredName(), total);
        builder.field(OPTIMIZED.getPreferredName(), optimized);
        builder.field(NEW_INDEX.getPreferredName(), newIndex);
        builder.endObject();
        return builder;
    }

}
