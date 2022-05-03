package com.fleetpin.graphql.database.manager;

import java.util.Map;
import java.util.List;

public class ParallelQueryBuilder<V extends Table> extends QueryBuilder<V, List<V>>  {
    private Map<String, ParallelStartsWith> after;
    private Integer parallelRequests;
    private String parallelGrouping;

    public ParallelQueryBuilder(QueryBuilder<V, ?> qb) {
        super(qb.type);
        this.limit = qb.limit;
        this.startsWith = qb.startsWith;

        if (this.after != null) {
            throw new RuntimeException("Need to specify partitions to use after in a parallel request");
        }
    }

    @Override
    public ParallelQueryBuilder<V> startsWith(String prefix) {
        return (ParallelQueryBuilder<V>)super.startsWith(prefix);
    }

    @Override
    public ParallelQueryBuilder<V> limit(Integer limit) {
        return (ParallelQueryBuilder<V>)super.limit(limit);
    }

    public ParallelQueryBuilder<V> after(Map<String, ParallelStartsWith> from) {
        this.after = from;
        return this;
    }

    public ParallelQueryBuilder<V> parallel(Integer parallelRequests, String parallelGrouping) {
        this.parallelRequests = parallelRequests;
        this.parallelGrouping = parallelGrouping;
        return this;
    }

    public ParallelQuery<V> build() {
        return new ParallelQuery(type, startsWith, limit, after, parallelRequests, parallelGrouping);
    }
}

