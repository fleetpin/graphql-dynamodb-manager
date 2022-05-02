package com.fleetpin.graphql.database.manager;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParallelQueryBuilder<V extends Table> extends QueryBuilder<V, Set<V>>  {
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

    public ParallelQueryBuilder<V> after(Map<String, ParallelStartsWith> from) {
        this.after = from;
        return this;
    }

    public ParallelQueryBuilder<V> parallel(Integer parallelRequests, String parallelGrouping) {
        this.parallelRequests = parallelRequests;
        this.parallelGrouping = parallelGrouping;
        return this;
    }

    public Query<V, Set<V>> build() {
        return new Query<V, Set<V>>(type, startsWith, after, limit, parallelRequests, parallelGrouping);
    }
}

