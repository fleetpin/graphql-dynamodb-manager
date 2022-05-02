package com.fleetpin.graphql.database.manager;

import java.util.Map;

public class ParallelQueryBuilder<V extends Table> extends QueryBuilder  {
    private Map<String, ParallelStartsWith> after;
    private Integer parallelRequests;
    private String parallelGrouping;

    public ParallelQueryBuilder(QueryBuilder<V> qb) {
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

    public ParallelQueryBuilder<V> after(String from) {
        throw new RuntimeException("Need to specify partitions to use after in a parallel request");
    }

    public ParallelQueryBuilder<V> parallel(Integer parallelRequests, String parallelGrouping) {
        this.parallelRequests = parallelRequests;
        this.parallelGrouping = parallelGrouping;
        return this;
    }

    public Query<V> build() {
        return new Query<V>(type, startsWith, after, limit, parallelRequests, parallelGrouping);
    }

    public static <V extends Table> ParallelQueryBuilder<V> create(QueryBuilder<V> qb) {
        return new ParallelQueryBuilder<V>(qb);
    }
}

