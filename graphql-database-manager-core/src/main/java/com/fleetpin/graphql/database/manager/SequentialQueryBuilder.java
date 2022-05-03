package com.fleetpin.graphql.database.manager;

import java.util.List;

public class SequentialQueryBuilder<V extends Table> extends QueryBuilder<V, List<V>> {
    private String after;

    protected SequentialQueryBuilder(Class<V> type) {
    	super(type);
	}

    @Override
    public SequentialQueryBuilder<V> startsWith(String prefix) {
        return (SequentialQueryBuilder<V>)super.startsWith(prefix);
    }

    @Override
    public SequentialQueryBuilder<V> limit(Integer limit) {
        return (SequentialQueryBuilder<V>)super.limit(limit);
    }

    public SequentialQueryBuilder<V> after(String from) {
    	this.after = from;
    	return this;
    }

    public ParallelQueryBuilder<V> parallel(Integer parallelRequests, String parallelGrouping) {
        var parallelQueryBuilder = new ParallelQueryBuilder<V>(this);
        parallelQueryBuilder.parallel(parallelRequests, parallelGrouping);
        return parallelQueryBuilder;
    }

    public SequentialQuery<V> build() {
    	return new SequentialQuery(type, startsWith, limit, after);
    }
}