package com.fleetpin.graphql.database.manager;

import java.util.List;
import java.util.function.Consumer;

public class SequentialQueryBuilder<V extends Table> extends QueryBuilder<V, List<V>> {
    private String after;

    protected SequentialQueryBuilder(Class<V> type) {
    	super(type);
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

    public Query<V, List<V>> build() {
    	return new Query<V, List<V>>(type, startsWith, after, limit);
    }
}