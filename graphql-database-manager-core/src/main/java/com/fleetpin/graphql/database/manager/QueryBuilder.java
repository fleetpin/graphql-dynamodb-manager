package com.fleetpin.graphql.database.manager;

import java.util.function.Consumer;

public class QueryBuilder<V extends Table> implements IQueryBuilder<V> {

    protected final Class<V> type;
	protected String startsWith;
    protected String after;
    protected Integer limit;

    protected QueryBuilder(Class<V> type) {
    	this.type = type;
	}

    public QueryBuilder<V> startsWith(String prefix) {
    	this.startsWith = prefix;
    	return this;
    }

    public QueryBuilder<V> limit(Integer limit) {
    	this.limit = limit;
    	return this;
    }

    public QueryBuilder<V> after(String from) {
    	this.after = from;
    	return this;
    }

    public ParallelQueryBuilder<V> parallel(Integer parallelRequests, String parallelGrouping) {
        var parallelQueryBuilder = ParallelQueryBuilder.create(this);
        parallelQueryBuilder.parallel(parallelRequests, parallelGrouping);
        return parallelQueryBuilder;
    }


    public QueryBuilder<V> applyMutation(Consumer<QueryBuilder<V>> mutator) {
        mutator.accept((QueryBuilder<V>) this);
        return (QueryBuilder<V>) this;
    }

    public Query<V> build() {
    	return new Query<V>(type, startsWith, after, limit);
    }
    
    public static <V extends Table> QueryBuilder<V> create(Class<V> type) {
    	return new QueryBuilder<V>(type);
    }
}