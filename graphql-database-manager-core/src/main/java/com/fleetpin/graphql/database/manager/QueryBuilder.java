package com.fleetpin.graphql.database.manager;

import java.util.function.Consumer;

public abstract class QueryBuilder<V extends Table, R> {
    protected final Class<V> type;
	protected String startsWith;
    protected String after;
    protected Integer limit;

    protected QueryBuilder(Class<V> type) {
    	this.type = type;
	}

    public QueryBuilder<V, R> startsWith(String prefix) {
    	this.startsWith = prefix;
    	return this;
    }

    public QueryBuilder<V, R> limit(Integer limit) {
    	this.limit = limit;
    	return this;
    }

    public QueryBuilder<V, R> applyMutation(Consumer<QueryBuilder<V, R>> mutator) {
        mutator.accept((QueryBuilder<V, R>) this);
        return (QueryBuilder<V, R>) this;
    }

    public abstract Query<V, R> build();

    public static <V extends Table> SequentialQueryBuilder<V> create(Class<V> type) {
    	return new SequentialQueryBuilder<V>(type);
    }
}