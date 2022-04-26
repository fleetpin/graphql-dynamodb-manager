package com.fleetpin.graphql.database.manager;

import java.util.Optional;
import java.util.function.Consumer;

public class QueryBuilder<V extends Table> {
	
	private final Class<V> type;
	private String startsWith;
	private String after;
	private Integer limit;
	private Optional<Integer> parallelRequests;
	
    private QueryBuilder(Class<V> type) {
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

    public QueryBuilder<V> parallel(Integer parallelRequests) {
        this.parallelRequests = Optional.of(parallelRequests);
        return this;
    }


    public QueryBuilder<V> applyMutation(Consumer<QueryBuilder<V>> mutator) {
        mutator.accept((QueryBuilder<V>) this);
        return (QueryBuilder<V>) this;
    }

    public Query<V> build() {
    	return new Query<V>(type, startsWith, after, limit, parallelRequests);
    }
    
    public static <V extends Table> QueryBuilder<V> create(Class<V> type) {
    	return new QueryBuilder<V>(type);
    }
    
    
}