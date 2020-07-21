package com.fleetpin.graphql.database.manager;

import java.time.Instant;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

public class QueryHistoryBuilder<V extends Table> {
	
	private final Class<V> type;
	private String startsWith;
	private String id;
	private Long fromRevision;
	private Long toRevision;
	private Instant fromUpdatedAt;
	private Instant toUpdatedAt;
	
    private QueryHistoryBuilder(Class<V> type) {
    	this.type = type;
	}

    public QueryHistoryBuilder<V> startsWith(String prefix) {
    	this.startsWith = prefix;
    	return this;
    }
    
    public QueryHistoryBuilder<V> id(String id) {
    	this.id = id;
    	return this;
    }
    
    public QueryHistoryBuilder<V> fromRevision(Long fromRevision) {
    	this.fromRevision = fromRevision;
    	return this;
    }
    
    public QueryHistoryBuilder<V> toRevision(Long toRevision) {
    	this.toRevision = toRevision;
    	return this;
    }
    
    public QueryHistoryBuilder<V> fromUpdatedAt(Instant fromUpdatedAt) {
    	this.fromUpdatedAt = fromUpdatedAt;
    	return this;
    }
    
    public QueryHistoryBuilder<V> toUpdatedAt(Instant toUpdatedAt) {
    	this.toUpdatedAt = toUpdatedAt;
    	return this;
    }
    
    public QueryHistoryBuilder<V> applyMutation(Consumer<QueryHistoryBuilder<V>> mutator) {
        mutator.accept((QueryHistoryBuilder<V>) this);
        return (QueryHistoryBuilder<V>) this;
    }

    public QueryHistory<V> build() {
    	Preconditions.checkArgument(!(id != null && startsWith != null), "ID and StartsWith cannot both be set.");
    	Preconditions.checkArgument(!(id == null && startsWith == null), "ID or StartsWith must be set.");
    	if (fromRevision != null || toRevision != null) {
    		Preconditions.checkArgument(fromUpdatedAt == null && toUpdatedAt == null, "Revision and CreatedAt cannot both be set.");
    		Preconditions.checkArgument(startsWith == null, "StartsWith can only be used with updatedAt.");
    	}
    	return new QueryHistory<V>(type, startsWith, id, fromRevision, toRevision, fromUpdatedAt, toUpdatedAt);
    }
    
    public static <V extends Table> QueryHistoryBuilder<V> create(Class<V> type) {
    	return new QueryHistoryBuilder<V>(type);
    }
    
    
}