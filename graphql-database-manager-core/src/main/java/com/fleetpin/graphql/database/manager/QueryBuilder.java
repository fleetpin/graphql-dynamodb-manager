package com.fleetpin.graphql.database.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface QueryBuilder<V extends Table> {
    public QueryBuilder<V> on(Class<V> table);

    public QueryBuilder<V> startsWith(String prefix);

    public QueryBuilder<V> limit(Integer i);

    public QueryBuilder<V> from(String s);

    public QueryBuilder<V> until(String s);

    public CompletableFuture<List<V>> exec();
}