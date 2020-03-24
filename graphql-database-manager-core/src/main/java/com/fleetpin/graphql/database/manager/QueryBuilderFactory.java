package com.fleetpin.graphql.database.manager;

public interface QueryBuilderFactory {
    public <V extends Table> QueryBuilder<V> getBuilder();
}
