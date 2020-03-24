package com.fleetpin.graphql.database.manager;

public interface KeyFactory {
    static <T extends Table> DatabaseKey<T> createDatabaseKey(
            final String organisationId,
            final Class<T> type,
            final String id
    ) {
        return new DatabaseKey<>(organisationId, type, id);
    }

    static <T extends Table> DatabaseQueryKey<T> createDatabaseQueryKey(
            final String organisationId,
            final Query<T> query
    ) {
        return new DatabaseQueryKey<>(organisationId, query);
    }
}
