package com.fleetpin.graphql.database.manager;

public interface KeyFactory {
    public static <T extends Table> DatabaseKey<T> createDatabaseKey(
            final String organisationId,
            final Class<T> type,
            final String id
    ) {
        return new DatabaseKey<>(organisationId, type, id);
    }

    public static <T extends Table> DatabaseQueryKey<T> createDatabaseQueryKey(
            final String organisationId,
            final Class<T> type
    ) {
        return new DatabaseQueryKey<>(organisationId, type);
    }
}
