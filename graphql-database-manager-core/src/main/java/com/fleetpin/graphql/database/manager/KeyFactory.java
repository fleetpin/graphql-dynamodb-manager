package com.fleetpin.graphql.database.manager;

public final class KeyFactory {
    public static DatabaseKey createDatabaseKey(
            final String organisationId,
            final Class<? extends Table> type,
            final String id
    ) {
        return new DatabaseKey(organisationId, type, id);
    }

    public static DatabaseQueryKey createDatabaseQueryKey(
            final String organisationId,
            final Class<? extends Table> type
    ) {
        return new DatabaseQueryKey(organisationId, type);
    }
}
