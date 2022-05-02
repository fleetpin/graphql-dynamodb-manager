package com.fleetpin.graphql.database.manager;

public interface KeyFactory {
    static <T extends Table> DatabaseKey<T> createDatabaseKey(
            final String organisationId,
            final Class<T> type,
            final String id
    ) {
        return new DatabaseKey<>(organisationId, type, id);
    }

    static <T extends Table> DatabaseSequentialQueryKey<T> createDatabaseSequentialQueryKey(
            final String organisationId,
            final SequentialQuery<T> query
    ) {
        return new DatabaseSequentialQueryKey<T>(organisationId, query);
    }

    static <T extends Table> DatabaseParallelQueryKey<T> createDatabaseParallelQueryKey(
            final String organisationId,
            final ParallelQuery<T> query
    ) {
        return new DatabaseParallelQueryKey<T>(organisationId, query);
    }

	static <T extends Table> DatabaseQueryHistoryKey<T> createDatabaseQueryHistoryKey(String organisationId,
			QueryHistory<T> queryHistory) {
		
		return new DatabaseQueryHistoryKey<>(organisationId, queryHistory);
	}
}
