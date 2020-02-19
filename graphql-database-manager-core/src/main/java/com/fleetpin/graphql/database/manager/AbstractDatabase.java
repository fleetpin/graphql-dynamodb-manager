package com.fleetpin.graphql.database.manager;

import com.google.common.collect.HashMultimap;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractDatabase {
    protected DatabaseKey createDatabaseKey(final String organisationId, final Class<? extends Table> type, final String id) {
        return new DatabaseKey(organisationId, type, id);
    }

    protected DatabaseQueryKey createDatabaseQueryKey(final String organisationId, final Class<? extends Table> type) {
        return new DatabaseQueryKey(organisationId, type);
    }

    protected <T extends Table> HashMultimap<String, String> getLinks(final T entity) {
        return entity.getLinks();
    }

    protected <T extends Table> String getSourceOrganistaionId(final T entity) {
        return entity.getSourceOrganistaionId();
    }

    protected <T extends Table> CompletableFuture<List<T>> all(final List<CompletableFuture<T>> collect) {
        return TableUtil.all(collect);
    }
}
