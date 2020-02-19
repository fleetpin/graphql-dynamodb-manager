package com.fleetpin.graphql.database.manager;

import com.google.common.collect.HashMultimap;

public abstract class AbstractTestDynamoDb {
    protected <T extends Table> HashMultimap<String, String> getLinks(final T entity) {
        return entity.getLinks();
    }

    protected <T extends Table> void setSource(
            final T entity,
            final String sourceTable,
            final HashMultimap<String, String> links,
            final String sourceOrganisationId
    ) {
        entity.setSource(sourceTable, links, sourceOrganisationId);
    }

    protected DatabaseKey createDatabaseKey(final String organisationId, final Class<? extends Table> type, final String id) {
        return new DatabaseKey(organisationId, type, id);
    }

    protected DatabaseQueryKey createDatabaseQueryKey(final String organisationId, final Class<? extends Table> type) {
        return new DatabaseQueryKey(organisationId, type);
    }
}
