package com.fleetpin.graphql.database.manager;

import com.google.common.collect.HashMultimap;

public interface TableAccess {
    public static <T extends Table> void setTableSource(
            final T table,
            final String sourceTable,
            final HashMultimap<String, String> links,
            final String sourceOrganisationId
    ) {
        table.setSource(sourceTable, links, sourceOrganisationId);
    }

    public static <T extends Table> String getTableSourceOrganisation(final T table) {
        return table.getSourceOrganisationId();
    }

    public static <T extends Table> HashMultimap<String, String> getTableLinks(final T table) {
        return table.getLinks();
    }
}