package com.fleetpin.graphql.dynamodb.manager;

import org.dataloader.DataLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class InMemoryDynamoDb implements DynamoDb {
    @Override
    public <T extends Table> CompletableFuture<T> delete(final String organisationId, final T entity) {
        return null;
    }

    @Override
    public <T extends Table> CompletableFuture<T> deleteLinks(final String organisationId, final T entity) {
        return null;
    }

    @Override
    public <T extends Table> CompletableFuture<T> put(final String organisationId, final T entity) {
        return null;
    }

    @Override
    public CompletableFuture<List<DynamoItem>> get(final List<DatabaseKey> keys) {
        return null;
    }

    @Override
    public CompletableFuture<List<DynamoItem>> getViaLinks(final String organisationId, final Table entry, final Class<? extends Table> type, final DataLoader<DatabaseKey, DynamoItem> items) {
        return null;
    }

    @Override
    public CompletableFuture<List<DynamoItem>> query(final DatabaseQueryKey key) {
        return null;
    }

    @Override
    public CompletableFuture<List<DynamoItem>> queryGlobal(final Class<? extends Table> type, final String value) {
        return null;
    }

    @Override
    public CompletableFuture<List<DynamoItem>> querySecondary(final Class<? extends Table> type, final String organisationId, final String value) {
        return null;
    }

    @Override
    public <T extends Table> CompletableFuture<T> link(final String organisationId, final T entry, final Class<? extends Table> class1, final List<String> groupIds) {
        return null;
    }

    @Override
    public int maxBatchSize() {
        return 0;
    }

    @Override
    public String newId() {
        return null;
    }
}
