/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.fleetpin.graphql.dynamodb.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dataloader.DataLoader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.fleetpin.graphql.dynamodb.manager.DynamoDbImpl.table;

public final class InMemoryDynamoDb implements DynamoDb {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<DatabaseKey, DynamoItem> map;
    private final Supplier<String> idGenerator;

    public InMemoryDynamoDb(final Supplier<String> idGenerator) {
        this.map = new ConcurrentHashMap<>();
        this.idGenerator = idGenerator;
    }

    @Override
    public <T extends Table> CompletableFuture<T> delete(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            if (!organisationId.equals(entity.getSourceOrganistaionId())) {
                return entity;
            }

            map.remove(new DatabaseKey(organisationId, entity.getClass(), entity.getId()));

            return entity;
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> deleteLinks(final String organisationId, final T entity) {
        return null;
    }

    @Override
    public <T extends Table> CompletableFuture<T> put(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            if (entity.getId() == null) {
                entity.setId(newId());
            }

            final var links = AttributeValue.builder().m(entity.getLinks()
                    .entries()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            value -> AttributeValue.builder().ss(value.getValue()).build()
                    ))
            ).build();

            final var item = new HashMap<String, AttributeValue>();
            item.put("organisationId", AttributeValue.builder().s(organisationId).build());
            item.put("id", createTableNamedKey(entity, entity.getId()));
            item.put("item", TableUtil.toAttributes(objectMapper, entity));
            item.put("links", links);
            appendSecondaryItemFields(entity, item);

            final var databaseKey = new DatabaseKey(organisationId, entity.getClass(), entity.getId());
            final var dynamoItem = new DynamoItem(entity.getSourceTable(), item);

            map.put(databaseKey, dynamoItem);

            return entity;
        });
    }

    @Override
    public CompletableFuture<List<DynamoItem>> get(final List<DatabaseKey> keys) {
        return CompletableFuture.supplyAsync(() -> getWithFilter(entry -> keys.stream().anyMatch(key -> foundInMap(entry, key))));
    }

    @Override
    public CompletableFuture<List<DynamoItem>> getViaLinks(final String organisationId, final Table entry, final Class<? extends Table> type, final DataLoader<DatabaseKey, DynamoItem> items) {
        return null;
    }

    @Override
    public CompletableFuture<List<DynamoItem>> query(final DatabaseQueryKey key) {
        return CompletableFuture.supplyAsync(() -> getWithFilter(entry -> foundInMap(entry, key)));
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
        return 50 / map.size();
    }

    @Override
    public String newId() {
        return idGenerator.get();
    }

    private <T extends Table> AttributeValue createTableNamedKey(final T entity, final String id) {
        final var tableName = table(entity.getClass());
        return AttributeValue.builder().s(tableName + ":" + id).build();
    }

    private <T extends Table> void appendSecondaryItemFields(final T entity, final HashMap<String, AttributeValue> item) {
        final var secondaryGlobal = TableUtil.getSecondaryGlobal(entity);
        if (secondaryGlobal != null) {
            item.put("secondaryGlobal", createTableNamedKey(entity, secondaryGlobal));
        }

        final var secondaryOrganisation = TableUtil.getSecondaryOrganisation(entity);
        if (secondaryOrganisation != null) {
            item.put("secondaryOrganisation", createTableNamedKey(entity, secondaryOrganisation));
        }
    }

    private List<DynamoItem> getWithFilter(final Predicate<Map.Entry<DatabaseKey, DynamoItem>> filterPredicate) {
        return map.entrySet()
                .stream()
                .filter(filterPredicate)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private boolean foundInMap(final Map.Entry<DatabaseKey, DynamoItem> entry, final DatabaseKey key) {
        return key.equals(entry.getKey()) ||
                (entry.getKey().getOrganisationId().equals("global") && key.getId().equals(entry.getKey().getId()));
    }

    private boolean foundInMap(final Map.Entry<DatabaseKey, DynamoItem> entry, final DatabaseQueryKey key) {
        return key.getType().equals(entry.getKey().getType()) &&
                (entry.getKey().getOrganisationId().equals("global") || key.getOrganisationId().equals(entry.getKey().getOrganisationId()));
    }
}
