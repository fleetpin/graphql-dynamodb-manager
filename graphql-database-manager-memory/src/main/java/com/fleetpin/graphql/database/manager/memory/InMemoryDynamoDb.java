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

package com.fleetpin.graphql.database.manager.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.*;
import org.dataloader.DataLoader;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.fleetpin.graphql.database.manager.util.DynamoDbUtil.table;

public final class InMemoryDynamoDb extends AbstractDynamoDb {
    private static final String SECONDARY_GLOBAL = "secondaryGlobal";
    private static final String SECONDARY_ORGANISATION = "secondaryOrganisation";
    private final ObjectMapper objectMapper;
    private final ConcurrentHashMap<DatabaseKey, DynamoItem> map;
    private final Supplier<String> idGenerator;

    public InMemoryDynamoDb(
            final ObjectMapper objectMapper,
            final ConcurrentHashMap<DatabaseKey, DynamoItem> map,
            final Supplier<String> idGenerator
    ) {
        this.objectMapper = objectMapper;
        this.map = map;
        this.idGenerator = idGenerator;
    }

    @Override
    public <T extends Table> CompletableFuture<T> delete(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            if (!organisationId.equals(getSourceOrganistaionId(entity))) {
                return entity;
            }

            map.remove(createDatabaseKey(organisationId, entity.getClass(), entity.getId()));

            return entity;
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> deleteLinks(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            final var databaseKey = createDatabaseKey(organisationId, entity.getClass(), entity.getId());
            final var item = map.get(databaseKey);

            map.forEach((key, value) -> value.getLinks().get(table(entity.getClass())).clear());
            item.getLinks().clear();

            getLinks(entity).clear();
            return entity;
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> put(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            entity.setId(Objects.requireNonNullElseGet(entity.getId(), () -> {
                setCreatedAt(entity, Instant.now());
                return newId();
            }));

            setUpdatedAt(entity, Instant.now());

            final var links = AttributeValue.builder().m(getLinks(entity)
                    .entries()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            value -> AttributeValue.builder().ss(value.getValue()).build()
                    ))
            ).build();

            final var item = new HashMap<String, AttributeValue>();
            item.put("organisationId", AttributeValue.builder().s(organisationId).build());
            item.put("id", createTableNamedKey(entity.getClass(), entity.getId()));
            item.put("item", toAttributes(objectMapper, entity));
            item.put("links", links);
            appendSecondaryItemFields(entity, item);

            final var databaseKey = createDatabaseKey(organisationId, entity.getClass(), entity.getId());
            final var dynamoItem = createDynamoItem(getSourceTable(entity), item);

            map.put(databaseKey, dynamoItem);

            return entity;
        });
    }

    @Override
    public CompletableFuture<List<DynamoItem>> get(final List<DatabaseKey> keys) {
        return CompletableFuture.supplyAsync(() -> keys.stream()
                .map(key -> map.entrySet()
                        .stream()
                        .filter(entry -> foundInMap(entry, key))
                        .findFirst()
                        .map(Map.Entry::getValue)
                        .orElse(null))
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<DynamoItem>> getViaLinks(
            final String organisationId,
            final Table entry,
            final Class<? extends Table> type,
            final DataLoader<DatabaseKey, DynamoItem> items
    ) {
        final var tableTarget = table(type);
        final var links = getLinks(entry).get(tableTarget);
        final var keys = links.stream()
                .map(link -> createDatabaseKey(organisationId, type, link))
                .collect(Collectors.toList());

        return items.loadMany(keys);
    }

    @Override
    public CompletableFuture<List<DynamoItem>> query(final DatabaseQueryKey key) {
        return CompletableFuture.supplyAsync(() -> getWithFilter(entry -> foundInMap(entry, key)));
    }

    @Override
    public CompletableFuture<List<DynamoItem>> queryGlobal(final Class<? extends Table> type, final String value) {
        return CompletableFuture.supplyAsync(() -> {
            final var tableName = createTableNamedKey(type, value);

            return getWithFilter(entry -> getItem(entry.getValue())
                    .get(SECONDARY_GLOBAL)
                    .equals(tableName)
            );
        });
    }

    @Override
    public CompletableFuture<List<DynamoItem>> querySecondary(
            final Class<? extends Table> type,
            final String organisationId,
            final String value
    ) {
        return CompletableFuture.supplyAsync(() -> {
            final var tableName = createTableNamedKey(type, value);

            return getWithFilter(entry -> entry.getKey().getOrganisationId().equals(organisationId) &&
                    getItem(entry.getValue()).get(SECONDARY_ORGANISATION).equals(tableName));
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> link(
            final String organisationId,
            final T entry,
            final Class<? extends Table> class1,
            final List<String> groupIds
    ) {
        return CompletableFuture.supplyAsync(() -> {
            final var targetDatabaseKey = createDatabaseKey(organisationId, entry.getClass(), entry.getId());

            final var targetItem = map.get(targetDatabaseKey);
            final var targetTable = table(entry.getClass());

            final var linkTable = table(class1);
            final var targetLinks = targetItem.getLinks().get(linkTable);

            targetLinks.forEach(linkedId -> {
                final var linkedDatabaseKey = createDatabaseKey(organisationId, class1, linkedId);
                map.get(linkedDatabaseKey).getLinks().get(targetTable).clear();

                targetLinks.remove(linkedId);
            });

            groupIds.forEach(groupId -> {
                targetLinks.add(groupId);

                final var groupDatabaseKey = createDatabaseKey(organisationId, class1, groupId);
                map.get(groupDatabaseKey).getLinks().get(targetTable).add(targetItem.getId());
            });

            getLinks(entry).putAll(targetItem.getLinks());

            return entry;
        });
    }

    @Override
    public int maxBatchSize() {
        return 1;
    }

    @Override
    public String newId() {
        return idGenerator.get();
    }

    private AttributeValue createTableNamedKey(final Class<? extends Table> type, final String id) {
        final var tableName = table(type);
        return AttributeValue.builder().s(tableName + ":" + id).build();
    }

    private <T extends Table> void appendSecondaryItemFields(final T entity, final HashMap<String, AttributeValue> item) {
        final var secondaryGlobal = getSecondaryGlobal(entity);
        if (secondaryGlobal != null) {
            item.put(SECONDARY_GLOBAL, createTableNamedKey(entity.getClass(), secondaryGlobal));
        }

        final var secondaryOrganisation = getSecondaryOrganisation(entity);
        if (secondaryOrganisation != null) {
            item.put(SECONDARY_ORGANISATION, createTableNamedKey(entity.getClass(), secondaryOrganisation));
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
        return key.getType().isAssignableFrom(entry.getKey().getType()) &&
                (entry.getKey().getOrganisationId().equals("global") || key.getOrganisationId().equals(entry.getKey().getOrganisationId()));
    }
}
