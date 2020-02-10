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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.fleetpin.graphql.dynamodb.manager.DynamoDbImpl.table;

public final class InMemoryDynamoDb implements DynamoDb {
    public static final ObjectMapper objectMapper = new ObjectMapper();
    private final ConcurrentHashMap<DatabaseKey, DynamoItem> map;

    public InMemoryDynamoDb() {
        this.map = new ConcurrentHashMap<>();
    }

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
        return CompletableFuture.supplyAsync(() -> {
            final var databaseKey = new DatabaseKey(organisationId, entity.getClass(), entity.getId());

            final var item = new HashMap<String, AttributeValue>();
            item.put("organisationId", AttributeValue.builder().s(organisationId).build());
            item.put("id", AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build());
            item.put("item", TableUtil.toAttributes(objectMapper, entity));
            item.put(
                    "links",
                    AttributeValue.builder().m(entity.getLinks()
                            .entries()
                            .stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    value -> AttributeValue.builder().ss(value.getValue()).build()
                            ))
                    ).build()
            );
            final var dynamoItem = new DynamoItem(entity.getSourceTable(), item);

            map.put(databaseKey, dynamoItem);

            return entity;
        });
    }

    @Override
    public CompletableFuture<List<DynamoItem>> get(final List<DatabaseKey> keys) {
        return CompletableFuture.supplyAsync(() -> {
            final var items = new ArrayList<DynamoItem>();

            map.forEach((key, value) -> items.add(value));

            return items;
        });
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
