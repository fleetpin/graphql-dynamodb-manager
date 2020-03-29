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

package com.fleetpin.graphql.database.manager.dynamo;

import static com.fleetpin.graphql.database.manager.util.TableCoreUtil.table;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.*;

import com.fleetpin.graphql.database.manager.util.CompletableFutureUtil;
import com.fleetpin.graphql.database.manager.util.TableCoreUtil;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.utils.CompletableFutureUtils;

public final class DynamoDb extends DatabaseDriver {
    private final AttributeValue GLOBAL = AttributeValue.builder().s("global").build();

    private final List<String> entityTables; //is in reverse order so easy to over ride as we go through
    private final String entityTable;
    private final DynamoDbAsyncClient client;
    private final ObjectMapper mapper;
    private final Supplier<String> idGenerator;

    public DynamoDb(ObjectMapper mapper, List<String> entityTables, DynamoDbAsyncClient client, Supplier<String> idGenerator) {
        this.mapper = mapper;
        this.entityTables = entityTables;
        this.entityTable = entityTables.get(entityTables.size() - 1);
        this.client = client;
        this.idGenerator = idGenerator;
    }

    public <T extends Table> CompletableFuture<T> delete(String organisationId, T entity) {


        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();


        String sourceOrganisation = getSourceOrganistaionId(entity);

        if (!sourceOrganisation.equals(organisationId)) {
            //trying to delete a global or something just return without doing anything
            return CompletableFuture.completedFuture(entity);
        }


        String sourceTable = getSourceTable(entity);
        if (sourceTable.equals(entityTable)) {

            Map<String, AttributeValue> key = new HashMap<>();
            key.put("organisationId", organisationIdAttribute);
            key.put("id", id);

            return client.deleteItem(request -> request.tableName(entityTable).key(key)).thenApply(response -> {
                return entity;
            });
        } else {
            //we mark as deleted not actual delete
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("organisationId", organisationIdAttribute);
            item.put("id", id);
            item.put("deleted", AttributeValue.builder().bool(true).build());

            return client.putItem(request -> request.tableName(entityTable).item(item)).thenApply(response -> {
                return entity;
            });
        }
    }

    public <T extends Table> CompletableFuture<T> put(String organisationId, T entity) {

        if (entity.getId() == null) {
            entity.setId(idGenerator.get());
            setCreatedAt(entity, Instant.now());
        }
        if (entity.getCreatedAt() == null) {
            setCreatedAt(entity, Instant.now()); //if missing for what ever reason
        }
        setUpdatedAt(entity, Instant.now());
        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("organisationId", organisationIdAttribute);
        item.put("id", id);
        item.put("item", TableUtil.toAttributes(mapper, entity));


        Map<String, AttributeValue> links = new HashMap<>();
        getLinks(entity).asMap().forEach((table, link) -> {
            if (!link.isEmpty()) {
                links.put(table, AttributeValue.builder().ss(link).build());
            }
        });

        item.put("links", AttributeValue.builder().m(links).build());
        setSource(entity, entityTable, getLinks(entity), organisationId);

        String secondaryOrganisation = TableUtil.getSecondaryOrganisation(entity);
        String secondaryGlobal = TableUtil.getSecondaryGlobal(entity);


        if (secondaryGlobal != null) {
            var index = AttributeValue.builder().s(table(entity.getClass()) + ":" + secondaryGlobal).build();
            item.put("secondaryGlobal", index);
        }
        if (secondaryOrganisation != null) {
            var index = AttributeValue.builder().s(table(entity.getClass()) + ":" + secondaryOrganisation).build();
            item.put("secondaryOrganisation", index);
        }

        return client.putItem(request -> request.tableName(entityTable).item(item)).thenApply(response -> {
            return entity;
        });

    }

    @Override
    public int maxBatchSize() {
        return 50 / entityTables.size();
    }


    @Override
    public <T extends Table> CompletableFuture<List<T>> get(List<DatabaseKey<T>> keys) {
        List<Map<String, AttributeValue>> entries = new ArrayList<>(keys.size() * 2);

        keys.forEach(key -> {
            AttributeValue value = AttributeValue.builder().s(table(key.getType()) + ":" + key.getId()).build();
            if (key.getOrganisationId() != null) {
                var organisation = new HashMap<String, AttributeValue>();
                organisation.put("id", value);
                organisation.put("organisationId", AttributeValue.builder().s(key.getOrganisationId()).build());
                entries.add(organisation);
            } else {
                System.out.println("null organisation " + key.getType());
            }
            var global = new HashMap<String, AttributeValue>();
            global.put("id", value);
            global.put("organisationId", GLOBAL);
            entries.add(global);
        });

        Map<String, KeysAndAttributes> items = new HashMap<>();

        for (String table : this.entityTables) {
            items.put(table, KeysAndAttributes.builder().keys(entries).consistentRead(true).build());
        }
        return client.batchGetItem(builder -> builder.requestItems(items)).thenApply(response -> {
            var responseItems = response.responses();

            var flattener = new Flatterner(false);
            entityTables.forEach(table -> {
                flattener.add(table, responseItems.get(table));
            });
            var toReturn = new ArrayList<T>();
            for (var key : keys) {

                var item = flattener.get(key.getId());
                if (item == null) {
                    toReturn.add(null);
                } else {
                    toReturn.add(item.convertTo(mapper, key.getType()));
                }
            }
            return toReturn;
        });
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> getViaLinks(String organisationId, Table entry, Class<T> type, TableDataLoader<DatabaseKey<Table>> items) {
        String tableTarget = table(type);
        var links = getLinks(entry).get(tableTarget);
        Class<Table> query = (Class<Table>) type;
        List<DatabaseKey<Table>> keys = links.stream().map(link -> createDatabaseKey(organisationId, query, link)).collect(Collectors.toList());
        return items.loadMany(keys);
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> query(DatabaseQueryKey<T> key) {
        var organisationId = AttributeValue.builder().s(key.getOrganisationId()).build();
        String prefix = Optional.ofNullable(key.getQuery().getStartsWith()).orElse("");
        var id = AttributeValue.builder().s(table(key.getQuery().getType()) + ":" + prefix).build();

        var futures = entityTables.stream()
                .flatMap(table -> Stream.of(Map.entry(table, GLOBAL), Map.entry(table, organisationId)))
                .map(pair -> query(pair.getKey(), pair.getValue(), id, key.getQuery()));

        var future = CompletableFutureUtil.sequence(futures);

        return future.thenApply(results -> {
            var flattener = new Flatterner(false);

            results.forEach(list -> flattener.addItems(list));
            return flattener.results(mapper, key.getQuery().getType(), Optional.ofNullable(key.getQuery().getLimit()));
        });
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> queryGlobal(Class<T> type, String value) {
        var id = AttributeValue.builder().s(table(type) + ":" + value).build();

        CompletableFuture<List<List<DynamoItem>>> future = CompletableFuture.completedFuture(new ArrayList<>());
        for (var table : entityTables) {
            future = future.thenCombine(queryGlobal(table, id), (a, b) -> {
                a.add(b);
                return a;
            });
        }
        return future.thenApply(results -> {
            var flattener = new Flatterner(true);
            results.forEach(list -> flattener.addItems(list));
            return flattener.results(mapper, type);
        });

    }

    private CompletableFuture<List<DynamoItem>> queryGlobal(String table, AttributeValue id) {
        Map<String, AttributeValue> keyConditions = new HashMap<>();
        keyConditions.put(":secondaryGlobal", id);

        var toReturn = new ArrayList<DynamoItem>();
        return client.queryPaginator(r -> r.tableName(table).indexName("secondaryGlobal")
                .keyConditionExpression("secondaryGlobal = :secondaryGlobal")
                .expressionAttributeValues(keyConditions)
        ).subscribe(response -> {
            response.items().forEach(item -> toReturn.add(new DynamoItem(table, item)));
        }).thenApply(__ -> {
            return toReturn;
        });
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> querySecondary(Class<T> type, String organisationId, String value, TableDataLoader<DatabaseKey<Table>> item) {
        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        var id = AttributeValue.builder().s(table(type) + ":" + value).build();

        CompletableFuture<Set<String>> future = CompletableFuture.completedFuture(new HashSet<>());
        for (var table : entityTables) {
            future = future.thenCombine(querySecondary(table, organisationIdAttribute, id), (a, b) -> {
                a.addAll(b);
                return a;
            });
        }

        return future.thenCompose(results -> {
            List<DatabaseKey<Table>> keys = results.stream().map(i -> (DatabaseKey<Table>) createDatabaseKey(organisationId, type, i)).collect(Collectors.toList());
            return item.loadMany(keys);
        });

    }

    private CompletableFuture<List<String>> querySecondary(String table, AttributeValue organisationId, AttributeValue id) {

        Map<String, AttributeValue> keyConditions = new HashMap<>();
        keyConditions.put(":organisationId", organisationId);
        keyConditions.put(":secondaryOrganisation", id);

        var toReturn = new ArrayList<String>();
        return client.queryPaginator(r -> r.tableName(table).indexName("secondaryOrganisation")
                .keyConditionExpression("organisationId = :organisationId AND secondaryOrganisation = :secondaryOrganisation")
                .expressionAttributeValues(keyConditions)
                .projectionExpression("id")
        ).subscribe(response -> {
            response.items().stream().map(item -> item.get("id").s()).map(itemId -> {
                return itemId.substring(itemId.indexOf(':') + 1); //Id contains entity name
            }).forEach(toReturn::add);
        }).thenApply(__ -> {
            return toReturn;
        });
    }

    private CompletableFuture<List<DynamoItem>> query(String table, AttributeValue organisationId, AttributeValue id, Query<?> query) {

        Map<String, AttributeValue> keyConditions = new HashMap<>();
        keyConditions.put(":organisationId", organisationId);
        keyConditions.put(":table", id);

        var s = new DynamoQuerySubscriber(table, query.getLimit());

       client.queryPaginator(r -> {
            r.tableName(table)
                    .consistentRead(true)
                    .keyConditionExpression("organisationId = :organisationId AND begins_with(id, :table)")
                    .expressionAttributeValues(keyConditions)
                    .applyMutation(b -> {
                        if (query.getLimit() != null) {
                            b.limit(query.getLimit());
                        }

                        if (query.getAfter() != null) {
                            b.exclusiveStartKey(Map.of(
                                    "id", AttributeValue.builder().s(TableCoreUtil.table(query.getType()) + ":" + query.getAfter()).build(),
                                    "organisationId", organisationId));
                        }
                    });
        }).subscribe(s);

        return s.getFuture();
    }

    @Override
    public <T extends Table> CompletableFuture<T> link(String organisationId, T entity, Class<? extends Table> class1, List<String> groupIds) {

        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        String source = table(entity.getClass());

        String target = table(class1);
        CompletableFuture<?> future = CompletableFuture.completedFuture(null);

        var existing = getLinks(entity).get(target);

        var toAdd = new HashSet<>(groupIds);
        toAdd.removeAll(existing);

        var toRemove = new HashSet<>(existing);
        toRemove.removeAll(groupIds);
        for (String targetId : toRemove) {
            var targetIdAttribute = AttributeValue.builder().s(target + ":" + targetId).build();
            Map<String, AttributeValue> targetKey = new HashMap<>();
            targetKey.put("organisationId", organisationIdAttribute);
            targetKey.put("id", targetIdAttribute);

            Map<String, AttributeValue> v = new HashMap<>();
            v.put(":val", AttributeValue.builder().ss(entity.getId()).build());

            Map<String, String> k = new HashMap<>();
            k.put("#table", source);

            var destination = client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("DELETE links.#table :val").expressionAttributeNames(k).expressionAttributeValues(v));
            future = future.thenCombine(destination, (a, b) -> b);
        }


        for (String targetId : toAdd) {
            var targetIdAttribute = AttributeValue.builder().s(target + ":" + targetId).build();
            Map<String, AttributeValue> targetKey = new HashMap<>();
            targetKey.put("organisationId", organisationIdAttribute);
            targetKey.put("id", targetIdAttribute);

            Map<String, AttributeValue> v = new HashMap<>();
            v.put(":val", AttributeValue.builder().ss(entity.getId()).build());

            Map<String, String> k = new HashMap<>();
            k.put("#table", source);

            var destination = client.updateItem(request -> request.tableName(entityTable).key(targetKey).conditionExpression("attribute_exists(links)").updateExpression("ADD links.#table :val").expressionAttributeNames(k).expressionAttributeValues(v))
                    .handle((r, e) -> {
                        if (e != null) {
                            if (e.getCause() instanceof ConditionalCheckFailedException) {
                                Map<String, AttributeValue> m = new HashMap<>();
                                m.put(source, AttributeValue.builder().ss(entity.getId()).build());
                                v.put(":val", AttributeValue.builder().m(m).build());
                                return client.updateItem(request -> request.tableName(entityTable).key(targetKey).conditionExpression("attribute_not_exists(links)").updateExpression("SET links = :val").expressionAttributeValues(v));
                            } else {
                                throw new RuntimeException(e);
                            }
                        }
                        return CompletableFuture.completedFuture(null);
                    }).thenCompose(a -> a)
                    .handle((r, e) -> { //nasty if attribute now exists use first approach again...
                        if (e != null) {
                            if (e.getCause() instanceof ConditionalCheckFailedException) {
                                return client.updateItem(request -> request.tableName(entityTable).key(targetKey).conditionExpression("attribute_exists(links)").updateExpression("ADD links.#table :val").expressionAttributeNames(k).expressionAttributeValues(v));
                            } else {
                                throw new RuntimeException(e);
                            }
                        }
                        return CompletableFuture.completedFuture(null);
                    }).thenCompose(a -> a);


            future = future.thenCombine(destination, (a, b) -> b);
        }
        var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("organisationId", organisationIdAttribute);
        key.put("id", id);

        Map<String, String> k = new HashMap<>();
        k.put("#table", target);

        Map<String, AttributeValue> values = new HashMap<>();
        if (groupIds.isEmpty()) {
            values.put(":val", AttributeValue.builder().nul(true).build());
        } else {
            values.put(":val", AttributeValue.builder().ss(groupIds).build());
        }
        setLinks(entity, target, groupIds);

        var destination = client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_exists(links)").updateExpression("SET links.#table = :val").expressionAttributeNames(k).expressionAttributeValues(values))
                .handle((r, e) -> {
                    if (e != null) {
                        if (e.getCause() instanceof ConditionalCheckFailedException) {
                            Map<String, AttributeValue> m = new HashMap<>();
                            m.put(target, values.get(":val"));
                            values.put(":val", AttributeValue.builder().m(m).build());
                            return client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_not_exists(links)").updateExpression("SET links = :val").expressionAttributeValues(values));
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                }).thenCompose(a -> a)
                .handle((r, e) -> {
                    if (e != null) {
                        if (e.getCause() instanceof ConditionalCheckFailedException) {
                            return client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_exists(links)").updateExpression("SET links.#table = :val").expressionAttributeNames(k).expressionAttributeValues(values));
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                    return CompletableFuture.completedFuture(null);
                }).thenCompose(a -> a);
        future = future.thenCombine(destination, (a, b) -> b);
        return future.thenApply(response -> {
            return entity;
        });
    }


    public <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity) {
        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        String source = table(entity.getClass());

        CompletableFuture<?> future = CompletableFuture.completedFuture(null);


        for (var link : getLinks(entity).entries()) {
            var targetIdAttribute = AttributeValue.builder().s(link.getKey() + ":" + link.getValue()).build();
            Map<String, AttributeValue> targetKey = new HashMap<>();
            targetKey.put("organisationId", organisationIdAttribute);
            targetKey.put("id", targetIdAttribute);

            Map<String, AttributeValue> v = new HashMap<>();
            v.put(":val", AttributeValue.builder().ss(entity.getId()).build());

            Map<String, String> k = new HashMap<>();
            k.put("#table", source);

            var destination = client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("DELETE links.#table :val").expressionAttributeNames(k).expressionAttributeValues(v));
            future = future.thenCombine(destination, (a, b) -> b);
        }
        getLinks(entity).clear();
        return future.thenApply(__ -> entity);
    }

    @Override
    public String newId() {
        return idGenerator.get();
    }

}
