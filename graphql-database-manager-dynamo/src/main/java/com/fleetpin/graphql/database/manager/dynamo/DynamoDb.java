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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.*;
import com.fleetpin.graphql.database.manager.util.CompletableFutureUtil;
import com.fleetpin.graphql.database.manager.util.TableCoreUtil;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fleetpin.graphql.database.manager.util.TableCoreUtil.table;

public class DynamoDb extends DatabaseDriver {
    private static final AttributeValue REVISION_INCREMENT = AttributeValue.builder().n("1").build();
    private static final AttributeValue GLOBAL = AttributeValue.builder().s("global").build();
    private static final int BATCH_WRITE_SIZE = 25;

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

            return client.deleteItem(request -> request.tableName(entityTable).key(key).applyMutation(mutator -> {
                
              
                  String sourceOrganisationId = getSourceOrganistaionId(entity);
                  if (!sourceOrganisationId.equals(organisationId)) {
                      return;
                  }
                  if(entity.getRevision() == 0) { //we confirm row does not exist with a revision since entry might predate feature
                      mutator.conditionExpression("attribute_not_exists(revision)");
                  }else {
                      Map<String, AttributeValue> variables = new HashMap<>();
                      variables.put(":revision", AttributeValue.builder().n(Long.toString(entity.getRevision())).build());
                      //check exists and matches revision
                      mutator.expressionAttributeValues(variables);
                      mutator.conditionExpression("revision = :revision");
                  }
                  
              }
                
            )).thenApply(response -> {
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

    public <T extends Table> CompletableFuture<T> put(String organisationId, T entity, boolean check) {

        if (entity.getId() == null) {
            entity.setId(idGenerator.get());
            setCreatedAt(entity, Instant.now());
        }
        if (entity.getCreatedAt() == null) {
            setCreatedAt(entity, Instant.now()); //if missing for what ever reason
        }
        final long revision = entity.getRevision();
        setUpdatedAt(entity, Instant.now());
        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("organisationId", organisationIdAttribute);
        item.put("id", id);
        var entries = TableUtil.toAttributes(mapper, entity);
        entries.remove("revision"); // needs to be at the top level as a limit on dynamo to be able to perform an atomic addition
        item.put("revision", AttributeValue.builder().n(Long.toString(revision + 1)).build()); 
        item.put("item", AttributeValue.builder().m(entries).build());

        Map<String, AttributeValue> links = new HashMap<>();
        getLinks(entity).asMap().forEach((table, link) -> {
            if (!link.isEmpty()) {
                links.put(table, AttributeValue.builder().ss(link).build());
            }
        });

        String sourceTable = getSourceTable(entity);
        
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
        return client.putItem(request -> request.tableName(entityTable).item(item).applyMutation(mutator -> {
            if(check) {
                String sourceOrganisationId = getSourceOrganistaionId(entity);
               
                if(sourceTable != null && !sourceTable.equals(entityTable) || !sourceOrganisationId.equals(organisationId) || revision == 0) { //we confirm row does not exist with a revision since entry might predate feature
                    mutator.conditionExpression("attribute_not_exists(revision)");
                }else {
                    Map<String, AttributeValue> variables = new HashMap<>();
                    variables.put(":revision", AttributeValue.builder().n(Long.toString(revision)).build());
                    //check exists and matches revision
                    mutator.expressionAttributeValues(variables);
                    mutator.conditionExpression("revision = :revision");
                }
                
            }
        })).exceptionally(failure -> {
            if(failure.getCause() instanceof ConditionalCheckFailedException) {
                throw new RevisionMismatchException(failure.getCause());
            }
            Throwables.throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }).thenApply(response -> {
            entity.setRevision(revision + 1);
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

                var item = flattener.get(key.getType(), key.getId());
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
    
    private CompletableFuture<?> removeLinks(AttributeValue organisationIdAttribute, String fromTable, Set<String> fromIds, String targetTable, String targetId) {

        var targetIdAttribute = AttributeValue.builder().ss(targetId).build();
        var futures = fromIds.stream().map(fromId ->  {
            var fromIdAttribute = AttributeValue.builder().s(fromTable + ":" + fromId).build();
            Map<String, AttributeValue> targetKey = new HashMap<>();
            targetKey.put("organisationId", organisationIdAttribute);
            targetKey.put("id", fromIdAttribute);

            Map<String, AttributeValue> v = new HashMap<>();
            v.put(":val", targetIdAttribute);
            v.put(":revisionIncrement", REVISION_INCREMENT);


            Map<String, String> k = new HashMap<>();
            k.put("#table", targetTable);

            return client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("DELETE links.#table :val ADD revision :revisionIncrement").expressionAttributeNames(k).expressionAttributeValues(v));
        }).toArray(CompletableFuture[]::new);
        
        return CompletableFuture.allOf(futures);
    }
    
    private CompletableFuture<?> addLinks(AttributeValue organisationIdAttribute, String fromTable, Set<String> fromIds, String targetTable, String targetId) {
        
        var targetIdAttribute = AttributeValue.builder().ss(targetId).build();

        var futures = fromIds.stream().map(fromId ->  {
            var fromIdAttribute = AttributeValue.builder().s(fromTable + ":" + fromId).build();
            Map<String, AttributeValue> targetKey = new HashMap<>();
            targetKey.put("organisationId", organisationIdAttribute);
            targetKey.put("id", fromIdAttribute);

            Map<String, AttributeValue> v = new HashMap<>();
            v.put(":val", targetIdAttribute);
            v.put(":revisionIncrement", REVISION_INCREMENT);
            Map<String, String> k = new HashMap<>();
            k.put("#table", targetTable);

            return client.updateItem(request -> request.tableName(entityTable).key(targetKey).conditionExpression("attribute_exists(links)").updateExpression("ADD links.#table :val, revision :revisionIncrement").expressionAttributeNames(k).expressionAttributeValues(v))
                    .handle((r, e) -> {
                        if (e != null) {
                            if (e.getCause() instanceof ConditionalCheckFailedException) {
                                Map<String, AttributeValue> m = new HashMap<>();
                                m.put(targetTable, targetIdAttribute);
                                v.put(":val", AttributeValue.builder().m(m).build());
                                v.put(":revisionIncrement", REVISION_INCREMENT);

                                return client.updateItem(request -> request.tableName(entityTable).key(targetKey).conditionExpression("attribute_not_exists(links)").updateExpression("SET links = :val ADD revision :revisionIncrement").expressionAttributeValues(v));
                            } else {
                                throw new RuntimeException(e);
                            }
                        }
                        return CompletableFuture.completedFuture(r);
                    }).thenCompose(a -> a)
                    .handle((r, e) -> { //nasty if attribute now exists use first approach again...
                        if (e != null) {
                            if (e.getCause() instanceof ConditionalCheckFailedException) {
                                return client.updateItem(request -> request.tableName(entityTable).key(targetKey).conditionExpression("attribute_exists(links)").updateExpression("ADD links.#table :val, revision :revisionIncrement").expressionAttributeNames(k).expressionAttributeValues(v));
                            } else {
                                throw new RuntimeException(e);
                            }
                        }
                        return CompletableFuture.completedFuture(r);
                    }).thenCompose(a -> a);


        }).toArray(CompletableFuture[]::new);
        
        return CompletableFuture.allOf(futures);
    }
    
    private <T extends Table> CompletableFuture<T> updateEntityLinks(String organisationId, T entity, String targetTable, Collection<String> targetId) {
        var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("organisationId", organisationIdAttribute);
        key.put("id", id);

        Map<String, String> k = new HashMap<>();
        k.put("#table", targetTable);

        Map<String, AttributeValue> values = new HashMap<>();
        if (targetId.isEmpty()) {
            values.put(":val", AttributeValue.builder().nul(true).build());
            
        } else {
            values.put(":val", AttributeValue.builder().ss(targetId).build());
        }
        values.put(":revisionIncrement", REVISION_INCREMENT);
        

        String extraConditions;
        
        String sourceTable = getSourceTable(entity);
        String sourceOrganisationId = getSourceOrganistaionId(entity);
        //revision checks don't really work when reading from one env and writing to another, or read from global write to organisation.
        //revision would only practically be empty if reading object before revision concept is present
        if (sourceTable.equals(entityTable) && sourceOrganisationId.equals(organisationId) && entity.getRevision() != 0) {
            values.put(":revision", AttributeValue.builder().n(Long.toString(entity.getRevision())).build());
            extraConditions = " AND revision = :revision";
        }else {
            extraConditions = "";
        }

        var destination = client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_exists(links)" + extraConditions).updateExpression("SET links.#table = :val ADD revision :revisionIncrement").expressionAttributeNames(k).expressionAttributeValues(values).returnValues(ReturnValue.UPDATED_NEW))
                .handle((r, e) -> {
                    if (e != null) {
                        if (e.getCause() instanceof ConditionalCheckFailedException) {
                            Map<String, AttributeValue> m = new HashMap<>();
                            m.put(targetTable, values.get(":val"));
                            values.put(":val", AttributeValue.builder().m(m).build());
                            
                            return client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_not_exists(links)" + extraConditions).updateExpression("SET links = :val ADD revision :revisionIncrement").expressionAttributeValues(values).returnValues(ReturnValue.UPDATED_NEW));
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                    return CompletableFuture.completedFuture(r);
                }).thenCompose(a -> a)
                .handle((r, e) -> {
                    if (e != null) {
                        if (e.getCause() instanceof ConditionalCheckFailedException) {
                            return client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_exists(links)" + extraConditions).updateExpression("SET links.#table = :val ADD revision :revisionIncrement").expressionAttributeNames(k).expressionAttributeValues(values).returnValues(ReturnValue.UPDATED_NEW));
                        } else {
                            throw new RuntimeException(e);
                        }
                    }
                    return CompletableFuture.completedFuture(r);
                }).thenCompose(a -> a);
        return destination.thenApply(response -> {
            entity.setRevision(Long.parseLong(response.attributes().get("revision").n()));
            return entity;
        }).exceptionally(failure -> {
            if(failure.getCause() instanceof ConditionalCheckFailedException) {
                throw new RevisionMismatchException(failure.getCause());
            }
            Throwables.throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> link(String organisationId, T entity, Class<? extends Table> class1, List<String> groupIds) {

        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        String source = table(entity.getClass());

        String target = table(class1);
        var existing = getLinks(entity).get(target);

        var toAdd = new HashSet<>(groupIds);
        toAdd.removeAll(existing);

        var toRemove = new HashSet<>(existing);
        toRemove.removeAll(groupIds);

        var entityFuture = updateEntityLinks(organisationId, entity, target, groupIds);
        
        return entityFuture.thenCompose(e -> {
            
            //wait until the entity has been updated in-case that fails then update the other targets.
            
            //remove links that have been removed
            CompletableFuture<?> removeFuture = removeLinks(organisationIdAttribute, target, toRemove, source, entity.getId());
            //add the new links
            CompletableFuture<?> addFuture = addLinks(organisationIdAttribute, target, toAdd, source, entity.getId());
            
            return CompletableFuture.allOf(removeFuture, addFuture).thenApply(__ -> {
                 setLinks(entity, target, groupIds);
                return e;
            });
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> unlink(
            final String organisationId,
            final T entity,
            final Class<? extends Table> clazz,
            final String targetId
    ) {
        final var updateEntityLinksRequest = createRemoveLinkRequest(organisationId, entity, clazz, targetId);

        return client.updateItem(updateEntityLinksRequest)
                .thenCompose(ignore -> get(List.of(createDatabaseKey(organisationId, clazz, targetId))))
                .thenCompose(targetEntities -> {
                    if (targetEntities.isEmpty()) {
                        throw new RuntimeException("Could not find link on the target: " + targetId);
                    }

                    final var updateTargetLinksRequest = createRemoveLinkRequest(
                            organisationId,
                            targetEntities.get(0),
                            entity.getClass(),
                            entity.getId()
                    );

                    return client.updateItem(updateTargetLinksRequest);
                })
                .thenApply(ignore -> {
                    getLinks(entity).remove(table(clazz), targetId);

                    return entity;
                });
    }

    private <T extends Table> UpdateItemRequest createRemoveLinkRequest(
            final String organisationId,
            final T entity,
            final Class<? extends Table> clazz,
            final String targetId
    ) {
        final AttributeValue linkMap = AttributeValue.builder()
                .m(getLinks(entity).asMap()
                        .entrySet()
                        .stream()
                        .filter(entry -> !entry.getValue().contains(targetId) && !entry.getKey().equals(table(clazz)))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> AttributeValue.builder().ss(entry.getValue()).build()
                        ))).build();

        // It would be nice to pull the revision logic out
        final var revisionNumber = entity.getRevision() != 0 ?
                                   String.valueOf(entity.getRevision() + Integer.parseInt(REVISION_INCREMENT.n())) :
                                   "0";

        final var revision = AttributeValueUpdate.builder()
                .action(AttributeAction.PUT)
                .value(
                        AttributeValue.builder()
                                .n(revisionNumber)
                                .build()
                )
                .build();

        final var linksAttributeMap = Map.of(
                "revision", revision,
                "links", AttributeValueUpdate.builder().action(AttributeAction.PUT).value(linkMap).build()
        );

        final Map<String, AttributeValue> entityItem = Map.of(
                "organisationId", AttributeValue.builder().s(organisationId).build(),
                "id", AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build()
        );

        return UpdateItemRequest.builder()
                .tableName(entityTable)
                .key(entityItem)
                .attributeUpdates(linksAttributeMap)
                .build();
    }

    public <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity) {
        var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
        var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
        //we first clear out our own object

        long revision = entity.getRevision();
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":val", AttributeValue.builder().m(new HashMap<>()).build());
        values.put(":revisionIncrement", REVISION_INCREMENT);
        
        Map<String, AttributeValue> sourceKey = new HashMap<>();
        sourceKey.put("organisationId", organisationIdAttribute);
        sourceKey.put("id", id);
        
        var clearEntity = client.updateItem(request -> request.tableName(entityTable).key(sourceKey).updateExpression("SET links = :val ADD revision :revisionIncrement").returnValues(ReturnValue.UPDATED_NEW).applyMutation(mutator -> {
            String sourceTable = getSourceTable(entity);
            //revision checks don't really work when reading from one env and writing to another.
            if (sourceTable != null && !sourceTable.equals(entityTable)) {
                return;
            }
            String sourceOrganisationId = getSourceOrganistaionId(entity);
            if (!sourceOrganisationId.equals(organisationId)) {
                return;
            }
            if(revision == 0) { //we confirm row does not exist with a revision since entry might predate feature
                mutator.conditionExpression("attribute_not_exists(revision)");
            }else {
                values.put(":revision", AttributeValue.builder().n(Long.toString(entity.getRevision())).build());
                mutator.conditionExpression("revision = :revision");
            }
        }).expressionAttributeValues(values)).thenApply(response -> {
            entity.setRevision(Long.parseLong(response.attributes().get("revision").n()));
            return entity;
        }).exceptionally(failure -> {
            if(failure.getCause() instanceof ConditionalCheckFailedException) {
                throw new RevisionMismatchException(failure.getCause());
            }
            Throwables.throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        });
        
        //after we successfully clear out our object we clear the remote references
        return clearEntity.thenCompose(r -> {
            CompletableFuture<?> future = CompletableFuture.completedFuture(null);

            var val = AttributeValue.builder().ss(entity.getId()).build();
            String source = table(entity.getClass());
            for (var link : getLinks(entity).entries()) {
                var targetIdAttribute = AttributeValue.builder().s(link.getKey() + ":" + link.getValue()).build();
                Map<String, AttributeValue> targetKey = new HashMap<>();
                targetKey.put("organisationId", organisationIdAttribute);
                targetKey.put("id", targetIdAttribute);

                Map<String, AttributeValue> v = new HashMap<>();
                v.put(":val", val);
                v.put(":revisionIncrement", REVISION_INCREMENT);

                Map<String, String> k = new HashMap<>();
                k.put("#table", source);

                var destination = client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("DELETE links.#table :val ADD revision :revisionIncrement").expressionAttributeNames(k).expressionAttributeValues(v));
                future = future.thenCombine(destination, (a, b) -> b);
            }
            getLinks(entity).clear();
            return future.thenApply(__ -> r);
            
        });


    }

    @Override
    public CompletableFuture<Boolean> destroyOrganisation(final String organisationId) {
        final var organisationCondition = Condition.builder()
                .comparisonOperator(ComparisonOperator.EQ)
                .attributeValueList(AttributeValue.builder().s(organisationId).build())
                .build();

        final var associatedOrganisationItems = QueryRequest.builder()
                .tableName(entityTable)
                .keyConditions(Map.of("organisationId", organisationCondition))
                .build();

        final var deletedOrganisationFuture = new CompletableFuture<Boolean>();

        client.query(associatedOrganisationItems)
                .thenApply(response -> {
                    if (!response.hasItems()) {
                        deletedOrganisationFuture.complete(false);
                        return Stream.<CompletableFuture<BatchWriteItemResponse>>empty();
                    }

                    return Lists.partition(response.items()
                            .stream()
                            .map(item -> {
                                final var deleteRequest = DeleteRequest.builder().key(Map.of(
                                        "organisationId", item.get("organisationId"),
                                        "id", item.get("id")
                                )).build();

                                return WriteRequest.builder().deleteRequest(deleteRequest).build();
                            })
                            .collect(Collectors.toList()), BATCH_WRITE_SIZE)
                            .stream()
                            .map(deleteRequestBatch -> {
                                final var batchDeleteItemRequest = BatchWriteItemRequest.builder()
                                        .requestItems(Map.of(entityTable, deleteRequestBatch))
                                        .build();

                                return client.batchWriteItem(batchDeleteItemRequest);
                            });
                })
                .thenAccept(futures -> futures
                        .map(future -> future.thenApply(BatchWriteItemResponse::hasUnprocessedItems))
                        .reduce((a, b) -> a.thenCombine(b, (aBoolean, bBoolean) -> !aBoolean && !bBoolean))
                        .ifPresentOrElse(
                                future -> future.thenAccept(deletedOrganisationFuture::complete),
                                () -> deletedOrganisationFuture.complete(false)
                        ));

        return deletedOrganisationFuture;
    }

    @Override
    public String newId() {
        return idGenerator.get();
    }

}
