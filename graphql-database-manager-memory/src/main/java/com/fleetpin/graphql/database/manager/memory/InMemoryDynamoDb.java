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
import com.fasterxml.jackson.databind.node.*;
import com.fleetpin.graphql.database.manager.*;
import com.fleetpin.graphql.database.manager.dynamo.Flatterner;
import com.google.common.collect.Multimap;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;

import org.dataloader.DataLoader;

import java.time.Instant;
import java.util.ArrayList;
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
import static com.fleetpin.graphql.database.manager.util.TableCoreUtil.table;

public final class InMemoryDynamoDb extends DatabaseDriver {
    private static final String SECONDARY_GLOBAL = "secondaryGlobal";
    private static final String SECONDARY_ORGANISATION = "secondaryOrganisation";
    private final ObjectMapper objectMapper;
    private final JsonNodeFactory factory;
    private final List<ConcurrentHashMap<DatabaseKey, MemoryItem>> tables;
    private final ConcurrentHashMap<DatabaseKey, MemoryItem> entityTable;
    private final Supplier<String> idGenerator;

    public InMemoryDynamoDb(
            final ObjectMapper objectMapper,
            final JsonNodeFactory factory,
            final List<ConcurrentHashMap<DatabaseKey, MemoryItem>> tables,
            final Supplier<String> idGenerator
    ) {
        this.objectMapper = objectMapper;
        this.factory = factory;
        this.tables = tables;
        this.idGenerator = idGenerator;
    }

    @Override
    public <T extends Table> CompletableFuture<T> delete(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            if (!organisationId.equals(getSourceOrganistaionId(entity))) {
                return entity;
            }
            
    		String sourceTable = getSourceTable(entity);
    		if(sourceTable.equals(entityTable)) {
                entityTable.remove(createDatabaseKey(organisationId, entity.getClass(), entity.getId()));
    		}else {
                entityTable.put(createDatabaseKey(organisationId, entity.getClass(), entity.getId()), MemoryItem.deleted());
    		}
            return entity;
        });
    }

    @Override
    public <T extends Table> CompletableFuture<T> deleteLinks(final String organisationId, final T entity) {
        return CompletableFuture.supplyAsync(() -> {
            final var databaseKey = createDatabaseKey(organisationId, entity.getClass(), entity.getId());
            final var item = entityTable.get(databaseKey);
            item.deleteLinks();
            //be more efficient and remove links that are marked above instead of finding all reverse links
            entityTable.forEach((key, value) -> value.deleteLinksTo(item));
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

            
            final var links = factory.objectNode();
            getLinks(entity).entries().forEach(entry -> {
                links.put(entry.getKey(), entry.getValue());
            });
            //may want copy so db state is constant is it possible?
            final var item = new MemoryItem(getLinks(entity), entity);
            final var databaseKey = createDatabaseKey(organisationId, entity.getClass(), entity.getId());
            entityTable.put(databaseKey, item);
            return entity;
        });
    }
    
    private MemoryItem itemMerger(MemoryItem first, MemoryItem second) {
    	return second; //actually merge
    }

    private static class MergeKey {
    	private final Class<?> type;
    	private final String id;
		public MergeKey(DatabaseKey<?> key) {
			this.type = key.getType();
			this.id = key.getId();
		}
		@Override
		public int hashCode() {
			return Objects.hash(id, type);
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MergeKey other = (MergeKey) obj;
			return Objects.equals(id, other.id) && Objects.equals(type, other.type);
		}
		
		
    	
    	
    }
    @Override
    public <T extends Table> CompletableFuture<List<T>> get(List<DatabaseKey<T>> keys) {
    	
    	Map<MergeKey, MemoryItem> items = new HashMap<>();

    	for(var map: tables) {
    		for(var key: keys) {
    			var item = map.get(createDatabaseKey("global", key.getType(), key.getId()));
    			items.merge(new MergeKey(key), item, this::itemMerger);
    		}
    	}
    	for(var map: tables) {
    		for(var key: keys) {
    			var item = map.get(key);
    			items.merge(new MergeKey(key), item, this::itemMerger);
    		}
    	}
    	
    	List<T> toReturn = new ArrayList<>(keys.size());
    	for(var key: keys) {
    		var item = items.get(new MergeKey(key));
    		if(item != null && !item.isDeleted()) {
    			toReturn.add(item.getEntity());
    		}else {
    			toReturn.add(null);
    		}
    	}
    	return CompletableFuture.completedFuture(toReturn);
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> getViaLinks(
            final String organisationId,
            final Table entry,
            final Class<? extends Table> type,
            final DataLoader<DatabaseKey, T> items
    ) {
        final var tableTarget = table(type);
        final var links = getLinks(entry).get(tableTarget);
        final var keys = links.stream()
                .map(link -> createDatabaseKey(organisationId, type, link))
                .collect(Collectors.toList());

        return items.loadMany(keys);
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> query(final DatabaseQueryKey key) {
        return CompletableFuture.supplyAsync(() -> getWithFilter(entry -> foundInMap(entry, key)));
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> queryGlobal(final Class<? extends Table> type, final String value) {
        return CompletableFuture.supplyAsync(() -> {
            final var tableName = createTableNamedKey(type, value);

            return getWithFilter(entry -> getItem(entry.getValue())
                    .get(SECONDARY_GLOBAL)
                    .equals(tableName)
            );
        });
    }

    @Override
    public <T extends Table> CompletableFuture<List<T>> querySecondary(
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

    private TextNode createTableNamedKey(final Class<? extends Table> type, final String id) {
        final var tableName = table(type);
        return T.builder().s(tableName + ":" + id).build();
    }

    private <T extends Table> void appendSecondaryItemFields(final T entity, final HashMap<String, TextNode> item) {
        final var secondaryGlobal = entity;
        if (secondaryGlobal != null) {
            item.put(SECONDARY_GLOBAL, createTableNamedKey(entity.getClass(), secondaryGlobal));
        }

        final var secondaryOrganisation = getSecondaryOrganisation(entity);
        if (secondaryOrganisation != null) {
            item.put(SECONDARY_ORGANISATION, createTableNamedKey(entity.getClass(), secondaryOrganisation));
        }
    }

    private <T extends Table> List<T> getWithFilter(final Predicate<Map.Entry<DatabaseKey, T>> filterPredicate) {
        return map.entrySet()
                .stream()
                .filter(filterPredicate)
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    private <T extends Table> boolean foundInMap(final Map.Entry<DatabaseKey, T> entry, final DatabaseKey key) {
        return key.equals(entry.getKey()) ||
                (entry.getKey().getOrganisationId().equals("global") && key.getId().equals(entry.getKey().getId()));
    }

    private <T extends Table> boolean foundInMap(final Map.Entry<DatabaseKey, T> entry, final DatabaseQueryKey key) {
        return key.getType().isAssignableFrom(entry.getKey().getType()) &&
                (entry.getKey().getOrganisationId().equals("global") || key.getOrganisationId().equals(entry.getKey().getOrganisationId()));
    }
}
