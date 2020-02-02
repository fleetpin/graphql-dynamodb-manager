package com.fleetpin.graphql.dynamodb.manager;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.dataloader.DataLoader;

import com.fasterxml.jackson.databind.ObjectMapper;

import graphql.VisibleForTesting;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;

public class DynamoDbImpl implements DynamoDb {
	private final AttributeValue GLOBAL = AttributeValue.builder().s("global").build();

	private final List<String> entityTables; //is in reverse order so easy to over ride as we go through
	private final String entityTable;
	private final DynamoDbAsyncClient client;
	private final ObjectMapper mapper;
	private final Supplier<String> idGenerator;

	DynamoDbImpl(ObjectMapper mapper, List<String> entityTables, DynamoDbAsyncClient client, Supplier<String> idGenerator) {
		this.mapper = mapper;
		this.entityTables = entityTables;
		this.entityTable = entityTables.get(entityTables.size() - 1);
		this.client = client;
		this.idGenerator = idGenerator;
	}

	public <T extends Table> CompletableFuture<T> delete(String organisationId, T entity) {
		

		var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
		var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
		

		String sourceOrganisation = entity.getSourceOrganistaionId();
		
		if(!sourceOrganisation.equals(organisationId)) {
			//trying to delete a global or something just return without doing anything
			return CompletableFuture.completedFuture(entity);
		}
		
		
		String sourceTable = entity.getSourceTable();
		if(sourceTable.equals(entityTable)) {
			
			Map<String, AttributeValue> key = new HashMap<>();
			key.put("organisationId", organisationIdAttribute);
			key.put("id", id);
			
			return client.deleteItem(request -> request.tableName(entityTable).key(key)).thenApply(response -> {
				return entity;
			});
		}else {
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
		
		if(entity.getId() == null) {
			entity.setId(idGenerator.get());
			entity.setCreatedAt(Instant.now());
		}
		if(entity.getCreatedAt() == null) {
			entity.setCreatedAt(Instant.now()); //if missing for what ever reason
		}
		entity.setUpdatedAt(Instant.now());
		var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
		var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
		Map<String, AttributeValue> item = new HashMap<>();
		item.put("organisationId", organisationIdAttribute);
		item.put("id", id);
		item.put("item", TableUtil.toAttributes(mapper, entity));
		
		
		Map<String, AttributeValue> links = new HashMap<>();
		entity.getLinks().asMap().forEach((table, link) -> {
			if(!link.isEmpty()) {
				links.put(table, AttributeValue.builder().ss(link).build());
			}
		});

		
		item.put("links", AttributeValue.builder().m(links).build());
		entity.setSource(entityTable, entity.getLinks(), organisationId);

		String secondaryOrganisation = TableUtil.getSecondaryOrganisation(entity);
		String secondaryGlobal = TableUtil.getSecondaryGlobal(entity);
		
		
		if(secondaryGlobal != null) {
			var index = AttributeValue.builder().s(table(entity.getClass()) + ":" + secondaryGlobal).build();
			item.put("secondaryGlobal", index);
		}
		if(secondaryOrganisation != null) {
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


	public CompletableFuture<List<DynamoItem>> get(List<DatabaseKey> keys) {
		List<Map<String, AttributeValue>> entries = new ArrayList<>(keys.size() * 2);

		keys.forEach(key -> {
			AttributeValue value = AttributeValue.builder().s(table(key.getType()) + ":" + key.getId()).build();
			if(key.getOrganisationId() != null) {
    			var organisation = new HashMap<String, AttributeValue>();
    			organisation.put("id", value);
    			organisation.put("organisationId", AttributeValue.builder().s(key.getOrganisationId()).build());
    			entries.add(organisation);
			}else {
				System.out.println("null organisation " + key.getType());
			}
			var global = new HashMap<String, AttributeValue>();
			global.put("id", value);
			global.put("organisationId", GLOBAL);
			entries.add(global);
		});

		Map<String, KeysAndAttributes> items = new HashMap<>();

		for(String table: this.entityTables) {
			items.put(table, KeysAndAttributes.builder().keys(entries).consistentRead(true).build());
		}
		return client.batchGetItem(builder -> builder.requestItems(items)).thenApply(response -> {
			var responseItems = response.responses();

			var flattener = new Flatterner();
			entityTables.forEach(table -> {
				flattener.add(table, responseItems.get(table));
			});
			var toReturn = new ArrayList<DynamoItem>();
			for(var key: keys) {
				toReturn.add(flattener.get(key.getId()));
			}
			return toReturn;
		});
	}
	
	
	@Override
	public CompletableFuture<List<DynamoItem>> getViaLinks(String organisationId, Table entry, Class<? extends Table> type, DataLoader<DatabaseKey, DynamoItem> items) {
		String tableTarget = table(type);
		var links = entry.getLinks().get(tableTarget);
		var keys = links.stream().map(link -> new DatabaseKey(organisationId, type, link)).collect(Collectors.toList());
		return items.loadMany(keys);
	}

	public CompletableFuture<List<DynamoItem>> query(DatabaseQueryKey key) {
		var organisationId = AttributeValue.builder().s(key.getOrganisationId()).build();
		var id = AttributeValue.builder().s(table(key.getType())).build();
		
		CompletableFuture<List<List<DynamoItem>>> future = CompletableFuture.completedFuture(new ArrayList<>());
		for(var table: entityTables) {
			future = future.thenCombine(query(table, GLOBAL, id), (a, b) -> {
				a.add(b);
				return a;
			});
			future = future.thenCombine(query(table, organisationId, id), (a, b) -> {
				a.add(b);
				return a;
			});
		}
		
		return future.thenApply(results -> {
			var flattener = new Flatterner();
			results.forEach(list -> flattener.addItems(list));
			return flattener.results();
		});
		
	}
	
	public CompletableFuture<List<DynamoItem>> queryGlobal(Class<? extends Table> type, String value) {
		var id = AttributeValue.builder().s(table(type) + ":" + value).build();
		
		CompletableFuture<List<List<DynamoItem>>> future = CompletableFuture.completedFuture(new ArrayList<>());
		for(var table: entityTables) {
			future = future.thenCombine(queryGlobal(table, id), (a, b) -> {
				a.add(b);
				return a;
			});
		}
		return future.thenApply(results -> {
			var flattener = new Flatterner();
			results.forEach(list -> flattener.addItems(list));
			return flattener.results();
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
	public CompletableFuture<List<DynamoItem>> querySecondary(Class<? extends Table> type, String organisationId, String value) {
		var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
		var id = AttributeValue.builder().s(table(type) + ":" + value).build();
		
		CompletableFuture<List<List<DynamoItem>>> future = CompletableFuture.completedFuture(new ArrayList<>());
		for(var table: entityTables) {
			future = future.thenCombine(querySecondary(table, organisationIdAttribute, id), (a, b) -> {
				a.add(b);
				return a;
			});
		}
		return future.thenApply(results -> {
			var flattener = new Flatterner();
			results.forEach(list -> flattener.addItems(list));
			return flattener.results();
		});
		
	}
    private CompletableFuture<List<DynamoItem>> querySecondary(String table, AttributeValue organisationId, AttributeValue id) {
    		
    		Map<String, AttributeValue> keyConditions = new HashMap<>();
    		keyConditions.put(":organisationId", organisationId);
    		keyConditions.put(":secondaryOrganisation", id);
    
    		var toReturn = new ArrayList<DynamoItem>();
    		return client.queryPaginator(r -> r.tableName(table).indexName("secondaryOrganisation")
    				.keyConditionExpression("organisationId = :organisationId AND secondaryOrganisation = :secondaryOrganisation")
    				.expressionAttributeValues(keyConditions)
    		).subscribe(response -> {
    			response.items().forEach(item -> toReturn.add(new DynamoItem(table, item)));
    		}).thenApply(__ -> {
    			return toReturn;
    		});
    	}
	
	private CompletableFuture<List<DynamoItem>> query(String table, AttributeValue organisationId, AttributeValue id) {
		
		Map<String, AttributeValue> keyConditions = new HashMap<>();
		keyConditions.put(":organisationId", organisationId);
		keyConditions.put(":table", id);

		var toReturn = new ArrayList<DynamoItem>();
		return client.queryPaginator(r -> r.tableName(table)
				.consistentRead(true)
				.keyConditionExpression("organisationId = :organisationId AND begins_with(id, :table)")
				.expressionAttributeValues(keyConditions)
		).subscribe(response -> {
			response.items().forEach(item -> toReturn.add(new DynamoItem(table, item)));
		}).thenApply(__ -> {
			return toReturn;
		});
	}
	
	@Override
	public <T extends Table> CompletableFuture<T> link(String organisationId, T entity, Class<? extends Table> class1, List<String> groupIds) {
		
		var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
		String source = table(entity.getClass());

		String target = table(class1);
		CompletableFuture<?> future = CompletableFuture.completedFuture(null);
		
		var existing = entity.getLinks().get(target);
		
		var toAdd = new HashSet<>(groupIds);
		toAdd.removeAll(existing);
		
		var toRemove = new HashSet<>(existing);
		toRemove.removeAll(groupIds);
		for(String targetId: toRemove) {
			var targetIdAttribute = AttributeValue.builder().s(target + ":" + targetId).build();
			Map<String, AttributeValue> targetKey = new HashMap<>();
			targetKey.put("organisationId", organisationIdAttribute);
			targetKey.put("id", targetIdAttribute);
			
			Map<String, AttributeValue> v = new HashMap<>();
			v.put(":val", AttributeValue.builder().ss(entity.getId()).build());
			
			Map<String, String> k = new HashMap<>();
			k.put("#table", source);
			
			var destination = client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("DELETE links.#table :val").expressionAttributeNames(k).expressionAttributeValues(v));
			future = future.thenCombine(destination, (a,b) -> b);
		}
		
		
		for(String targetId: toAdd) {
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
						if(e != null) {
							if(e.getCause() instanceof ConditionalCheckFailedException) {
								Map<String, AttributeValue> m = new HashMap<>();
								m.put(source, AttributeValue.builder().ss(entity.getId()).build());
								v.put(":val", AttributeValue.builder().m(m).build());
								return client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("SET links = :val").expressionAttributeValues(v));
							}else {
								throw new RuntimeException(e);
							}
						}
						return CompletableFuture.completedFuture(null);
					}).thenCompose(a -> a);
			future = future.thenCombine(destination, (a,b) -> b);
		}
			var id = AttributeValue.builder().s(table(entity.getClass()) + ":" + entity.getId()).build();
			Map<String, AttributeValue> key = new HashMap<>();
			key.put("organisationId", organisationIdAttribute);
			key.put("id", id);
			
			Map<String, String> k = new HashMap<>();
			k.put("#table", target);
			
			Map<String, AttributeValue> values = new HashMap<>();
			if(groupIds.isEmpty()) {
				values.put(":val", AttributeValue.builder().nul(true).build());
			}else {
				values.put(":val", AttributeValue.builder().ss(groupIds).build());
			}
			entity.setLinks(target, groupIds);

    		var destination = client.updateItem(request -> request.tableName(entityTable).key(key).conditionExpression("attribute_exists(links)").updateExpression("SET links.#table = :val" ).expressionAttributeNames(k).expressionAttributeValues(values))
    				.handle((r, e) -> {
    					if(e != null) {
    						if(e.getCause() instanceof ConditionalCheckFailedException) {
    							Map<String, AttributeValue> m = new HashMap<>();
    							m.put(target, values.get(":val"));
    							values.put(":val", AttributeValue.builder().m(m).build());
    							return client.updateItem(request -> request.tableName(entityTable).key(key).updateExpression("SET links = :val").expressionAttributeValues(values));
    						}else {
    							throw new RuntimeException(e);
    						}
    					}
    					return CompletableFuture.completedFuture(null);
    				}).thenCompose(a -> a);
			future = future.thenCombine(destination, (a,b) -> b);
		return future.thenApply(response -> {
			return entity;
		});
	}
	

	public <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity) {
		var organisationIdAttribute = AttributeValue.builder().s(organisationId).build();
		String source = table(entity.getClass());

		CompletableFuture<?> future = CompletableFuture.completedFuture(null);
		
		
		
		for(var link: entity.getLinks().entries()) {
			var targetIdAttribute = AttributeValue.builder().s(link.getKey() + ":" + link.getValue()).build();
			Map<String, AttributeValue> targetKey = new HashMap<>();
			targetKey.put("organisationId", organisationIdAttribute);
			targetKey.put("id", targetIdAttribute);
			
			Map<String, AttributeValue> v = new HashMap<>();
			v.put(":val", AttributeValue.builder().ss(entity.getId()).build());
			
			Map<String, String> k = new HashMap<>();
			k.put("#table", source);
			
			var destination = client.updateItem(request -> request.tableName(entityTable).key(targetKey).updateExpression("DELETE links.#table :val").expressionAttributeNames(k).expressionAttributeValues(v));
			future = future.thenCombine(destination, (a,b) -> b);
		}
		entity.getLinks().clear();
		return future.thenApply(__ -> entity);
	}
	
	@VisibleForTesting
	public static String table(Class<? extends Table> type) {
		Class<?> tmp = type;
		TableName name = null;
		while(name == null && tmp != null) {
			name = tmp.getDeclaredAnnotation(TableName.class);
			tmp = tmp.getSuperclass();
		}
		if(name == null) {
			return type.getSimpleName().toLowerCase() + "s";
		} else {
			return name.value();
		}
	}

	@Override
	public String newId() {
		return idGenerator.get();
	}
}
