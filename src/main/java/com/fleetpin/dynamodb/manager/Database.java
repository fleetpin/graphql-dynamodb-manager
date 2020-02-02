package com.fleetpin.dynamodb.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Database {

	private String organisationId;
	private final DynamoDb dynamo;

	private final ObjectMapper mapper;
	
	private final DataLoader<DatabaseKey, DynamoItem> items;
	private final DataLoader<DatabaseQueryKey, List<DynamoItem>> queries;

	Database(ObjectMapper mapper, String organisationId, DynamoDb dynamo) {
		this.mapper = mapper;
		this.organisationId = organisationId;
		this.dynamo = dynamo;

		items = new DataLoader<DatabaseKey, DynamoItem>(keys -> {
			return dynamo.get(keys);
		}, DataLoaderOptions.newOptions().setMaxBatchSize(50)); // will auto call global
		
		queries = new DataLoader<DatabaseQueryKey, List<DynamoItem>>(keys -> {
			return merge(keys.stream().map(key -> dynamo.query(key)));
		}, DataLoaderOptions.newOptions().setBatchingEnabled(false)); // will auto call global
	}


	public <T extends Table> CompletableFuture<List<T>> query(Class<T> type) {
		return queries.load(new DatabaseQueryKey(organisationId, type))
				.thenApply(items -> items.stream().map(item -> item.convertTo(mapper, type)).filter(Objects::nonNull).collect(Collectors.toList()));
	}

	public <T extends Table> CompletableFuture<List<T>> queryGlobal(Class<T> type, String id) {
		return dynamo.queryGlobal(type, id)
				.thenApply(items -> items.stream().map(item -> item.convertTo(mapper, type)).collect(Collectors.toList()));
	}
	public <T extends Table> CompletableFuture<T> queryGlobalUnique(Class<T> type, String id) {
		return queryGlobal(type, id).thenApply(items -> {
			if(items.size() > 1) {
				throw new RuntimeException("expected single linkage");
			}
			if(items.size() == 0) {
				return null;
			}
			return items.get(0);
		});
	}

	public <T extends Table> CompletableFuture<List<T>> querySecondary(Class<T> type, String id) {
		return dynamo.querySecondary(type, organisationId, id)
				.thenApply(items -> items.stream().map(item -> item.convertTo(mapper, type)).collect(Collectors.toList()));
	}
	public <T extends Table> CompletableFuture<T> querySecondaryUnique(Class<T> type, String id) {
		return querySecondary(type, id).thenApply(items -> {
			if(items.size() > 1) {
				throw new RuntimeException("expected single linkage");
			}
			if(items.size() == 0) {
				return null;
			}
			return items.get(0);
		});
	}
	public <T extends Table> CompletableFuture<Optional<T>> getOptional(Class<T> type, String id) {
		if(id == null) {
			return CompletableFuture.completedFuture(Optional.empty());
		}
		return items.load(new DatabaseKey(organisationId, type, id)).thenApply(item -> {
			if(item == null) {
				return Optional.empty();
			}else {
				return Optional.of(item.convertTo(mapper, type));
			}
		});
	}
	
	public <T extends Table> CompletableFuture<T> get(Class<T> type, String id) {
		return items.load(new DatabaseKey(organisationId, type, id)).thenApply(item -> {
			if(item == null) {
				return null;
			}else {
				return item.convertTo(mapper, type);
			}
		});
	}

	public <T extends Table> CompletableFuture<T> delete(T entity, boolean deleteLinks) {
		items.clear(new DatabaseKey(organisationId, entity.getClass(), entity.getId()));
		queries.clear(new DatabaseQueryKey(organisationId, entity.getClass()));
		
		if(deleteLinks) {
			return deleteLinks(entity).thenCompose(t -> dynamo.delete(organisationId, entity));
		}else {
			if(!entity.getLinks().isEmpty()) {
				throw new RuntimeException("deleting would leave dangling links");
			}
		}
		
		return dynamo.delete(organisationId, entity);
	}

	public <T extends Table> CompletableFuture<List<T>> getLinks(Table entry, Class<T> type) {
		return dynamo.getViaLinks(organisationId, entry, type, items)
			.thenApply(items -> items.stream().map(item -> item.convertTo(mapper, type)).filter(Objects::nonNull).collect(Collectors.toList()));
	}

	public <T extends Table> CompletableFuture<T> getLink(Table entry, Class<T> type) {
		return getLinks(entry, type).thenApply(items -> {
			if (items.size() > 1) {
				throw new RuntimeException("Bad data"); // TODO: more info in failure
			}
			return items.stream().findFirst().orElse(null);
		});

	}
	
	public <T extends Table> CompletableFuture<Optional<T>> getLinkOptional(Table entry, Class<T> type) {
		return getLink(entry, type).thenApply(t -> Optional.ofNullable(t));

	}

	public <T extends Table> CompletableFuture<T> deleteLinks(T entity) {
		return dynamo.deleteLinks(organisationId, entity).thenCompose(t -> put(entity));
	}

	public <T extends Table> CompletableFuture<T> put(T entity) {
		items.clear(new DatabaseKey(organisationId, entity.getClass(), entity.getId()));
		queries.clear(new DatabaseQueryKey(organisationId, entity.getClass()));
		return dynamo.put(organisationId, entity);
	}
	public <T extends Table> CompletableFuture<T> putGlobal(T entity) {
		items.clear(new DatabaseKey("global", entity.getClass(), entity.getId()));
		queries.clear(new DatabaseQueryKey("global", entity.getClass()));
		return dynamo.put("global", entity);
		
	}

	private <T> CompletableFuture<List<T>> merge(Stream<CompletableFuture<T>> stream) {
		List<CompletableFuture<T>> list = stream.collect(Collectors.toList());
		
		return CompletableFuture.allOf(list.toArray(CompletableFuture[]::new)).thenApply(__ -> {
			List<T> toReturn = new ArrayList<>(list.size());
			for(var item: list) {
				try {
					toReturn.add(item.get());
				} catch (InterruptedException | ExecutionException e) {
					throw new RuntimeException(e);
				}
			}
			return toReturn; 
		});
		
	}

	private static final Executor DELAYER = CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS);
	@SuppressWarnings("rawtypes")
	public void start(CompletableFuture<?> toReturn) {
		if(toReturn.isDone()) {
			return;
		}
		
		if(items.dispatchDepth() > 0 || queries.dispatchDepth() > 0) {
			CompletableFuture[] all = new CompletableFuture[] {items.dispatch(), queries.dispatch()};
			CompletableFuture.allOf(all).whenComplete((response, error) -> {
				//go around again
				start(toReturn);
			});
		}else {
			CompletableFuture.supplyAsync(() -> null, DELAYER).acceptEither(toReturn, __ -> start(toReturn));
		}
	}


	public <T extends Table> CompletableFuture<T> links(T entity, Class<? extends Table> class1, List<String> groupIds) {
		items.clear(new DatabaseKey(organisationId, entity.getClass(), entity.getId()));
		queries.clear(new DatabaseQueryKey(organisationId, entity.getClass()));
		for(String id: groupIds) {
			items.clear(new DatabaseKey(organisationId, class1, id));
		}
		queries.clear(new DatabaseQueryKey(organisationId, class1));
		return dynamo.link(organisationId, entity, class1, groupIds);
	}


	public <T extends Table> CompletableFuture<T> link(T entity, Class<? extends Table> class1, String groupIds) {
		if(groupIds == null) {
			return links(entity, class1, Collections.emptyList());	
		}else {
			return links(entity, class1, Arrays.asList(groupIds));
		}
	}
	

	public <T extends Table> CompletableFuture<List<T>> get(Class<T> class1, List<String> ids) {
		if(ids == null) {
			return CompletableFuture.completedFuture(Collections.emptyList());
		}
		return TableUtil.all(ids.stream().map(id -> get(class1, id)).collect(Collectors.toList()));
	}


	public void setOrganisationId(String organisationId) {
		this.organisationId = organisationId;
	}


	public String getSourceOrganisationId(Table table) {
		return table.getSourceOrganistaionId();
	}

	public String newId() {
		return dynamo.newId();
	}
	
}
