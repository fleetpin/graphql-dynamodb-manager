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

package com.fleetpin.graphql.database.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

import com.fleetpin.graphql.database.manager.access.ForbiddenWriteException;
import com.fleetpin.graphql.database.manager.access.ModificationPermission;
import com.fleetpin.graphql.database.manager.util.TableCoreUtil;

@SuppressWarnings("unchecked")
public class Database {
	private String organisationId;
	private final DatabaseDriver driver;

	private final TableDataLoader<DatabaseKey<Table>> items;
	private final TableDataLoader<DatabaseQueryKey<Table>> queries;

	private final QueryBuilderFactory queryBuilderFactory;
	private final Function<Table, CompletableFuture<Boolean>> putAllow;
	
	Database(String organisationId, DatabaseDriver driver, ModificationPermission putAllow, QueryBuilderFactory factory) {
		this.organisationId = organisationId;
		this.driver = driver;
		this.putAllow = putAllow;
		this.queryBuilderFactory = factory;

		items = new TableDataLoader<>(new DataLoader<DatabaseKey<Table>, Table>(keys -> {
			return driver.get(keys);
		}, DataLoaderOptions.newOptions().setMaxBatchSize(driver.maxBatchSize()))); // will auto call global
		
		queries = new TableDataLoader<>(new DataLoader<DatabaseQueryKey<Table>, List<Table>>(keys -> {
			return merge(keys.stream().map(key -> driver.query(key)));
		}, DataLoaderOptions.newOptions().setBatchingEnabled(false))); // will auto call global
	}

	public <T extends Table> CompletableFuture<List<T>> query(Class<T> type, Function<QueryBuilder<T>, QueryBuilder<T>> func) {
		return query(func.apply(queryBuilderFactory.getBuilder(type)));
	}

	public <T extends Table> CompletableFuture<List<T>> query(QueryBuilder<T> builder) {
		return builder.exec();
	}

	public <T extends Table> CompletableFuture<List<T>> query(Class<T> type) {
		DatabaseQueryKey<Table> key = (DatabaseQueryKey<Table>) KeyFactory.createDatabaseQueryKey(organisationId, type);
		CompletableFuture<List<T>> toReturn = queries.load(key);
		return toReturn
				.thenApply(items -> items.stream().filter(Objects::nonNull).collect(Collectors.toList()));
	}

	public <T extends Table> CompletableFuture<List<T>> queryGlobal(Class<T> type, String id) {
		return driver.queryGlobal(type, id)
				.thenApply(items -> items.stream().map(item -> (T) item).collect(Collectors.toList()));
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
		return driver.querySecondary(type, organisationId, id, items)
				.thenApply(items -> items.stream().map(item -> (T) item).collect(Collectors.toList()));
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
		DatabaseKey<Table> key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, type, id);
		return items.load(key).thenApply(item -> {
			if(item == null) {
				return Optional.empty();
			}else {
				return Optional.of((T) item);
			}
		});
	}
	
	public <T extends Table> CompletableFuture<T> get(Class<T> type, String id) {
		DatabaseKey<Table> key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, type, id);
		return items.load(key).thenApply(item -> {
		return (T) item;
		});
	}

	public <T extends Table> CompletableFuture<T> delete(T entity, boolean deleteLinks) {
		if(!deleteLinks) {
			if(!TableAccess.getTableLinks(entity).isEmpty()) {
				throw new RuntimeException("deleting would leave dangling links");
			}
		}
		return putAllow.apply(entity).thenCompose(allow -> {
			if(!allow) {
				throw new ForbiddenWriteException("Delete not allowed for " + TableCoreUtil.table(entity.getClass()) + " with id " + entity.getId());
			}
			DatabaseKey<Table> key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, entity.getClass(), entity.getId());
    		items.clear(key);
    		DatabaseQueryKey<Table> queryKey = (DatabaseQueryKey<Table>) KeyFactory.createDatabaseQueryKey(organisationId, entity.getClass());
    		queries.clear(queryKey);
    		
    		if(deleteLinks) {
    			return deleteLinks(entity).thenCompose(t -> driver.delete(organisationId, entity));
    		}
    		return driver.delete(organisationId, entity);
		});
	}

	public <T extends Table> CompletableFuture<List<T>> getLinks(final Table entry, Class<T> target) {
		return driver.getViaLinks(organisationId, entry, target, items)
			.thenApply(items -> items.stream().filter(Objects::nonNull).map(item -> (T) item).collect(Collectors.toList()));
	}

	public <T extends Table> CompletableFuture<T> getLink(final Table entry, Class<T> target) {
		return getLinks(entry, target).thenApply(items -> {
			if (items.size() > 1) {
				throw new RuntimeException("Requests a link that expects a single mapping but found multiple entities"); // TODO: more info in failure
			}
			return (T) items.stream().findFirst().orElse(null);
		});

	}
	
	public <T extends Table> CompletableFuture<Optional<T>> getLinkOptional(final Table entry, Class<T> target) {
		return getLink(entry, target).thenApply(t -> Optional.ofNullable(t));

	}

	public <T extends Table> CompletableFuture<T> deleteLinks(T entity) {
		return putAllow.apply(entity).thenCompose(allow -> {
			if(!allow) {
				throw new ForbiddenWriteException("Delete links not allowed for " + TableCoreUtil.table(entity.getClass()) + " with id " + entity.getId());
			}
			return driver.deleteLinks(organisationId, entity).thenCompose(t -> put(entity));
		});
	}

	public <T extends Table> CompletableFuture<T> put(T entity) {
		return putAllow.apply(entity).thenCompose(allow -> {
			if(!allow) {
				throw new ForbiddenWriteException("put not allowed for " + TableCoreUtil.table(entity.getClass()) + " with id " + entity.getId());
			}
			DatabaseKey<Table> key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, entity.getClass(), entity.getId());
    		items.clear(key);
    		DatabaseQueryKey<Table> queryKey = (DatabaseQueryKey<Table>) KeyFactory.createDatabaseQueryKey(organisationId, entity.getClass());
    		queries.clear(queryKey);
    		return driver.put(organisationId, entity);
		});
	}
	public <T extends Table> CompletableFuture<T> putGlobal(T entity) {
		return putAllow.apply(entity).thenCompose(allow -> {
			if(!allow) {
				throw new ForbiddenWriteException("put global not allowed for " + TableCoreUtil.table(entity.getClass()) + " with id " + entity.getId());
			}
			DatabaseKey<Table> key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, entity.getClass(), entity.getId());
    		items.clear(key);
    		DatabaseQueryKey<Table> queryKey = (DatabaseQueryKey<Table>) KeyFactory.createDatabaseQueryKey(organisationId, entity.getClass());
    		queries.clear(queryKey);
    		return driver.put("global", entity);
		});
		
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


	public <T extends Table> CompletableFuture<T> links(T entity, Class<? extends Table> class1, List<String> targetIds) {
		return putAllow.apply(entity).thenCompose(allow -> {
			if(!allow) {
				throw new ForbiddenWriteException("Link not allowed for " + TableCoreUtil.table(entity.getClass()) + " with id " + entity.getId());
			}
			
			DatabaseKey<Table> key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, entity.getClass(), entity.getId());
    		items.clear(key);
    		DatabaseQueryKey<Table> queryKey = (DatabaseQueryKey<Table>) KeyFactory.createDatabaseQueryKey(organisationId, entity.getClass());
    		queries.clear(queryKey);
    		
    		for(String id: targetIds) {
    			key = (DatabaseKey<Table>) KeyFactory.createDatabaseKey(organisationId, class1, id);
        		items.clear(key);
    		}
    		queries.clear((DatabaseQueryKey<Table>) KeyFactory.createDatabaseQueryKey(organisationId, class1));
    		return driver.link(organisationId, entity, class1, targetIds);
		});
	}


	public <T extends Table> CompletableFuture<T> link(T entity, Class<? extends Table> class1, String targetId) {
		if(targetId == null) {
			return links(entity, class1, Collections.emptyList());	
		}else {
			return links(entity, class1, Arrays.asList(targetId));
		}
	}
	

	public <T extends Table> CompletableFuture<List<T>> get(Class<T> class1, List<String> ids) {
		if(ids == null) {
			return CompletableFuture.completedFuture(Collections.emptyList());
		}
		return TableCoreUtil.all(ids.stream().map(id -> get(class1, id)).collect(Collectors.toList()));
	}


	public void setOrganisationId(String organisationId) {
		this.organisationId = organisationId;
	}


	public String getSourceOrganisationId(Table table) {
		return TableAccess.getTableSourceOrganisation(table);
	}

	public String newId() {
		return driver.newId();
	}


	public Set<String> getLinkIds(Table entity, Class<? extends Table> type) {
		return Collections.unmodifiableSet(TableAccess.getTableLinks(entity).get(TableCoreUtil.table(type)));
	}
	
}
