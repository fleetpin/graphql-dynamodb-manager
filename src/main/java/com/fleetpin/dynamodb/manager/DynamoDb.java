package com.fleetpin.dynamodb.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.dataloader.DataLoader;

public interface DynamoDb {
	public <T extends Table> CompletableFuture<T> delete(String organisationId, T entity);
	public <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity);

	public <T extends Table> CompletableFuture<T> put(String organisationId, T entity);

	public CompletableFuture<List<DynamoItem>> get(List<DatabaseKey> keys);
	
	
	public CompletableFuture<List<DynamoItem>> getViaLinks(String organisationId, Table entry, Class<? extends Table> type, DataLoader<DatabaseKey, DynamoItem> items);
	public CompletableFuture<List<DynamoItem>> query(DatabaseQueryKey key);
	
	public CompletableFuture<List<DynamoItem>> queryGlobal(Class<? extends Table> type, String value);
	public CompletableFuture<List<DynamoItem>> querySecondary(Class<? extends Table> type, String organisationId, String value);
	public <T extends Table> CompletableFuture<T> link(String organisationId, T entry, Class<? extends Table> class1, List<String> groupIds);
	public int maxBatchSize();
	public String newId();

}
