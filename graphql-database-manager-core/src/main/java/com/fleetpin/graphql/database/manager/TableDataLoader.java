package com.fleetpin.graphql.database.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.dataloader.DataLoader;

public class TableDataLoader<K> {

	private final DataLoader<K, ?> loader;
	
	TableDataLoader(DataLoader<K, ?> loader) {
		this.loader = loader;
	}
	
	public <T> CompletableFuture<T> load(K key) {
		return (CompletableFuture<T>) loader.load(key);
	}
	public <T> CompletableFuture<List<T>> loadMany(List<K> keys) {
		//annoying waste of memory/cpu to get around cast :(
		return loader.loadMany(keys).thenApply(r -> r.stream().map(t -> (T) t).collect(Collectors.toList()));
	}

	public void clear(K key) {
		loader.clear(key);
	}
	
	public void clearAll() {
		loader.clearAll();
	}

	public int dispatchDepth() {
		return loader.dispatchDepth();
	}

	public CompletableFuture dispatch() {
		return loader.dispatch();
	}

	
	
	
}
