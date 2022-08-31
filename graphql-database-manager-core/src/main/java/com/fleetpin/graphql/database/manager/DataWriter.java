package com.fleetpin.graphql.database.manager;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DataWriter {
	private final Function<List<PutValue>, CompletableFuture<Void>> bulkWriter;
	private final List<PutValue> toPut = new ArrayList<>();

	public DataWriter(Function<List<PutValue>, CompletableFuture<Void>> bulkWriter) {
		this.bulkWriter = bulkWriter;
	}

	public int dispatchSize() {
		synchronized (toPut) {
			return toPut.size();
		}
	}

	public CompletableFuture<Void> dispatch() {
		List<PutValue> toSend = null;
		synchronized (toPut) {
			if (!toPut.isEmpty()) {
				toSend = new ArrayList<>(toPut);
				toPut.clear();
			}
		}
		if (toSend == null) {
			return CompletableFuture.completedFuture(null);
		} else {
			return bulkWriter.apply(toSend);
		}
	}

	public <T extends Table> CompletableFuture<T> put(String organisationId, T entity, boolean check) {
		var future = new CompletableFuture<T>();
		var putValue = new PutValue<T>(organisationId, entity, check, future);
		synchronized (toPut) {
			toPut.add(putValue);
		}
		return future;
	}
}
