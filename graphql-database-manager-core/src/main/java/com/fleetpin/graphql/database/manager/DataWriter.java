package com.fleetpin.graphql.database.manager;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DataWriter {
    private final Function<List<PutValue>, CompletableFuture<Void>> bulkWriter;
    private List<PutValue> toPut = new ArrayList<>();

    public DataWriter(Function<List<PutValue>, CompletableFuture<Void>> bulkWriter) {
        this.bulkWriter = bulkWriter;
    }

    public synchronized int dispatchSize() {
        return toPut.size();
    }

    public synchronized CompletableFuture<Void> dispatch() {
        //clear and take all from thing
        var future = bulkWriter.apply(toPut);
        toPut = new ArrayList<>();
        return future;
    }

    public synchronized <T extends Table> CompletableFuture<T> put(String organisationId, T entity, boolean check) {
        var future = new CompletableFuture<T>();
        var putValue = new PutValue<T>(organisationId, entity, check, future);
        toPut.add(putValue);
        return future;
    }
}
