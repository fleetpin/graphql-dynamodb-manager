package com.fleetpin.graphql.database.manager.util;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompletableFutureUtil {
    public static <T> CompletableFuture<List<T>> sequence(Stream<CompletableFuture<T>> s) {
        return s.collect(CompletableFutureCollector.allOf());
    }

    public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> com) {
        return CompletableFuture.allOf(com.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> {
                return    com.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList());
                }
                );
    }
}
