package com.fleetpin.graphql.dynamodb.manager;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface PutPermission extends Function<Table, CompletableFuture<Boolean>>{

}
