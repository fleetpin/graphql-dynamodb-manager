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

package com.fleetpin.graphql.dynamodb.manager;

import org.dataloader.DataLoader;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DynamoDb {
    <T extends Table> CompletableFuture<T> delete(String organisationId, T entity);

    <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity);

    <T extends Table> CompletableFuture<T> put(String organisationId, T entity);

    CompletableFuture<List<DynamoItem>> get(List<DatabaseKey> keys);

    CompletableFuture<List<DynamoItem>> getViaLinks(String organisationId, Table entry, Class<? extends Table> type, DataLoader<DatabaseKey, DynamoItem> items);

    CompletableFuture<List<DynamoItem>> query(DatabaseQueryKey key);

    CompletableFuture<List<DynamoItem>> queryGlobal(Class<? extends Table> type, String value);

    CompletableFuture<List<DynamoItem>> querySecondary(Class<? extends Table> type, String organisationId, String value);

    <T extends Table> CompletableFuture<T> link(String organisationId, T entry, Class<? extends Table> class1, List<String> groupIds);

    int maxBatchSize();

    String newId();
}
