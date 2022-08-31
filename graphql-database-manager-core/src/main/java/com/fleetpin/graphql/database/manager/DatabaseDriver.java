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

import com.fleetpin.graphql.database.manager.util.BackupItem;
import com.google.common.collect.HashMultimap;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class DatabaseDriver {

	public abstract <T extends Table> CompletableFuture<T> delete(String organisationId, T entity);

	public abstract <T extends Table> CompletableFuture<List<T>> delete(String organisationId, Class<T> clazz);

	public abstract <T extends Table> CompletableFuture<T> deleteLinks(String organisationId, T entity);

	public abstract CompletableFuture<Void> bulkPut(List<PutValue> values);

	public abstract <T extends Table> CompletableFuture<List<T>> get(List<DatabaseKey<T>> keys);

	public abstract <T extends Table> CompletableFuture<List<T>> getViaLinks(
		String organisationId,
		Table entry,
		Class<T> type,
		TableDataLoader<DatabaseKey<Table>> items
	);

	public abstract <T extends Table> CompletableFuture<List<T>> query(DatabaseQueryKey<T> key);

	public abstract CompletableFuture<Void> restoreBackup(List<BackupItem> entities);

	public abstract CompletableFuture<List<BackupItem>> takeBackup(String organisationId);

	public abstract <T extends Table> CompletableFuture<List<T>> queryHistory(DatabaseQueryHistoryKey<T> key);

	public abstract <T extends Table> CompletableFuture<List<T>> queryGlobal(Class<T> type, String value);

	public abstract <T extends Table> CompletableFuture<List<T>> querySecondary(
		Class<T> type,
		String organisationId,
		String value,
		TableDataLoader<DatabaseKey<Table>> items
	);

	public abstract <T extends Table> CompletableFuture<T> link(String organisationId, T entry, Class<? extends Table> class1, List<String> groupIds);

	public abstract <T extends Table> CompletableFuture<T> unlink(
		final String organisationId,
		final T entity,
		final Class<? extends Table> clazz,
		final String targetId
	);

	public abstract int maxBatchSize();

	public abstract String newId();

	public abstract CompletableFuture<Boolean> destroyOrganisation(final String organisationId);

	protected <T extends Table> String getSourceOrganisationId(final T entity) {
		return entity.getSourceOrganisationId();
	}

	protected <T extends Table> void setLinks(final T entity, final String type, final Collection<String> groupIds) {
		entity.setLinks(type, groupIds);
	}

	protected <T extends Table> HashMultimap<String, String> getLinks(final T entity) {
		return entity.getLinks();
	}

	protected <T extends Table> void setCreatedAt(final T entity, final Instant createdAt) {
		entity.setCreatedAt(createdAt);
	}

	protected <T extends Table> void setUpdatedAt(final T entity, final Instant updatedAt) {
		entity.setUpdatedAt(updatedAt);
	}

	protected <T extends Table> void setSource(
		final T entity,
		final String sourceTable,
		final HashMultimap<String, String> links,
		final String sourceOrganisationId
	) {
		entity.setSource(sourceTable, links, sourceOrganisationId);
	}

	protected <T extends Table> String getSourceTable(final T entity) {
		return entity.getSourceTable();
	}

	protected <T extends Table> DatabaseKey<T> createDatabaseKey(final String organisationId, final Class<T> type, final String id) {
		return new DatabaseKey<>(organisationId, type, id);
	}
}
