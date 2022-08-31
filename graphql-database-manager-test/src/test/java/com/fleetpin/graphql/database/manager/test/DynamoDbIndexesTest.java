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

package com.fleetpin.graphql.database.manager.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.GlobalIndex;
import com.fleetpin.graphql.database.manager.annotations.SecondaryIndex;
import com.fleetpin.graphql.database.manager.dynamo.DynamoDbManager;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseOrganisation;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import com.fleetpin.graphql.database.manager.util.BackupItem;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;

final class DynamoDbIndexesTest {

	ObjectMapper mapper = new ObjectMapper();

	@TestDatabase
	void testGlobal(final Database db) throws InterruptedException, ExecutionException {
		var list = db.queryGlobal(SimpleTable.class, "john").get();
		Assertions.assertEquals(0, list.size());

		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());

		list = db.queryGlobal(SimpleTable.class, "john").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("garry", list.get(0).getName());
		Assertions.assertEquals("garry", db.queryGlobalUnique(SimpleTable.class, "john").get().getName());
	}

	@TestDatabase
	void testGlobalInheritance(@DatabaseNames({ "prod", "stage" }) final Database db, @DatabaseNames({ "prod" }) final Database dbProd)
		throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = dbProd.put(entry1).get();

		SimpleTable entry2 = new SimpleTable("barry", "john");
		entry2.setId(entry1.getId());
		db.put(entry2).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());

		var list = db.queryGlobal(SimpleTable.class, "john").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("barry", list.get(0).getName());
		Assertions.assertEquals("barry", db.queryGlobalUnique(SimpleTable.class, "john").get().getName());
	}

	@TestDatabase
	void testSecondary(final Database db) throws InterruptedException, ExecutionException {
		var list = db.querySecondary(SimpleTable.class, "garry").get();
		Assertions.assertEquals(0, list.size());

		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());

		list = db.querySecondary(SimpleTable.class, "garry").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("garry", list.get(0).getName());
		Assertions.assertEquals("garry", db.querySecondaryUnique(SimpleTable.class, "garry").get().getName());
	}

	@TestDatabase
	void testSecondaryInheritance(@DatabaseNames({ "prod", "stage" }) final Database db, @DatabaseNames({ "prod" }) final Database dbProd)
		throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = dbProd.put(entry1).get();

		SimpleTable entry2 = new SimpleTable("garry", "barry");
		entry2.setId(entry1.getId());
		db.put(entry2).get();

		var list = db.querySecondary(SimpleTable.class, "garry").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("barry", list.get(0).getGlobalLookup());
		Assertions.assertEquals("barry", db.querySecondaryUnique(SimpleTable.class, "garry").get().getGlobalLookup());
	}

	@TestDatabase
	void testSecondaryUnique(final Database db) throws InterruptedException, ExecutionException {
		var entry = db.querySecondaryUnique(SimpleTable.class, "garry").get();
		Assertions.assertNull(entry);

		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();

		entry = db.querySecondaryUnique(SimpleTable.class, "garry").get();
		Assertions.assertEquals("garry", entry.getName());

		SimpleTable entry2 = new SimpleTable("garry", "john");
		db.put(entry2).get();

		ExecutionException t = Assertions.assertThrows(ExecutionException.class, () -> db.querySecondaryUnique(SimpleTable.class, "garry").get());
		Assertions.assertTrue(t.getCause().getMessage().contains("expected single linkage"));
	}

	@TestDatabase
	void testGlobalUnique(final Database db) throws InterruptedException, ExecutionException {
		var entry = db.queryGlobalUnique(SimpleTable.class, "john").get();
		Assertions.assertNull(entry);

		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();

		entry = db.queryGlobalUnique(SimpleTable.class, "john").get();
		Assertions.assertEquals("garry", entry.getName());

		SimpleTable entry2 = new SimpleTable("garry", "john");
		db.put(entry2).get();

		ExecutionException t = Assertions.assertThrows(ExecutionException.class, () -> db.queryGlobalUnique(SimpleTable.class, "john").get());
		Assertions.assertTrue(t.getCause().getMessage().contains("expected single linkage"));
	}

	@TestDatabase
	void testMultiOrganisationSecondaryIndexWithDynamoDbManager(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final var db0 = dynamoDbManager.getDatabase("organisation-0");
		final var db1 = dynamoDbManager.getDatabase("organisation-1");
		db0.start(new CompletableFuture<>());
		db1.start(new CompletableFuture<>());

		final var putAvocado = db0.put(new SimpleTable("avocado", "fruit")).get();

		final var exists = db0.get(SimpleTable.class, putAvocado.getId()).get();
		Assertions.assertNotNull(exists);
		Assertions.assertEquals(putAvocado, exists);

		final var nonExistent = db1.get(SimpleTable.class, "avocado").get();
		Assertions.assertNull(nonExistent);
	}

	@TestDatabase
	void testMultiOrganisationSecondaryIndexWithAnnotations(@DatabaseOrganisation("newdude") final Database db0, final Database db1)
		throws ExecutionException, InterruptedException {
		final var putJohn = db0.put(new SimpleTable("john", "nhoj")).get();

		final var exists = db0.get(SimpleTable.class, putJohn.getId()).get();
		Assertions.assertNotNull(exists);
		Assertions.assertEquals(putJohn, exists);

		final var nonExistent = db1.get(SimpleTable.class, putJohn.getId()).get();
		Assertions.assertNull(nonExistent);
	}

	private void checkResponseNameField(List<BackupItem> queryResult, Integer rank, List<String> names) {
		var jsonMap = queryResult.get(rank).getItem();
		ObjectMapper om = new ObjectMapper();
		var itemMap = om.convertValue(jsonMap, new TypeReference<Map<String, Object>>() {});
		Assertions.assertTrue(names.contains(((HashMap<String, Object>) itemMap.get("item")).get("name")));
	}

	public static class Drink extends Table {

		private String name;
		private Boolean alcoholic;

		public Drink() {}

		public Drink(String name, Boolean alcoholic) {
			this.name = name;
			this.alcoholic = alcoholic;
		}

		public String getName() {
			return name;
		}

		public Boolean getAlcoholic() {
			return alcoholic;
		}
	}

	public static class SimpleTable extends Table {

		private String name;
		private String globalLookup;

		public SimpleTable() {}

		public SimpleTable(String name, String globalLookup) {
			this.name = name;
			this.globalLookup = globalLookup;
		}

		@SecondaryIndex
		public String getName() {
			return name;
		}

		@GlobalIndex
		public String getGlobalLookup() {
			return globalLookup;
		}
	}
}
