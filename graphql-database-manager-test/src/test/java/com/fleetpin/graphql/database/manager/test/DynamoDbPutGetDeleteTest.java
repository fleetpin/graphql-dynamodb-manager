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

import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.dynamo.DynamoDbManager;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseOrganisation;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

final class DynamoDbPutGetDeleteTest {

	@TestDatabase
	void testSimplePutGetDelete(final Database db) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		db.delete(entry1, false).get();
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertNull(entry1);
	}

	@TestDatabase
	void testGlobalPutGetDelete(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = db.putGlobal(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());

		entry1 = dbProd.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());

		
		db.delete(entry1, false).get();
		
		//will not actually delete as is in global space
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
	}
	
	@TestDatabase
	void testClimbingSimplePutGetDelete(final @DatabaseNames({"prod", "stage"}) Database db, @DatabaseNames("prod") final Database dbProd) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = dbProd.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		db.delete(entry1, false).get();
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertNull(entry1);
		
		entry1 = dbProd.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		var entry2 = new SimpleTable("two");
		entry2.setId(entry1.getId());
		db.put(entry2).get();
		
		entry1 = dbProd.get(SimpleTable.class, id).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		

		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertEquals("two", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
	}
	
	@TestDatabase
	void testClimbingGlobalPutGetDelete(@DatabaseNames({"prod", "stage"}) final Database db, @DatabaseNames("prod") final Database dbProd) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = dbProd.putGlobal(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		//is global so should do nothing
		db.delete(entry1, false).get();
		entry1 = db.get(SimpleTable.class, id).get();
		
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		entry1 = dbProd.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		var entry2 = new SimpleTable("two");
		entry2.setId(entry1.getId());
		db.put(entry2).get();
		
		entry1 = dbProd.get(SimpleTable.class, id).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		

		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertEquals("two", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		
	}

	@TestDatabase
	void testTwoOrganisationsPutGetDelete(final Database db, @DatabaseOrganisation("org-777") final Database db2) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertNull(db2.get(SimpleTable.class, id).get());

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		db.delete(entry1, false).get();
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertNull(entry1);
	}

	@TestDatabase
	void testTwoManagedDatabasesOnSameOrganisationPutGetDelete(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final var db = dynamoDbManager.getDatabase("test");
		final var db2 = dynamoDbManager.getDatabase("test");

		db.start(new CompletableFuture<>());
		db2.start(new CompletableFuture<>());

		final var joPutEntry = db.put(new SimpleTable("jo")).get();
		Assertions.assertEquals("jo", joPutEntry.getName());
		Assertions.assertNotNull(joPutEntry.getId());

		final var joGetEntry = db.get(SimpleTable.class, joPutEntry.getId()).get();
		Assertions.assertNotNull(joGetEntry.getId());

		final var joWasFoundEntry = db2.get(SimpleTable.class, joPutEntry.getId()).get();
		Assertions.assertNotNull(joWasFoundEntry.getId());

		db.delete(joGetEntry, false).get();

		final var joWasDeleted = db.get(SimpleTable.class, joGetEntry.getId()).get();
		Assertions.assertNull(joWasDeleted);
	}

	@TestDatabase
	void testTwoManagedDatabasesPutGetDelete(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final var db = dynamoDbManager.getDatabase("test");
		final var db2 = dynamoDbManager.getDatabase("test2");

		db.start(new CompletableFuture<>());
		db2.start(new CompletableFuture<>());

		final var janePutEntry = db.put(new SimpleTable("jane")).get();
		Assertions.assertEquals("jane", janePutEntry.getName());
		Assertions.assertNotNull(janePutEntry.getId());

		final var janeGetEntry = db.get(SimpleTable.class, janePutEntry.getId()).get();
		Assertions.assertNotNull(janeGetEntry.getId());

		final var janeNotExists = db2.get(SimpleTable.class, janePutEntry.getId()).get();
		Assertions.assertNull(janeNotExists);

		db.delete(janeGetEntry, false).get();

		final var janeWasDeleted = db.get(SimpleTable.class, janeGetEntry.getId()).get();
		Assertions.assertNull(janeWasDeleted);
	}
	
	@TestDatabase
	void testSameIdDifferentTypes(final Database db) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		SimpleTable2 entry2 = new SimpleTable2("bob");
		entry1.setId("iamthesame");
		entry2.setId("iamthesame");
		entry1 = db.put(entry1).get();
		entry2 = db.put(entry2).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();		
		
		var entry1Future = db.get(SimpleTable.class, id);
		var entry2Future = db.get(SimpleTable2.class, id);
		
		Assertions.assertEquals("garry", entry1Future.get().getName());
		Assertions.assertEquals("bob", entry2Future.get().getName());
	}
	
	

	static class SimpleTable extends Table {
		private String name;

		public SimpleTable() {
		}

		public SimpleTable(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
	
	static class SimpleTable2 extends Table {
		private String name;

		public SimpleTable2() {
		}

		public SimpleTable2(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
