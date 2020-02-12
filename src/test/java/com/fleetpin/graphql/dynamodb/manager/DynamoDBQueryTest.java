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

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class DynamoDBQueryTest extends DynamoDBBase {
	@ParameterizedTest
	@EnumSource(DatabaseType.class)
	public void testSimpleQuery(final DatabaseType dbType) throws InterruptedException, ExecutionException {
		final var db = createTestDatabase(dbType, "test");

		db.put(new SimpleTable("garry")).get();
		db.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(3, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("bob", entries.get(0).name);
		Assertions.assertEquals("frank", entries.get(1).name);
		Assertions.assertEquals("garry", entries.get(2).name);
	}

	@ParameterizedTest
	@EnumSource(DatabaseType.class)
	public void testTwoTablesQuery(final DatabaseType dbType) throws InterruptedException, ExecutionException {
		final var db = createTestDatabase(dbType, "test");

		db.put(new SimpleTable("garry")).get();
		db.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		db.put(new AnotherTable("ed"));
		db.put(new AnotherTable("eddie"));

		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(3, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("bob", entries.get(0).name);
		Assertions.assertEquals("frank", entries.get(1).name);
		Assertions.assertEquals("garry", entries.get(2).name);

		var entriesOther = db.query(AnotherTable.class).get();
		Assertions.assertEquals(2, entriesOther.size());

		entriesOther.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("ed", entriesOther.get(0).name);
		Assertions.assertEquals("eddie", entriesOther.get(1).name);

	}



	@ParameterizedTest
	@EnumSource(DatabaseType.class)
	public void testQueryDeleteQuery(final DatabaseType dbType) throws InterruptedException, ExecutionException {
		final var db = createTestDatabase(dbType, "test");

		db.put(new SimpleTable("garry")).get();
		db.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(3, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("bob", entries.get(0).name);
		Assertions.assertEquals("frank", entries.get(1).name);
		Assertions.assertEquals("garry", entries.get(2).name);

		db.delete(entries.get(1), false);

		entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(2, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("bob", entries.get(0).name);
		Assertions.assertEquals("garry", entries.get(1).name);

	}


	@Test
	public void testClimbingQuery() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var prod = getDatabaseProduction("test");


		var garry = prod.put(new SimpleTable("garry")).get();
		var garryLocal = new SimpleTable("GARRY");
		garryLocal.setId(garry.getId());
		db.put(garryLocal);
		prod.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(3, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("GARRY", entries.get(0).name);
		Assertions.assertEquals("bob", entries.get(1).name);
		Assertions.assertEquals("frank", entries.get(2).name);

		db.delete(entries.get(1), false);

		entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(2, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("GARRY", entries.get(0).name);
		Assertions.assertEquals("frank", entries.get(1).name);

	}

	@Test
	public void testClimbingGlobalQuery() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var prod = getDatabaseProduction("test");


		var garry = prod.put(new SimpleTable("garry")).get();
		var garryLocal = new SimpleTable("GARRY");
		garryLocal.setId(garry.getId());
		db.put(garryLocal);
		prod.put(new SimpleTable("bob")).get();
		db.putGlobal(new SimpleTable("frank")).get();

		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(3, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("GARRY", entries.get(0).name);
		Assertions.assertEquals("bob", entries.get(1).name);
		Assertions.assertEquals("frank", entries.get(2).name);

		db.delete(entries.get(1), false);

		entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(2, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("GARRY", entries.get(0).name);
		Assertions.assertEquals("frank", entries.get(1).name);
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

	static class AnotherTable extends Table {
		private String name;

		public AnotherTable() {
		}

		public AnotherTable(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

}
