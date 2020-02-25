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
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

final class DynamoDbQueryTest {
	@TestDatabase
	void testSimpleQuery(final Database db) throws InterruptedException, ExecutionException {
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

	@TestDatabase
	void testTwoTablesQuery(final Database db) throws InterruptedException, ExecutionException {
		db.put(new SimpleTable("garry")).get();
		db.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		db.put(new AnotherTable("ed")).get();
		db.put(new AnotherTable("eddie")).get();

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

	@TestDatabase
	void testQueryDeleteQuery(final Database db) throws InterruptedException, ExecutionException {
		db.put(new SimpleTable("garry")).get();
		db.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(3, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("bob", entries.get(0).name);
		Assertions.assertEquals("frank", entries.get(1).name);
		Assertions.assertEquals("garry", entries.get(2).name);

		db.delete(entries.get(1), false).get();

		entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(2, entries.size());

		entries.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("bob", entries.get(0).name);
		Assertions.assertEquals("garry", entries.get(1).name);

	}


	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testClimbingQuery(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
		var garry = dbProd.put(new SimpleTable("garry")).get();
		var garryLocal = new SimpleTable("GARRY");
		garryLocal.setId(garry.getId());
		db.put(garryLocal);
		dbProd.put(new SimpleTable("bob")).get();
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

	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testClimbingGlobalQuery(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
		var garry = dbProd.put(new SimpleTable("garry")).get();
		var garryLocal = new SimpleTable("GARRY");
		garryLocal.setId(garry.getId());
		db.put(garryLocal);
		dbProd.put(new SimpleTable("bob")).get();
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
		
		@Override
		public String toString() {
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
