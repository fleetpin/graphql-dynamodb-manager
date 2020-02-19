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

import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.GlobalIndex;
import com.fleetpin.graphql.database.manager.annotations.SecondaryIndex;
import com.fleetpin.graphql.database.manager.dynamo.Database;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.ExecutionException;

final class DynamoDbIndexesTest {

	@TestDatabase
	void testGlobal(final Database db) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());

		var list = db.queryGlobal(SimpleTable.class, "john").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("garry", list.get(0).getName());
		Assertions.assertEquals("garry", db.queryGlobalUnique(SimpleTable.class, "john").get().getName());
	}

	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testGlobalInheritance(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = dbProd.put(entry1).get();

		SimpleTable entry2 = new SimpleTable("barry", "john");
		entry2.setId(entry1.getId());
		db.put(entry2);


		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());

		var list = db.queryGlobal(SimpleTable.class, "john").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("barry", list.get(0).getName());
		Assertions.assertEquals("barry", db.queryGlobalUnique(SimpleTable.class, "john").get().getName());
	}


	@TestDatabase
	void testSecondary(final Database db) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());

		var list = db.querySecondary(SimpleTable.class, "garry").get();
		Assertions.assertEquals(1, list.size());

		Assertions.assertEquals("garry", list.get(0).getName());
		Assertions.assertEquals("garry", db.querySecondaryUnique(SimpleTable.class, "garry").get().getName());
	}

	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testSecondaryInheritance(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
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

	public static class SimpleTable extends Table {
		private String name;
		private String globalLookup;

		public SimpleTable() {
		}

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
