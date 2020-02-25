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

	@TestDatabase(organisationIds = {"test", "test"})
	void testGlobalPutGetDelete(final Database db, final Database db2) throws InterruptedException, ExecutionException {
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = db.putGlobal(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());

		entry1 = db2.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());

		
		db.delete(entry1, false).get();
		
		//will not actually delete as is in global space
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
	}
	
	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testClimbingSimplePutGetDelete(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
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
	
	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testClimbingGlobalPutGetDelete(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
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

	@TestDatabase(organisationIds = {"test", "test2"})
	void testTwoOrganisationsPutGetDelete(final Database db, final Database db2) throws InterruptedException, ExecutionException {
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
}
