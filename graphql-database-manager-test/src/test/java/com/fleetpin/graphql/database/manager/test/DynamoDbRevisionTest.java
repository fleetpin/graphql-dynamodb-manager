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

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;

import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.RevisionMismatchException;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;

public class DynamoDbRevisionTest {
	
	@TestDatabase
	public void testCreateNewObject(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		db.checkPut(entry).get();
		var entries = db.query(SimpleTable.class).get();
		Assertions.assertEquals(1, entries.size());
		Assertions.assertEquals("garry", entries.get(0).name);
		Assertions.assertEquals(1, entries.get(0).getRevision());
		
		var e = new SimpleTable("1", "garry");
		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(e).get());
		
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
	}
	
	@TestDatabase
	public void testRevisionMustStartAt0(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		entry.setRevision(1);
		//does not exist yet so fails
		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(entry).get());
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
	}

	@TestDatabase
	public void testIncrementsAfterExists(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		
		entry = db.checkPut(entry).get();
		Assertions.assertEquals(1, entry.getRevision());
		entry = db.checkPut(entry).get();
		Assertions.assertEquals(2, entry.getRevision());
		
		entry = db.get(SimpleTable.class, entry.getId()).get();
		Assertions.assertEquals(2, entry.getRevision());
		
		entry = db.checkPut(entry).get();
		Assertions.assertEquals(3, entry.getRevision());
		
		//check any out of order revisions are reflected
		{
			entry.setRevision(5);
			var e = entry;
			var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(e).get());
			Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
		}
		
		{
			entry.setRevision(1);
			var e = entry;
			var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(e).get());
			Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
		}
		
		{
			entry.setRevision(0);
			var e = entry;
			var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(e).get());
			Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
		}
	}
	
	@TestDatabase
	public void testLink(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		var other1 = new AnotherTable("1", "pants");
		var other2 = new AnotherTable("2", "socks");	
		
		entry = db.checkPut(entry).get();
		other1 = db.checkPut(other1).get();
		other2 = db.checkPut(other2).get();
		
		
		Assertions.assertEquals(1, entry.getRevision());
		Assertions.assertEquals(1, other1.getRevision());
		Assertions.assertEquals(1, other2.getRevision());
		
		entry = db.links(entry, AnotherTable.class, Arrays.asList("1", "2")).get();
		
		other1 = db.get(AnotherTable.class, "1").get();
		other2 = db.get(AnotherTable.class, "2").get();
		
		Assertions.assertEquals(2, entry.getRevision());
		Assertions.assertEquals(2, other1.getRevision());
		Assertions.assertEquals(2, other2.getRevision());
		
		entry = db.links(entry, AnotherTable.class, Arrays.asList("1")).get();
		
		
		other1 = db.get(AnotherTable.class, "1").get();
		other2 = db.get(AnotherTable.class, "2").get();
		
		Assertions.assertNull(db.getLink(other2, AnotherTable.class).get());
		
		Assertions.assertEquals(3, entry.getRevision());
		Assertions.assertEquals(2, other1.getRevision()); // no change
		Assertions.assertEquals(3, other2.getRevision()); // delete will also bump revision
		
		
		entry = db.links(entry, AnotherTable.class, Arrays.asList("2")).get();
		
		
		other1 = db.get(AnotherTable.class, "1").get();
		other2 = db.get(AnotherTable.class, "2").get();
		
		Assertions.assertEquals(4, entry.getRevision());
		Assertions.assertEquals(3, other1.getRevision());
		Assertions.assertEquals(4, other2.getRevision());
		
	}
	
	
	@TestDatabase
	public void testMultipleEnv(final @DatabaseNames({"prod", "stage"}) Database db, @DatabaseNames("prod") final Database dbProd) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");

		entry = dbProd.checkPut(entry).get();
		
		Assertions.assertEquals(1, entry.getRevision());
		
		entry = db.get(SimpleTable.class, "1").get();
		//ignores revision when its going from one db to another if not already there
		entry = db.put(entry).get();
		
		Assertions.assertEquals(2, entry.getRevision());
		
		//can no remake once set in db though
		var e = new SimpleTable("1", "garry");
		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(e).get());
		
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
		
	}

	@TestDatabase
	public void testMultipleEnvConfirmRevisionIgnored(final @DatabaseNames({"prod", "stage"}) Database db, @DatabaseNames("prod") final Database dbProd) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");

		entry = dbProd.checkPut(entry).get();
		
		Assertions.assertEquals(1, entry.getRevision());
		
		entry = db.get(SimpleTable.class, "1").get();
		//ignores revision when its going from one db to another if not already there
		entry.setRevision(100);
		entry = db.put(entry).get();
		
		Assertions.assertEquals(101, entry.getRevision());
		
		//can no remake once set in db though
		var e = new SimpleTable("1", "garry");
		e.setRevision(40);
		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(e).get());
		
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());
		
	}
	
	@TestDatabase
	public void testMultipleEnvCreatedInDbBeforePut(final @DatabaseNames({"prod", "stage"}) Database db, @DatabaseNames("prod") final Database dbProd) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");

		entry = dbProd.checkPut(entry).get();
		entry = dbProd.checkPut(entry).get();
		entry = dbProd.checkPut(entry).get();
		
		Assertions.assertEquals(3, entry.getRevision());
		
		entry = db.get(SimpleTable.class, "1").get();
		
		Assertions.assertEquals(3, entry.getRevision());
		
		var newEntry = new SimpleTable("1", "garry");

		//this part would happen on different maching or something
		newEntry = db.checkPut(newEntry).get();
		Assertions.assertEquals(1, newEntry.getRevision());
		newEntry = db.get(SimpleTable.class, "1").get();
		Assertions.assertEquals(1, newEntry.getRevision());
		

		var fixed = entry;
		
		Assertions.assertThrows(ExecutionException.class, () -> db.checkPut(fixed).get());

	}
	

	static class SimpleTable extends Table {
		private String name;

		public SimpleTable() {
		}

		public SimpleTable(String id, String name) {
			setId(id);
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

		public AnotherTable(String id, String name) {
			setId(id);
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

}
