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
import com.fleetpin.graphql.database.manager.RevisionMismatchException;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;

/**
 * most of the test cases are cover in the RevisionPut tests this just covers links failure behavior
 * @author ashley
 *
 */
public class DynamoDbRevisionLinkTest {

	@TestDatabase
	public void testDelete(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		var other1 = new AnotherTable("1", "pants");
		var other2 = new AnotherTable("2", "socks");

		entry = db.put(entry).get();
		other1 = db.put(other1).get();
		other2 = db.put(other2).get();

		Assertions.assertEquals(1, entry.getRevision());
		Assertions.assertEquals(1, other1.getRevision());
		Assertions.assertEquals(1, other2.getRevision());

		entry = db.links(entry, AnotherTable.class, Arrays.asList("1", "2")).get();

		var snapshot = db.get(SimpleTable.class, entry.getId()).get();

		Assertions.assertEquals(2, entry.getRevision());

		entry = db.put(entry).get();

		Assertions.assertEquals(2, snapshot.getRevision());
		Assertions.assertEquals(3, entry.getRevision());

		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.links(snapshot, AnotherTable.class, Arrays.asList("2")).get());
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());

		//confirm no state change
		Assertions.assertEquals(2, snapshot.getRevision());
		Assertions.assertEquals(3, entry.getRevision());
	}

	@TestDatabase
	public void testLink(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		var other1 = new AnotherTable("1", "pants");
		var other2 = new AnotherTable("2", "socks");

		entry = db.put(entry).get();
		other1 = db.put(other1).get();
		other2 = db.put(other2).get();

		Assertions.assertEquals(1, entry.getRevision());
		Assertions.assertEquals(1, other1.getRevision());
		Assertions.assertEquals(1, other2.getRevision());

		entry = db.links(entry, AnotherTable.class, Arrays.asList("1", "2")).get();

		//link changed these objects
		var o1 = other1;
		var o2 = other2;
		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.put(o1).get());
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());

		cause = Assertions.assertThrows(ExecutionException.class, () -> db.put(o2).get());
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());

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
	public void testLinkDeleteObject(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		var other1 = new AnotherTable("1", "pants");
		var other2 = new AnotherTable("2", "socks");

		entry = db.put(entry).get();
		other1 = db.put(other1).get();
		other2 = db.put(other2).get();

		Assertions.assertEquals(1, entry.getRevision());
		Assertions.assertEquals(1, other1.getRevision());
		Assertions.assertEquals(1, other2.getRevision());

		entry = db.links(entry, AnotherTable.class, Arrays.asList("1", "2")).get();

		other1 = db.get(AnotherTable.class, "1").get();
		other2 = db.get(AnotherTable.class, "2").get();

		Assertions.assertEquals(2, entry.getRevision());
		Assertions.assertEquals(2, other1.getRevision());
		Assertions.assertEquals(2, other2.getRevision());

		db.delete(other1, true).get();

		entry = db.get(SimpleTable.class, "1").get();
		other1 = db.get(AnotherTable.class, "1").get();
		other2 = db.get(AnotherTable.class, "2").get();

		Assertions.assertEquals(3, entry.getRevision());
		Assertions.assertEquals(2, other2.getRevision()); // nochange
		Assertions.assertNull(other1);
	}

	@TestDatabase
	public void testMultipleEnv(final @DatabaseNames({ "prod", "stage" }) Database db, @DatabaseNames("prod") final Database dbProd)
		throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");
		var other1 = new AnotherTable("1", "pants");
		var other2 = new AnotherTable("2", "socks");

		entry = dbProd.put(entry).get();
		Assertions.assertEquals(1, entry.getRevision());
		entry = dbProd.put(entry).get(); //want to get this ahead of the stage db
		other1 = dbProd.put(other1).get();
		other2 = db.put(other2).get();
		Assertions.assertEquals(2, entry.getRevision());

		Assertions.assertEquals(1, other1.getRevision());
		Assertions.assertEquals(1, other2.getRevision());
		entry = db.get(SimpleTable.class, "1").get();

		db.links(entry, AnotherTable.class, Arrays.asList("1", "2")).get();

		entry = db.get(SimpleTable.class, "1").get(); //revision will come from stage db

		other1 = db.get(AnotherTable.class, "1").get();
		other2 = db.get(AnotherTable.class, "2").get();
		Assertions.assertEquals(1, entry.getRevision()); //first write into this db

		Assertions.assertEquals(1, other1.getRevision()); //first write into this db
		Assertions.assertEquals(2, other2.getRevision()); //second write into this db
	}

	static class SimpleTable extends Table {

		private String name;

		public SimpleTable() {}

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

		public AnotherTable() {}

		public AnotherTable(String id, String name) {
			setId(id);
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
