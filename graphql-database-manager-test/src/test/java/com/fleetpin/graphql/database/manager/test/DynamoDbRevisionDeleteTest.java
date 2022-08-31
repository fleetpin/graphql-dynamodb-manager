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
 * most of the test cases are cover in the RevisionPut tests this just covers delete failure behavior
 * @author ashley
 *
 */
public class DynamoDbRevisionDeleteTest {

	@TestDatabase
	public void testDelete(final Database db) throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");

		entry = db.put(entry).get();

		Assertions.assertEquals(1, entry.getRevision());

		var snapshot = db.get(SimpleTable.class, entry.getId()).get();

		entry = db.put(entry).get();

		Assertions.assertEquals(1, snapshot.getRevision());
		Assertions.assertEquals(2, entry.getRevision());

		var cause = Assertions.assertThrows(ExecutionException.class, () -> db.delete(snapshot, true).get());
		Assertions.assertEquals(RevisionMismatchException.class, cause.getCause().getClass());

		//confirm no state change
		Assertions.assertEquals(2, db.get(SimpleTable.class, "1").get().getRevision());

		db.delete(entry, true).get();

		//confirm deleted
		Assertions.assertNull(db.get(SimpleTable.class, "1").get());
	}

	@TestDatabase
	public void testMultipleEnv(final @DatabaseNames({ "prod", "stage" }) Database db, @DatabaseNames("prod") final Database dbProd)
		throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");

		entry = dbProd.put(entry).get();

		Assertions.assertEquals(1, entry.getRevision());

		var snapshot = db.get(SimpleTable.class, entry.getId()).get();

		entry = dbProd.put(entry).get();

		Assertions.assertEquals(1, snapshot.getRevision());
		Assertions.assertEquals(2, entry.getRevision());

		db.delete(snapshot, false).get(); // this passes because we don't have it in this database yet so revision check can not be done

		//confirm deleted
		Assertions.assertNull(db.get(SimpleTable.class, "1").get());

		//still in prod
		Assertions.assertEquals(2, dbProd.get(SimpleTable.class, "1").get().getRevision());
	}

	@TestDatabase
	public void testMultipleEnvCheckLinks(final @DatabaseNames({ "prod", "stage" }) Database db, @DatabaseNames("prod") final Database dbProd)
		throws InterruptedException, ExecutionException {
		var entry = new SimpleTable("1", "garry");

		entry = dbProd.put(entry).get();

		Assertions.assertEquals(1, entry.getRevision());

		var snapshot = db.get(SimpleTable.class, entry.getId()).get();

		entry = dbProd.put(entry).get();

		Assertions.assertEquals(1, snapshot.getRevision());
		Assertions.assertEquals(2, entry.getRevision());

		db.delete(snapshot, true).get(); // this passes because we don't have it in this database yet so revision check can not be done

		//confirm deleted
		Assertions.assertNull(db.get(SimpleTable.class, "1").get());

		//still in prod
		Assertions.assertEquals(2, dbProd.get(SimpleTable.class, "1").get().getRevision());
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
