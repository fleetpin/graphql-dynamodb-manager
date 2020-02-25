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

final class DynamoDbLinkTest {

	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testSimpleQuery(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
		var garry = db.put(new SimpleTable("garry")).get();
		var john = db.put(new AnotherTable("john")).get();
		var frank = dbProd.put(new SimpleTable("frank")).get();
		var bob = dbProd.put(new AnotherTable("bob")).get();

		db.link(garry, bob.getClass(), bob.getId()).get();
		dbProd.link(frank, bob.getClass(), bob.getId()).get();
		db.link(garry, john.getClass(), john.getId()).get();

		john = db.get(AnotherTable.class, john.getId()).get();
		bob = db.get(AnotherTable.class, bob.getId()).get();

		var johnLink = db.getLink(john, SimpleTable.class).get();
		Assertions.assertEquals("garry", johnLink.name);

		var bobLinks = db.getLinks(bob, SimpleTable.class).get();

		Assertions.assertEquals(1, bobLinks.size());
		bobLinks.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("frank", bobLinks.get(0).name);
	}

	@TestDatabase(useProd = true, organisationIds = {"test", "test"})
	void testDoubleLinkage(final Database db, final Database dbProd) throws InterruptedException, ExecutionException {
		var garry = db.put(new SimpleTable("garry")).get();
		var frank = dbProd.put(new SimpleTable("frank")).get();
		var bob = dbProd.put(new AnotherTable("bob")).get();

		db.link(garry, bob.getClass(), bob.getId()).get();
		dbProd.link(frank, bob.getClass(), bob.getId()).get();

		bob = db.get(AnotherTable.class, bob.getId()).get();

		var bobLinks = db.getLinks(bob, SimpleTable.class).get();

		Assertions.assertEquals(2, bobLinks.size());
		bobLinks.sort(Comparator.comparing(a -> a.name));

		Assertions.assertEquals("frank", bobLinks.get(0).name);
		Assertions.assertEquals("garry", bobLinks.get(1).name);
	}


	@TestDatabase
	void testUpdate(final Database db) throws InterruptedException, ExecutionException {
		var garry = db.put(new SimpleTable("garry")).get();
		var john = db.put(new AnotherTable("john")).get();
		var bob = db.put(new AnotherTable("bob")).get();

		db.link(garry, john.getClass(), john.getId()).get();
		db.link(garry, bob.getClass(), bob.getId()).get();

		garry = db.get(SimpleTable.class, garry.getId()).get();
		john = db.get(AnotherTable.class, john.getId()).get();
		bob = db.get(AnotherTable.class, bob.getId()).get();

		var bobLinks = db.getLink(bob, SimpleTable.class).get();
		Assertions.assertEquals("garry", bobLinks.name);

		var garryLink = db.getLink(garry, AnotherTable.class).get();
		Assertions.assertEquals("bob", garryLink.getName());

		var johnLink = db.getLinks(john, SimpleTable.class).get();
		Assertions.assertEquals(0, johnLink.size());
	}


	@TestDatabase
	void testDelete(final Database db) throws InterruptedException, ExecutionException {
		var garry = db.put(new SimpleTable("garry")).get();
		var john = db.put(new AnotherTable("john")).get();

		db.link(garry, john.getClass(), john.getId()).get();

		Assertions.assertThrows(RuntimeException.class, () -> {
			db.delete(garry, false).get();
		});

		db.delete(garry, true).get();

		var list = db.getLinks(john, SimpleTable.class).get();
		Assertions.assertEquals(0, list.size());
	}

	@TestDatabase
	void testDeleteLinks(final Database db) throws InterruptedException, ExecutionException {
		var garry = db.put(new SimpleTable("garry")).get();
		var john = db.put(new AnotherTable("john")).get();


		garry = db.link(garry, john.getClass(), john.getId()).get();

		garry = db.deleteLinks(garry).get();

		garry = db.get(SimpleTable.class, garry.getId()).get();

		var list = db.getLinks(john, SimpleTable.class).get();
		Assertions.assertEquals(0, list.size());

		var list2 = db.getLinks(garry, AnotherTable.class).get();
		Assertions.assertEquals(0, list2.size());

	}

	//TODO: test link joining logic between global org and multiple databases

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
