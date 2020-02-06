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

import com.fleetpin.graphql.dynamodb.manager.Table;

public class DynamoDBLinkTest extends DynamoDBBase {

	@Test
	public void testSimpleQuery() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var dbProd = getDatabaseProduction("test");
		
		var garry = db.put(new SimpleTable("garry")).get();
		var john = db.put(new AnotherTable("john")).get();
		var frank = dbProd.put(new SimpleTable("frank")).get();
		var bob = dbProd.put(new AnotherTable("bob")).get();
		
		db.link(garry, bob.getClass(), bob.getId()).get();
		dbProd.link(frank, bob.getClass(), bob.getId()).get();
		db.link(garry, john.getClass(), john.getId()).get();

		garry = db.get(SimpleTable.class, garry.getId()).get();
		frank = db.get(SimpleTable.class, frank.getId()).get();
		john = db.get(AnotherTable.class, john.getId()).get();
		bob = db.get(AnotherTable.class, bob.getId()).get();
		
		var johnLink = db.getLink(john, SimpleTable.class).get();
		Assertions.assertEquals("garry", johnLink.name);
		
		var bobLinks = db.getLinks(bob, SimpleTable.class).get();
		
		Assertions.assertEquals(1, bobLinks.size());
		bobLinks.sort(Comparator.comparing(a -> a.name));
		
		Assertions.assertEquals("frank", bobLinks.get(0).name);
	}
	
	@Test
	public void testDoubleLinkage() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var dbProd = getDatabaseProduction("test");
		
		var garry = db.put(new SimpleTable("garry")).get();
		var frank = dbProd.put(new SimpleTable("frank")).get();
		var bob = dbProd.put(new AnotherTable("bob")).get();
		
		db.link(garry, bob.getClass(), bob.getId()).get();
		dbProd.link(frank, bob.getClass(), bob.getId()).get();

		garry = db.get(SimpleTable.class, garry.getId()).get();
		frank = db.get(SimpleTable.class, frank.getId()).get();
		bob = db.get(AnotherTable.class, bob.getId()).get();
		
		var bobLinks = db.getLinks(bob, SimpleTable.class).get();
		
		Assertions.assertEquals(2, bobLinks.size());
		bobLinks.sort(Comparator.comparing(a -> a.name));
		
		Assertions.assertEquals("frank", bobLinks.get(0).name);
		Assertions.assertEquals("garry", bobLinks.get(1).name);
	}
	
	
	@Test
	public void testUpdate() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
	
	
	@Test
	public void testDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
	
	@Test
	public void testDeleteLinks() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
