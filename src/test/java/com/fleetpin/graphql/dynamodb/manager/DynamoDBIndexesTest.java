package com.fleetpin.graphql.dynamodb.manager;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fleetpin.graphql.dynamodb.manager.GlobalIndex;
import com.fleetpin.graphql.dynamodb.manager.SecondaryIndex;
import com.fleetpin.graphql.dynamodb.manager.Table;

public class DynamoDBIndexesTest extends DynamoDBBase {

	@Test
	public void testGlobal() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		var list = db.queryGlobal(SimpleTable.class, "john").get();
		Assertions.assertEquals(1, list.size());
		
		Assertions.assertEquals("garry", list.get(0).getName());
		Assertions.assertEquals("garry", db.queryGlobalUnique(SimpleTable.class, "john").get().getName());
	}

	@Test
	public void testGlobalInheritance() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var prod = getDatabaseProduction("test");
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = prod.put(entry1).get();
		
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
	

	@Test
	public void testSecondary() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = db.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		var list = db.querySecondary(SimpleTable.class, "garry").get();
		Assertions.assertEquals(1, list.size());
		
		Assertions.assertEquals("garry", list.get(0).getName());
		Assertions.assertEquals("garry", db.querySecondaryUnique(SimpleTable.class, "garry").get().getName());
	}
	@Test
	public void testSecondaryInheritance() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var prod = getDatabaseProduction("test");
		SimpleTable entry1 = new SimpleTable("garry", "john");
		entry1 = prod.put(entry1).get();
		
		SimpleTable entry2 = new SimpleTable("garry", "barry");
		entry2.setId(entry1.getId());
		db.put(entry2);
		
		
		
		var list = db.querySecondary(SimpleTable.class, "garry").get();
		Assertions.assertEquals(1, list.size());
		
		Assertions.assertEquals("barry", list.get(0).getGlobalLookup());
		Assertions.assertEquals("barry", db.querySecondaryUnique(SimpleTable.class, "garry").get().getGlobalLookup());
	}
	static class SimpleTable extends Table {
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
