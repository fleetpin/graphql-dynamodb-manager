package com.fleetpin.graphql.dynamodb.manager;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fleetpin.graphql.dynamodb.manager.Table;

public class DynamoDBQueryTest extends DynamoDBBase {

	@Test
	public void testSimpleQuery() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
	
	@Test
	public void testTwoTablesQuery() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
	
	

	@Test
	public void testQueryDeleteQuery() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
