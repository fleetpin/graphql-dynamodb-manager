package com.fleetpin.dynamodb.manager;

import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fleetpin.dynamodb.manager.Table;

public class DynamoDBPutGetDeleteTest extends DynamoDBBase {

	@Test
	public void testSimplePutGetDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		
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
	
	@Test
	public void testGlobalPutGetDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var db2 = getDatabase("test");
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
	
	@Test
	public void testClimbingSimplePutGetDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var prod = getDatabaseProduction("test");
		
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = prod.put(entry1).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertNotNull(entry1.getId());
		
		String id = entry1.getId();
		
		entry1 = db.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		db.delete(entry1, false).get();
		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertNull(entry1);
		
		entry1 = prod.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		var entry2 = new SimpleTable("two");
		entry2.setId(entry1.getId());
		db.put(entry2).get();
		
		entry1 = prod.get(SimpleTable.class, id).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		

		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertEquals("two", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		
	}
	
	@Test
	public void testClimbingGlobalPutGetDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var prod = getDatabaseProduction("test");
		
		SimpleTable entry1 = new SimpleTable("garry");
		entry1 = prod.putGlobal(entry1).get();
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
		
		entry1 = prod.get(SimpleTable.class, id).get();

		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		var entry2 = new SimpleTable("two");
		entry2.setId(entry1.getId());
		db.put(entry2).get();
		
		entry1 = prod.get(SimpleTable.class, id).get();
		Assertions.assertEquals("garry", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		

		entry1 = db.get(SimpleTable.class, id).get();
		Assertions.assertEquals("two", entry1.getName());
		Assertions.assertEquals(id, entry1.getId());
		
		
	}
	
	@Test
	public void testTwoOrganisationsPutGetDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");
		var db2 = getDatabase("test2");
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
