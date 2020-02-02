package com.fleetpin.graphql.dynamodb.manager;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fleetpin.graphql.dynamodb.manager.Table;
import com.fleetpin.graphql.dynamodb.manager.TableName;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public class DynamoDBInheritanceTest extends DynamoDBBase {

	@Test
	public void testSimplePutGetDelete() throws InterruptedException, ExecutionException {
		var db = getDatabase("test");

		db.put(new NameTable("garry")).get();
		db.put(new AgeTable("19")).get();

		var entries = db.query(BaseTable.class).get();

		Assertions.assertEquals(2, entries.size());
		entries.sort(Comparator.comparing(t -> t.getClass().getSimpleName()));
		Assertions.assertEquals("19", ((AgeTable)entries.get(0)).getAge());
		Assertions.assertEquals("garry", ((NameTable)entries.get(1)).getName());
	}




	@TableName("base")
	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
	@JsonSubTypes({ 
		@Type(value = NameTable.class, name = "name"), 
		@Type(value = AgeTable.class, name = "age")
	})
	static abstract class BaseTable extends Table {
	}

	static class NameTable extends BaseTable {
		String name;

		public NameTable() {
		}
		public NameTable(String name) {
			this.name = name;
		}


		public String getName() {
			return name;
		}
	}
	static class AgeTable extends BaseTable {
		String name;

		public AgeTable() {
		}
		public AgeTable(String age) {
			this.name = age;
		}


		public String getAge() {
			return name;
		}
	}
}
