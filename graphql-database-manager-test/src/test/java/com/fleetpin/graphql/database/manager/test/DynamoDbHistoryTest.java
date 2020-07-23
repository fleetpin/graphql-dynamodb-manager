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
import com.fleetpin.graphql.database.manager.QueryHistoryBuilder;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.History;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;

final class DynamoDbHistoryTest {
	@TestDatabase(withHistory=true)
	void testGetRevisionsById(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        db.put(table1).get();
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        db.put(table1).get();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").build()).get();
		Assertions.assertEquals(2, history.size());
		Assertions.assertEquals("revision1", history.get(0).getName());
		Assertions.assertEquals(1L, history.get(0).getRevision());
		Assertions.assertEquals("revision2", history.get(1).getName());
		Assertions.assertEquals(2L, history.get(1).getRevision());
	}

	@TestDatabase(withHistory=true)
	void testHistoryProcessor(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        db.put(table1).get();
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        db.put(table1).get();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").build()).get();
		Assertions.assertEquals(0, history.size());

	}
	
	@TestDatabase(withHistory=true)
	void testHistoryProcessor2(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        db.put(table1).get();
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        db.put(table1).get();
		historyProcessor.process();
		
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").build()).get();
		Assertions.assertEquals(2, history.size());
	}
	
	@TestDatabase(withHistory=true)
	void testFromRevisionToRevisionQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        db.put(table1).get();
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        db.put(table1).get();
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        db.put(table1).get();
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        db.put(table1).get();
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        db.put(table1).get();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").fromRevision(2L).toRevision(3L).build()).get();
		Assertions.assertEquals(2, history.size());
		Assertions.assertEquals("revision2", history.get(0).getName());
		Assertions.assertEquals(2L, history.get(0).getRevision());
		Assertions.assertEquals("revision3", history.get(1).getName());
		Assertions.assertEquals(3L, history.get(1).getRevision());
	}
	
	@TestDatabase(withHistory=true)
	void testFromRevisionQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        db.put(table1).get();
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        db.put(table1).get();
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        db.put(table1).get();
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        db.put(table1).get();
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        db.put(table1).get();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").fromRevision(3L).build()).get();
		Assertions.assertEquals(3, history.size());
		Assertions.assertEquals("revision3", history.get(0).getName());
		Assertions.assertEquals(3L, history.get(0).getRevision());
		Assertions.assertEquals("revision4", history.get(1).getName());
		Assertions.assertEquals(4L, history.get(1).getRevision());
		Assertions.assertEquals("revision5", history.get(2).getName());
		Assertions.assertEquals(5L, history.get(2).getRevision());
	}
	
	@TestDatabase(withHistory=true)
	void testLotsOfRevisions(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {


        for (int i=0; i<20; i++) {
            var table1 = new SimpleTable("revision" + (i+1));
            table1.setId("testTable1");
            table1.setRevision(i);
            db.put(table1).get();
        }
        	
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").fromRevision(1L).toRevision(2L).build()).get();
		Assertions.assertEquals(2, history.size());
		Assertions.assertEquals("revision1", history.get(0).getName());
		Assertions.assertEquals(1L, history.get(0).getRevision());
		Assertions.assertEquals("revision2", history.get(1).getName());
		Assertions.assertEquals(2L, history.get(1).getRevision());
	}
	
	@TestDatabase(withHistory=true)
	void testFromUpdatedAtToUpdatedAtQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		
		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        var revision1Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        var revision2Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        var revision3Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        var revision4Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        var revision5Time = db.put(table1).get().getUpdatedAt();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").fromUpdatedAt(revision2Time).toUpdatedAt(revision4Time).build()).get();
		Assertions.assertEquals(3, history.size());
		Assertions.assertEquals("revision2", history.get(0).getName());
		Assertions.assertEquals(2L, history.get(0).getRevision());
		Assertions.assertEquals("revision3", history.get(1).getName());
		Assertions.assertEquals(3L, history.get(1).getRevision());
		Assertions.assertEquals("revision4", history.get(2).getName());
		Assertions.assertEquals(4L, history.get(2).getRevision());
		Assertions.assertEquals(revision4Time, history.get(2).getUpdatedAt());
	}
	
	@TestDatabase(withHistory=true)
	void testFromUpdatedAtQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        var revision1Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        var revision2Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        var revision3Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        var revision4Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        var revision5Time = db.put(table1).get().getUpdatedAt();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").fromUpdatedAt(revision3Time).build()).get();
		Assertions.assertEquals(3, history.size());
		Assertions.assertEquals("revision3", history.get(0).getName());
		Assertions.assertEquals(3L, history.get(0).getRevision());
		Assertions.assertEquals("revision4", history.get(1).getName());
		Assertions.assertEquals(4L, history.get(1).getRevision());
		Assertions.assertEquals("revision5", history.get(2).getName());
		Assertions.assertEquals(5L, history.get(2).getRevision());
		Assertions.assertEquals(revision4Time, history.get(1).getUpdatedAt());
	}
	
	@TestDatabase(withHistory=true)
	void testToUpdatedAtQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {

		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        var revision1Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        var revision2Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        var revision3Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        var revision4Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        var revision5Time = db.put(table1).get().getUpdatedAt();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").toUpdatedAt(revision3Time).build()).get();
		Assertions.assertEquals(3, history.size());
		Assertions.assertEquals("revision1", history.get(0).getName());
		Assertions.assertEquals(1L, history.get(0).getRevision());
		Assertions.assertEquals("revision2", history.get(1).getName());
		Assertions.assertEquals(2L, history.get(1).getRevision());
		Assertions.assertEquals("revision3", history.get(2).getName());
		Assertions.assertEquals(3L, history.get(2).getRevision());
		Assertions.assertEquals(revision2Time, history.get(1).getUpdatedAt());
	}
	
	@TestDatabase(withHistory=true)
	void testStartWithUpdatedAtToUpdatedAtQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		
		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        var revision1Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        var revision2Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        var revision3Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        var revision4Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        var revision5Time = db.put(table1).get().getUpdatedAt();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).startsWith("test").fromUpdatedAt(revision2Time).toUpdatedAt(revision4Time).build()).get();
		System.out.println(history);
		Assertions.assertEquals(3, history.size());
		Assertions.assertEquals("revision2", history.get(0).getName());
		Assertions.assertEquals(2L, history.get(0).getRevision());
		Assertions.assertEquals("revision3", history.get(1).getName());
		Assertions.assertEquals(3L, history.get(1).getRevision());
		Assertions.assertEquals("revision4", history.get(2).getName());
		Assertions.assertEquals(4L, history.get(2).getRevision());
		Assertions.assertEquals(revision4Time, history.get(2).getUpdatedAt());
	}
	
	@TestDatabase(withHistory=true)
	void testStartWithUpdatedAtQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		
		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        var revision1Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        var revision2Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        var revision3Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        var revision4Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        var revision5Time = db.put(table1).get().getUpdatedAt();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).startsWith("test").fromUpdatedAt(revision2Time).build()).get();
		System.out.println(history);
		Assertions.assertEquals(4, history.size());
		Assertions.assertEquals("revision2", history.get(0).getName());
		Assertions.assertEquals(2L, history.get(0).getRevision());
		Assertions.assertEquals("revision3", history.get(1).getName());
		Assertions.assertEquals(3L, history.get(1).getRevision());
		Assertions.assertEquals("revision4", history.get(2).getName());
		Assertions.assertEquals(4L, history.get(2).getRevision());
		Assertions.assertEquals(revision4Time, history.get(2).getUpdatedAt());
		Assertions.assertEquals("revision5", history.get(3).getName());
		Assertions.assertEquals(5L, history.get(3).getRevision());
	}
	
	@TestDatabase(withHistory=true)
	void testStartWithToUpdatedAtQuery(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		
		var table1 = new SimpleTable("revision1");
		table1.setId("testTable1");
        var revision1Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision2");
        table1.setId("testTable1");
        table1.setRevision(1);
        var revision2Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision3");
        table1.setId("testTable1");
        table1.setRevision(2);
        var revision3Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision4");
        table1.setId("testTable1");
        table1.setRevision(3);
        var revision4Time = db.put(table1).get().getUpdatedAt();
        Thread.sleep(1000);
        
        table1 = new SimpleTable("revision5");
        table1.setId("testTable1");
        table1.setRevision(4);
        var revision5Time = db.put(table1).get().getUpdatedAt();
		
		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).startsWith("test").toUpdatedAt(revision4Time).build()).get();
		System.out.println(history);
		Assertions.assertEquals(4, history.size());
		Assertions.assertEquals("revision1", history.get(0).getName());
		Assertions.assertEquals(1L, history.get(0).getRevision());
		Assertions.assertEquals("revision2", history.get(1).getName());
		Assertions.assertEquals(2L, history.get(1).getRevision());
		Assertions.assertEquals("revision3", history.get(2).getName());
		Assertions.assertEquals(3L, history.get(2).getRevision());
		Assertions.assertEquals("revision4", history.get(3).getName());
		Assertions.assertEquals(4L, history.get(3).getRevision());
		Assertions.assertEquals(revision4Time, history.get(3).getUpdatedAt());
	}
	
	@History
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
		
		@Override
		public String toString() {
			return name;
		}
	}
// need add test to test noHistory
	@History
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
