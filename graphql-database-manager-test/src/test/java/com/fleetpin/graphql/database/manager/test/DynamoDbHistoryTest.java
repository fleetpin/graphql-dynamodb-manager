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
import com.fleetpin.graphql.database.manager.annotations.TableName;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.time.Instant;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;

final class DynamoDbHistoryTest {

	@TestDatabase
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

	@TestDatabase
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

	@TestDatabase
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

	@TestDatabase
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

	@TestDatabase
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

	@TestDatabase
	void testLotsOfRevisions(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		for (int i = 0; i < 20; i++) {
			var table1 = new SimpleTable("revision" + (i + 1));
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

	@TestDatabase
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
		var history = db
			.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").fromUpdatedAt(revision2Time).toUpdatedAt(revision4Time).build())
			.get();
		Assertions.assertEquals(3, history.size());
		Assertions.assertEquals("revision2", history.get(0).getName());
		Assertions.assertEquals(2L, history.get(0).getRevision());
		Assertions.assertEquals("revision3", history.get(1).getName());
		Assertions.assertEquals(3L, history.get(1).getRevision());
		Assertions.assertEquals("revision4", history.get(2).getName());
		Assertions.assertEquals(4L, history.get(2).getRevision());
		Assertions.assertEquals(revision4Time, history.get(2).getUpdatedAt());
	}

	@TestDatabase
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

	@TestDatabase
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

	@TestDatabase
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
		var history = db
			.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).startsWith("test").fromUpdatedAt(revision2Time).toUpdatedAt(revision4Time).build())
			.get();
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

	@TestDatabase
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

	@TestDatabase
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

	@TestDatabase
	void testNoHistoryTable(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		var table1 = new NoHistorySameNameTable("revision1");
		table1.setId("testTable1");
		db.put(table1).get();
		table1 = new NoHistorySameNameTable("revision2");
		table1.setId("testTable1");
		table1.setRevision(1);
		db.put(table1).get();

		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SameNameTable.class).id("testTable1").build()).get();
		Assertions.assertEquals(0, history.size());
	}

	@TestDatabase
	void testNoHistoryTable2(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		Assertions.assertThrows(
			IllegalArgumentException.class,
			() -> {
				var table1 = new NoHistorySameNameTable("revision1");
				table1.setId("testTable1");
				db.put(table1).get();
				table1 = new NoHistorySameNameTable("revision2");
				table1.setId("testTable1");
				table1.setRevision(1);
				db.put(table1).get();

				historyProcessor.process();
				db.queryHistory(QueryHistoryBuilder.create(NoHistorySameNameTable.class).id("testTable1").build()).get();
			},
			"Can only do history when history annotation is present."
		);
	}

	@TestDatabase
	void testIdAndStartWith(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		Assertions.assertThrows(
			IllegalArgumentException.class,
			() -> {
				var table1 = new SimpleTable("revision1");
				table1.setId("testTable1");
				db.put(table1).get();

				historyProcessor.process();
				db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("testTable1").startsWith("test").build()).get();
			},
			"ID and StartsWith cannot both be set."
		);
	}

	@TestDatabase
	void testIdAndStartWith2(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		Assertions.assertThrows(
			IllegalArgumentException.class,
			() -> {
				var table1 = new SimpleTable("revision1");
				table1.setId("testTable1");
				db.put(table1).get();

				historyProcessor.process();
				db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).build()).get();
			},
			"ID or StartsWith must be set."
		);
	}

	@TestDatabase
	void testRevisionAndCreatedAt(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		Assertions.assertThrows(
			IllegalArgumentException.class,
			() -> {
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

				historyProcessor.process();
				db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).startsWith("test").fromUpdatedAt(revision2Time).fromRevision(1L).build()).get();
			},
			"Revision and CreatedAt cannot both be set."
		);
	}

	@TestDatabase
	void testStartWith(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		Assertions.assertThrows(
			IllegalArgumentException.class,
			() -> {
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

				historyProcessor.process();
				db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).startsWith("test").fromRevision(1L).build()).get();
			},
			"StartsWith can only be used with updatedAt."
		);
	}

	@History
	static class SimpleTable extends Table {

		private String name;

		public SimpleTable() {}

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

	@History
	static class AnotherTable extends Table {

		private String name;

		public AnotherTable() {}

		public AnotherTable(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	@History
	@TableName("SameName")
	static class SameNameTable extends Table {

		private String name;

		public SameNameTable() {}

		public SameNameTable(String name) {
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

	@TableName("SameName")
	static class NoHistorySameNameTable extends Table {

		private String name;

		public NoHistorySameNameTable() {}

		public NoHistorySameNameTable(String name) {
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
}
