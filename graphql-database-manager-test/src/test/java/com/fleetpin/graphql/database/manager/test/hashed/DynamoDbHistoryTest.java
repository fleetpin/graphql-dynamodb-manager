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

package com.fleetpin.graphql.database.manager.test.hashed;

import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.QueryHistoryBuilder;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.Hash;
import com.fleetpin.graphql.database.manager.annotations.History;
import com.fleetpin.graphql.database.manager.test.HistoryProcessor;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Assertions;

final class DynamoDbHistoryTest {

	@TestDatabase(hashed = true)
	void testGetRevisionsById(final Database db, final HistoryProcessor historyProcessor) throws InterruptedException, ExecutionException {
		var table1 = new SimpleTable("revision1");
		table1.setId("hash:testTable1");
		db.put(table1).get();
		table1 = new SimpleTable("revision2");
		table1.setId("hash:testTable1");
		table1.setRevision(1);
		db.put(table1).get();

		historyProcessor.process();
		var history = db.queryHistory(QueryHistoryBuilder.create(SimpleTable.class).id("hash:testTable1").build()).get();
		Assertions.assertEquals(2, history.size());
		Assertions.assertEquals("revision1", history.get(0).getName());
		Assertions.assertEquals(1L, history.get(0).getRevision());
		Assertions.assertEquals("revision2", history.get(1).getName());
		Assertions.assertEquals(2L, history.get(1).getRevision());
	}

	@History
	@Hash(SimplerHasher.class)
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
}
