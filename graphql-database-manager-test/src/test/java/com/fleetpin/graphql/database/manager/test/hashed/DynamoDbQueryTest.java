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

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.Hash;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.util.concurrent.ExecutionException;

final class DynamoDbQueryTest {

	@TestDatabase(hashed = true)
	void testSimpleQuery(final Database db) throws InterruptedException, ExecutionException {
		db.put(new SimpleTable("garry")).get();
		db.put(new SimpleTable("bob")).get();
		db.put(new SimpleTable("frank")).get();

		assertThrows(RuntimeException.class, () -> db.query(SimpleTable.class).join());
	}

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
