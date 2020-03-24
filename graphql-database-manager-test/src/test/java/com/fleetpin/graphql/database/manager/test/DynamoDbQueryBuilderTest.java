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
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.util.Comparator;
import java.util.concurrent.ExecutionException;

final class DynamoDbQueryBuilderTest {
	static class Ticket extends Table {
		private String id;
		private String value;

		public Ticket(String id, String value) {
			this.id = id;
			this.value = value;
		}

		@Override
		public String toString() {
			return "Ticket{" +
					"id='" + id + '\'' +
					", value='" + value + '\'' +
					'}';
		}
	}

	@TestDatabase
	void testSimpleQuery(final Database db) throws InterruptedException, ExecutionException {
		db.put(new Ticket("budgetId1:123", "123")).get();
		db.put(new Ticket("budgetId1:456", "456")).get();
		db.put(new Ticket("budgetId2:123", "123")).get();

		var result = db.query(Ticket.class, builder -> builder.on(Ticket.class));
		Assertions.assertEquals(3, result.get().size());

		Assertions.assertEquals(2, db.query(Ticket.class, builder -> builder.on(Ticket.class).limit(2)).get().size());
	}

}
