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
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.Hash;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;

final class DynamoDbQueryBuilderTest {

	@Hash(SimplerHasher.class)
	static class Ticket extends Table {

		private String value;

		public Ticket(String id, String value) {
			setId(id);
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "Ticket{" + "id='" + getId() + '\'' + ", value='" + value + '\'' + '}';
		}
	}

	@TestDatabase(hashed = true)
	void testAfter(final Database db) throws InterruptedException, ExecutionException {
		db.put(new Ticket("budgetId1:sales;trinkets:2020/10", "6 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2020/11", "7 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2020/12", "8 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2021/01", "9 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2021/02", "10 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets;whatchamacallits:2020/10", "11 whatchamacallits")).get();
		db.put(new Ticket("budgetId1:sales;trinkets;whatchamacallits:2020/11", "12 whatchamacallits")).get();
		db.put(new Ticket("budgetId1:sales;trinkets;whatchamacallits:2020/12", "13 whatchamacallits")).get();
		db.put(new Ticket("budgetId2:usa;expenses;flights;domestic:2020/10", "1 million dollars")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2020/10", "1 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2020/11", "2 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2020/12", "3 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2021/01", "4 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2021/02", "5 widgets")).get();

		var result2 = db.query(Ticket.class, builder -> builder.startsWith("budgetId1:").after("budgetId1:sales;trinkets:2020/10").limit(10)).get();
		Assertions.assertEquals("budgetId1:sales;trinkets:2020/11", result2.get(0).getId());
		Assertions.assertEquals(10, result2.size());
	}

	static String getId(int i) {
		return String.format("%04d", i);
	}
}
