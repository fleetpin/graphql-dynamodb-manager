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
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;
import org.w3c.dom.Attr;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

final class DynamoDbQueryBuilderTest {
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
			return "Ticket{" +
					"id='" + getId() + '\'' +
					", value='" + value + '\'' +
					'}';
		}
	}

	@TestDatabase
	void testSimpleQuery(final Database db) throws InterruptedException, ExecutionException {
		db.put(new Ticket("budgetId1:123", "123")).get();
		db.put(new Ticket("budgetId1:456", "456")).get();
		db.put(new Ticket("budgetId2:123", "123")).get();

		Assertions.assertEquals(2, db.query(Ticket.class, builder -> builder.startsWith("ticket:budgetId1")).get().size());

		var result = db.query(Ticket.class, builder -> builder);
		Assertions.assertEquals(3, result.get().size());
	}

	@TestDatabase
	void testFrom(final Database db, final DynamoDbAsyncClient client) throws InterruptedException, ExecutionException {
		db.put(new Ticket("budgetId1:1", "1")).get();
		db.put(new Ticket("budgetId1:2", "2")).get();
		db.put(new Ticket("budgetId1:3", "3")).get();
		db.put(new Ticket("budgetId1:4", "4")).get();
		db.put(new Ticket("budgetId1:5", "5")).get();
		db.put(new Ticket("budgetId1:6", "6")).get();
		db.put(new Ticket("budgetId1:7", "7")).get();
		db.put(new Ticket("budgetId1:8", "8")).get();
		db.put(new Ticket("budgetId1:9", "9")).get();
		db.put(new Ticket("budgetId1:10", "10")).get();
		db.put(new Ticket("budgetId1:11", "11")).get();
		db.put(new Ticket("budgetId1:12", "12")).get();
		db.put(new Ticket("budgetId1:13", "13")).get();
		db.put(new Ticket("budgetId2:1", "123")).get();

		var result = db.query(Ticket.class, builder -> builder.startsWith("budgetId1:").after("budgetId1:2").limit(3)).get();
		Assertions.assertEquals(3, result.size());
		Assertions.assertEquals("budgetId1:3", result.get(0).getId());
		Assertions.assertEquals("3", result.get(0).getValue());

//		Map<String, AttributeValue> keyConditions = new HashMap<>();
//		keyConditions.put(":organisationId", AttributeValue.builder().s("organisation").build());
//		keyConditions.put(":table", AttributeValue.builder().s("tickets:budgetId1:").build());
//
//		client.queryPaginator(builder -> {
//			builder.consistentRead(true).tableName("table").keyConditionExpression("organisationId = :organisationId AND begins_with(id, :table)")
//					.expressionAttributeValues(keyConditions)
//					.exclusiveStartKey(Map.of("organisationId", AttributeValue.builder().s("organisation").build(),
//							"id", AttributeValue.builder().s("tickets:budgetId1:456").build()))
//					.limit(2);
//		}).subscribe(z -> {
//			System.out.println(z);
//		}).thenApply(__ -> "ok").get();
//		Map<String, AttributeValue> keyConditions = new HashMap<>();
//		keyConditions.put(":organisationId", AttributeValue.builder().s("organisation").build());
//		keyConditions.put(":table", AttributeValue.builder().s("tickets:budgetId1:").build());
//
//		client.queryPaginator(builder -> {
//			builder.consistentRead(true).tableName("table").keyConditionExpression("organisationId = :organisationId AND begins_with(id, :table)")
//					.expressionAttributeValues(keyConditions)
//					.consistentRead(true)
//					.applyMutation(x -> x.limit(2));
////					.exclusiveStartKey(Map.of("organisationId", AttributeValue.builder().s("organisation").build(),
////							"id", AttributeValue.builder().s("tickets:budgetId1:456").build()));
////					.limit(2);
//
//			System.out.println(builder);
//		}).subscribe(z -> {
//			System.out.println(z);
//		}).thenApply(__ -> "ok").get();


	}

}
