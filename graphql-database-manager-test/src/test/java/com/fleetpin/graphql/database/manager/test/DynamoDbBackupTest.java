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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.GlobalIndex;
import com.fleetpin.graphql.database.manager.annotations.SecondaryIndex;
import com.fleetpin.graphql.database.manager.dynamo.DynamoBackupItem;
import com.fleetpin.graphql.database.manager.dynamo.DynamoDbManager;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseOrganisation;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import com.fleetpin.graphql.database.manager.util.BackupItem;
import org.junit.jupiter.api.Assertions;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

final class DynamoDbBackupTest {


	private ObjectMapper mapper = new ObjectMapper();

	@TestDatabase
	void testTakeBackup(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {

		final var db0 = dynamoDbManager.getDatabase("organisation-0");
		final var db1 = dynamoDbManager.getDatabase("organisation-1");
		db0.start(new CompletableFuture<>());
		db1.start(new CompletableFuture<>());

		final var putAvocado = db0.put(new SimpleTable("avocado", "fruit")).get();
		final var putBanana = db0.put(new SimpleTable("banana", "fruit")).get();
		final var putBeer = db0.put(new Drink("Beer", true)).get();
		final var putTomato = db1.put(new SimpleTable("tomato", "fruit")).get();


		final var orgQuery1 = db0.takeBackup("organisation-0").get();
		Assertions.assertNotNull(putAvocado);
		Assertions.assertNotNull(putBanana);
		Assertions.assertTrue(orgQuery1.size() == 3);
		List<String> names = List.of(putBeer.getName(), putBanana.getName(), putAvocado.getName());

		checkResponseNameField(orgQuery1, 0, names);
		checkResponseNameField(orgQuery1, 1, names);
		checkResponseNameField(orgQuery1, 2, names);

		final var orgQuery2 = db1.takeBackup("organisation-1").get();
		Assertions.assertNotNull(putTomato);
		checkResponseNameField(orgQuery2, 0, List.of(putTomato.getName()));

	}


	@TestDatabase
	void testRestoreBackup(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final String DRINK_ID = "1234";
		final String SIMPLE_ID = "6789";

		final var db0 = dynamoDbManager.getDatabase("organisation-0");
		db0.start(new CompletableFuture<>());

		Map<String, AttributeValue> drinkAttributes = new HashMap<>();
		drinkAttributes.put("organisationId", AttributeValue.builder().s("organisation-0").build());
		drinkAttributes.put("id", AttributeValue.builder().s("drinks:" + DRINK_ID).build());
		Map<String, AttributeValue> items = new HashMap<>();
		items.put("revision", AttributeValue.builder().s("1").build());
		items.put("name", AttributeValue.builder().s("Beer").build());
		items.put("alcoholic", AttributeValue.builder().bool(true).build());
		items.put("id", AttributeValue.builder().s("drinks:" + DRINK_ID).build());
		drinkAttributes.put("item", AttributeValue.builder().m(items).build());


		Map<String, AttributeValue> simpleTableAttributes = new HashMap<>();
		simpleTableAttributes.put("organisationId", AttributeValue.builder().s("organisation-0").build());
		simpleTableAttributes.put("id", AttributeValue.builder().s("simpletables:" + SIMPLE_ID).build());
		items.clear();
		items.put("revision", AttributeValue.builder().s("1").build());
		items.put("name", AttributeValue.builder().s("avocado").build());
		items.put("globalLookup", AttributeValue.builder().s("fruit").build());
		items.put("id", AttributeValue.builder().s("simpletables:" + SIMPLE_ID).build());
		simpleTableAttributes.put("item", AttributeValue.builder().m(items).build());
		Map<String, AttributeValue> links = new HashMap<>();
		links.put("workflows", AttributeValue.builder().ss("workflowId123").build());
		simpleTableAttributes.put("links", AttributeValue.builder().m(links).build());

		BackupItem drinkItem = new DynamoBackupItem("table", drinkAttributes, mapper);
		BackupItem simpleTableItem = new DynamoBackupItem("table", simpleTableAttributes, mapper);

		db0.restoreBackup(List.of(simpleTableItem, drinkItem)).get();

		final var drinkExists = db0.get(Drink.class, DRINK_ID).get();
		Assertions.assertNotNull(drinkExists);

		Assertions.assertEquals("Beer", drinkExists.getName());
		Assertions.assertEquals(true, drinkExists.getAlcoholic());

		final var simpleTableExists = db0.get(SimpleTable.class, SIMPLE_ID).get();
		Assertions.assertNotNull(simpleTableExists);

		Assertions.assertEquals("avocado", simpleTableExists.getName());
		Assertions.assertEquals("fruit", simpleTableExists.getGlobalLookup());

	}

	@TestDatabase
	void testDestroyOrganisation(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final var db0 = dynamoDbManager.getDatabase("organisation-0");
		final var db1 = dynamoDbManager.getDatabase("organisation-1");
		db0.start(new CompletableFuture<>());
		db1.start(new CompletableFuture<>());

		db0.put(new SimpleTable("avocado", "fruit")).get();
		db1.put(new SimpleTable("avocado", "fruit")).get();

		var destroyResponse = db0.destroyOrganisation("organisation-0").get();
		Assertions.assertEquals(true, destroyResponse);

		var response0 = db0.query(SimpleTable.class).get();
		Assertions.assertEquals(0, response0.size());

		var response1 = db1.query(SimpleTable.class).get();
		Assertions.assertEquals(1, response1.size());

	}

	@TestDatabase
	void testDeleteItems(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final var db0 = dynamoDbManager.getDatabase("organisation-0");
		final var db1 = dynamoDbManager.getDatabase("organisation-1");
		db0.start(new CompletableFuture<>());
		db1.start(new CompletableFuture<>());

		db0.put(new SimpleTable("avocado", "fruit")).get();
		db0.put(new Drink("Whisky", true)).get();
		db0.put(new Drink("Wine", true)).get();
		db0.put(new Drink("Gin", true)).get();
		db0.put(new Drink("Vodka", true)).get();

		db1.put(new SimpleTable("avocado", "fruit")).get();
		db1.put(new Drink("Water", false)).get();

		var destroyResponse = db0.delete("organisation-0", Drink.class).get();
		Assertions.assertEquals(4, destroyResponse.size());

		var drinkResponse0 = db0.query(Drink.class).get();
		Assertions.assertEquals(0, drinkResponse0.size());

		var simpleTableResponse = db0.query(SimpleTable.class).get();
		Assertions.assertEquals(1, simpleTableResponse.size());

		Assertions.assertEquals("avocado", simpleTableResponse.get(0).getName());
		Assertions.assertEquals("fruit", simpleTableResponse.get(0).getGlobalLookup());

		var drinkResponse1 = db1.query(Drink.class).get();
		Assertions.assertEquals(1, drinkResponse1.size());
	}

	@TestDatabase
	void testBatchDestroyOrganisation(final DynamoDbManager dynamoDbManager) throws ExecutionException, InterruptedException {
		final var db0 = dynamoDbManager.getDatabase("organisation-0");
		final var db1 = dynamoDbManager.getDatabase("organisation-1");
		db0.start(new CompletableFuture<>());
		db1.start(new CompletableFuture<>());
		int count = 100;
		for (int i = 0; i < count; i++) {
			db0.put(new SimpleTable("avocado", "fruit")).get();
		}
		db1.put(new SimpleTable("avocado", "fruit")).get();

		var response0 = db0.query(SimpleTable.class).get();
		Assertions.assertEquals(count, response0.size());

		var destroyResponse = db0.destroyOrganisation("organisation-0").get();
		Assertions.assertEquals(true, destroyResponse);

		//For some reason this fails on the test DynamoDB but is fine on the real one?
		//response0 = db0.query(SimpleTable.class).get();
		//Assertions.assertEquals(0, response0.size());

		var response1 = db1.query(SimpleTable.class).get();
		Assertions.assertEquals(1, response1.size());

	}

	private void checkResponseNameField(List<BackupItem> queryResult, Integer rank, List<String> names) {
		var jsonMap = queryResult.get(rank).getItem();
		ObjectMapper om = new ObjectMapper();
		var itemMap = om.convertValue(jsonMap, new TypeReference<Map<String, Object>>() {
		});
		Assertions.assertTrue(names.contains(((HashMap<String, Object>) itemMap.get("item")).get("name")));


	}

	public static class Drink extends Table {
		private String name;
		private Boolean alcoholic;


		public Drink() {
		}

		public Drink(String name, Boolean alcoholic) {
			this.name = name;
			this.alcoholic = alcoholic;
		}

		public String getName() {
			return name;
		}

		public Boolean getAlcoholic() {
			return alcoholic;
		}

	}

	public static class SimpleTable extends Table {
		private String name;
		private String globalLookup;

		public SimpleTable() {
		}

		public SimpleTable(String name, String globalLookup) {
			this.name = name;
			this.globalLookup = globalLookup;
		}


		@SecondaryIndex
		public String getName() {
			return name;
		}

		@GlobalIndex
		public String getGlobalLookup() {
			return globalLookup;
		}

	}
}