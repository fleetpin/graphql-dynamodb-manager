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
package com.fleetpin.graphql.dynamodb.manager;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.ServerSocket;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class DynamoDBBase {

	private DynamoDBProxyServer server;
	private DynamoDbManager local;
	private DynamoDbManager stage;
	private DynamoDbManager production;
	private CompletableFuture<Void> finished;
	private DynamoDbAsyncClient async;


	private static void createTable(DynamoDbAsyncClient client, String name) throws InterruptedException, ExecutionException {
		client.createTable(t -> t.tableName(name).keySchema(
				KeySchemaElement.builder()
				.attributeName("organisationId")
				.keyType(KeyType.HASH)
				.build(),
				KeySchemaElement.builder()
				.attributeName("id")
				.keyType(KeyType.RANGE)
				.build()
				).globalSecondaryIndexes(builder -> builder.indexName("secondaryGlobal").provisionedThroughput(p -> p.readCapacityUnits(10L).writeCapacityUnits(10L)).projection(b -> b.projectionType(ProjectionType.ALL)).keySchema(KeySchemaElement.builder()
						.attributeName("secondaryGlobal")
						.keyType(KeyType.HASH)
						.build()))
				.localSecondaryIndexes(builder -> builder.indexName("secondaryOrganisation").projection(b -> b.projectionType(ProjectionType.ALL)).keySchema(	KeySchemaElement.builder()
						.attributeName("organisationId")
						.keyType(KeyType.HASH)
						.build(),
						KeySchemaElement.builder()
						.attributeName("secondaryOrganisation")
						.keyType(KeyType.RANGE)
						.build()))
				.attributeDefinitions(
						AttributeDefinition.builder().attributeName("organisationId").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("secondaryGlobal").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("secondaryOrganisation").attributeType(ScalarAttributeType.S).build()
						).provisionedThroughput(p -> p.readCapacityUnits(10L).writeCapacityUnits(10L).build())
				).get();
	}

	@BeforeEach
	public void setup() throws Exception {
		finished = new CompletableFuture<Void>();
		System.setProperty("sqlite4java.library.path", "native-libs");
		ServerSocket s = new ServerSocket(0);
		String port = Integer.toString(s.getLocalPort());
		s.close();
		final String[] localArgs = { "-inMemory", "-port", port };
		server = ServerRunner.createServerFromCommandLineArgs(localArgs);
		server.start();

		this.async = DynamoDbAsyncClient.builder().endpointOverride(new URI("http://localhost:" + port)).build();

		createTable(async, "stage");
		createTable(async, "prod");

		final var objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).registerModule(new ParameterNamesModule())
				.registerModule(new Jdk8Module())
				.registerModule(new JavaTimeModule())
				.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS).disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
				.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
		final var concurrentHashMap = new ConcurrentHashMap<DatabaseKey, DynamoItem>();

		this.local = DynamoDbManager.builder().tables("local").dynamoDb(new InMemoryDynamoDb(objectMapper, concurrentHashMap, () -> UUID.randomUUID().toString())).build();
		this.stage = DynamoDbManager.builder().tables("prod", "stage").dynamoDbAsyncClient(async).build();
		this.production = DynamoDbManager.builder().tables("prod").dynamoDbAsyncClient(async).build();
	}
	
	public DynamoDbAsyncClient getAsync() {
		return async;
	}

	@AfterEach
	public void tearDown() throws Exception {
		finished.complete(null);
		server.stop();
	}
	
	//TODO: add in memory option that we can also use to speed up testing. Should be able to paramatise tests once implemented
	
	public Database getDatabase(String organisationId) {
		var db = stage.getDatabase(organisationId);
		db.start(finished);
		return db;
	}

	public Database getDatabaseProduction(String organisationId) {
		var db = production.getDatabase(organisationId);
		db.start(finished);
		return db;
	}

	public Database getInMemoryDatabase(final String organisationId) {
		final var db = local.getDatabase(organisationId);
		db.start(finished);
		return db;
	}

	public Database createTestDatabase(final DatabaseType dbType, final String organisationId) {
		Database db;
		switch (dbType) {
			case IN_MEMORY:
				db = getInMemoryDatabase(organisationId);
				break;
			case EMBEDDED:
				db = getDatabase(organisationId);
				break;
			default:
				db = null;
		}
		
		return db;
	}
}
