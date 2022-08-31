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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.dynamo.DynamoDbManager;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

final class DynamoDbInitializer {

	@SuppressWarnings("unchecked")
	static void createTable(final DynamoDbAsyncClient client, final String name) throws ExecutionException, InterruptedException {
		if (client.listTables().get().tableNames().contains(name)) {
			return;
		}

		client
			.createTable(t ->
				t
					.tableName(name)
					.keySchema(
						KeySchemaElement.builder().attributeName("organisationId").keyType(KeyType.HASH).build(),
						KeySchemaElement.builder().attributeName("id").keyType(KeyType.RANGE).build()
					)
					.streamSpecification(streamSpecification -> streamSpecification.streamEnabled(true).streamViewType(StreamViewType.NEW_IMAGE))
					.globalSecondaryIndexes(builder ->
						builder
							.indexName("secondaryGlobal")
							.provisionedThroughput(p -> p.readCapacityUnits(10L).writeCapacityUnits(10L))
							.projection(b -> b.projectionType(ProjectionType.ALL))
							.keySchema(KeySchemaElement.builder().attributeName("secondaryGlobal").keyType(KeyType.HASH).build())
					)
					.localSecondaryIndexes(builder ->
						builder
							.indexName("secondaryOrganisation")
							.projection(b -> b.projectionType(ProjectionType.ALL))
							.keySchema(
								KeySchemaElement.builder().attributeName("organisationId").keyType(KeyType.HASH).build(),
								KeySchemaElement.builder().attributeName("secondaryOrganisation").keyType(KeyType.RANGE).build()
							)
					)
					.attributeDefinitions(
						AttributeDefinition.builder().attributeName("organisationId").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("id").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("secondaryGlobal").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("secondaryOrganisation").attributeType(ScalarAttributeType.S).build()
					)
					.provisionedThroughput(p -> p.readCapacityUnits(10L).writeCapacityUnits(10L).build())
			)
			.get();
	}

	static void createHistoryTable(final DynamoDbAsyncClient client, final String name) throws ExecutionException, InterruptedException {
		if (client.listTables().get().tableNames().contains(name)) {
			return;
		}

		client
			.createTable(t ->
				t
					.tableName(name)
					.keySchema(
						KeySchemaElement.builder().attributeName("organisationIdType").keyType(KeyType.HASH).build(),
						KeySchemaElement.builder().attributeName("idRevision").keyType(KeyType.RANGE).build()
					)
					.localSecondaryIndexes(
						builder ->
							builder
								.indexName("startsWithUpdatedAt")
								.projection(b -> b.projectionType(ProjectionType.ALL))
								.keySchema(
									KeySchemaElement.builder().attributeName("organisationIdType").keyType(KeyType.HASH).build(),
									KeySchemaElement.builder().attributeName("startsWithUpdatedAt").keyType(KeyType.RANGE).build()
								),
						builder ->
							builder
								.indexName("idDate")
								.projection(b -> b.projectionType(ProjectionType.ALL))
								.keySchema(
									KeySchemaElement.builder().attributeName("organisationIdType").keyType(KeyType.HASH).build(),
									KeySchemaElement.builder().attributeName("idDate").keyType(KeyType.RANGE).build()
								)
					)
					.attributeDefinitions(
						AttributeDefinition.builder().attributeName("organisationIdType").attributeType(ScalarAttributeType.S).build(),
						AttributeDefinition.builder().attributeName("idRevision").attributeType(ScalarAttributeType.B).build(),
						AttributeDefinition.builder().attributeName("idDate").attributeType(ScalarAttributeType.B).build(),
						AttributeDefinition.builder().attributeName("startsWithUpdatedAt").attributeType(ScalarAttributeType.B).build()
					)
					.provisionedThroughput(p -> p.readCapacityUnits(10L).writeCapacityUnits(10L).build())
			)
			.get();
	}

	static DynamoDBProxyServer startDynamoServer(final String port) throws Exception {
		final String[] localArgs = { "-inMemory", "-port", port };
		final var server = ServerRunner.createServerFromCommandLineArgs(localArgs);
		server.start();

		return server;
	}

	static DynamoDbAsyncClient startDynamoClient(final String port) throws URISyntaxException {
		return DynamoDbAsyncClient
			.builder()
			.region(Region.AWS_GLOBAL)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("anything", "anything")))
			.endpointOverride(new URI("http://localhost:" + port))
			.build();
	}

	static DynamoDbStreamsAsyncClient startDynamoStreamClient(final String port) throws URISyntaxException {
		return DynamoDbStreamsAsyncClient
			.builder()
			.region(Region.AWS_GLOBAL)
			.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("anything", "anything")))
			.endpointOverride(new URI("http://localhost:" + port))
			.build();
	}

	static String findFreePort() throws IOException {
		final var serverSocket = new ServerSocket(0);
		final var port = String.valueOf(serverSocket.getLocalPort());
		serverSocket.close();

		return port;
	}

	static Database getEmbeddedDatabase(final DynamoDbManager dynamoDbManager, final String organisationId, final CompletableFuture<Object> future) {
		final var database = dynamoDbManager.getDatabase(organisationId);
		database.start(future);

		return database;
	}

	static DynamoDbManager getDatabaseManager(final DynamoDbAsyncClient client, final String[] tables, String historyTable) {
		return DynamoDbManager.builder().tables(tables).dynamoDbAsyncClient(client).historyTable(historyTable).build();
	}
	//    static Database getInMemoryDatabase(
	//            final String organisationId,
	//            final ConcurrentHashMap<DatabaseKey, Table> map,
	//            final CompletableFuture<Object> future
	//    ) {
	//        final var objectMapper = new ObjectMapper()
	//                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
	//                .registerModule(new ParameterNamesModule())
	//                .registerModule(new Jdk8Module())
	//                .registerModule(new JavaTimeModule())
	//                .disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
	//                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
	//                .disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
	//                .disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
	//                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
	//
	//        final var factory = new JsonNodeFactory(false);
	//
	//        final Supplier<String> idGenerator = () -> UUID.randomUUID().toString();
	//
	//        final var database = DynamoDbManager.builder()
	//                .tables("local")
	//                .dynamoDb(new InMemoryDynamoDb(objectMapper, factory, map, idGenerator))
	//                .build()
	//                .getDatabase(organisationId);
	//
	//        database.start(future);
	//        return database;
	//    }
}
