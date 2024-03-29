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

import static com.fleetpin.graphql.database.manager.test.DynamoDbInitializer.*;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.dynamo.DynamoDbManager;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseOrganisation;
import com.fleetpin.graphql.database.manager.test.annotations.GlobalEnabled;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.lang.reflect.AnnotatedElement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

public final class TestDatabaseProvider implements ArgumentsProvider {

	private DynamoDBProxyServer server;
	private CompletableFuture<Object> finished;

	@Override
	public Stream<Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
		closePreviousRun();

		final String port = findFreePort();

		server = startDynamoServer(port);
		final var client = startDynamoClient(port);
		final var streamClient = startDynamoStreamClient(port);

		System.setProperty("sqlite4java.library.path", "native-libs");

		finished = new CompletableFuture<>();

		final var testMethod = extensionContext.getRequiredTestMethod();
		final var organisationId = testMethod.getAnnotation(TestDatabase.class).organisationId();
		final var hashed = testMethod.getAnnotation(TestDatabase.class).hashed();
		final var classPath = testMethod.getAnnotation(TestDatabase.class).classPath();

		final var withHistory = Arrays
			.stream(testMethod.getParameters())
			.map(parameter -> parameter.getType().isAssignableFrom(HistoryProcessor.class))
			.filter(p -> p)
			.findFirst()
			.orElse(false);

		final var argumentsList = Arrays
			.stream(testMethod.getParameters())
			.map(parameter -> {
				try {
					if (parameter.getType().isAssignableFrom(DynamoDbManager.class)) {
						return createDynamoDbManager(client, streamClient, parameter, withHistory, hashed, classPath);
					} else if (parameter.getType().isAssignableFrom(HistoryProcessor.class)) {
						return new HistoryProcessor(client, streamClient, parameter, organisationId);
					} else {
						return createDatabase(client, streamClient, parameter, organisationId, withHistory, hashed, classPath);
					}
				} catch (final Exception e) {
					e.printStackTrace();
					throw new ExceptionInInitializerError("Could not build parameters");
				}
			})
			.collect(Collectors.toList());

		return Stream.of(gatherArguments(argumentsList));
	}

	private void closePreviousRun() throws Exception {
		if (server != null) {
			finished.complete(null);
			server.stop();
		}
	}

	private Database createDatabase(
		final DynamoDbAsyncClient client,
		final DynamoDbStreamsAsyncClient streamClient,
		final AnnotatedElement parameter,
		final String organisationId,
		final boolean withHistory,
		final boolean hashed,
		final String classPath
	) throws ExecutionException, InterruptedException {
		final var databaseOrganisation = parameter.getAnnotation(DatabaseOrganisation.class);
		final var correctOrganisationId = databaseOrganisation != null ? databaseOrganisation.value() : organisationId;

		final var dynamoDbManager = createDynamoDbManager(client, streamClient, parameter, withHistory, hashed, classPath);

		return getEmbeddedDatabase(dynamoDbManager, correctOrganisationId, finished);
	}

	private DynamoDbManager createDynamoDbManager(
		final DynamoDbAsyncClient client,
		final DynamoDbStreamsAsyncClient streamClient,
		final AnnotatedElement parameter,
		final boolean withHistory,
		final boolean hashed,
		final String classPath
	) throws ExecutionException, InterruptedException {
		final var databaseNames = parameter.getAnnotation(DatabaseNames.class);
		var tables = databaseNames != null ? databaseNames.value() : new String[] { "table" };

		String historyTable = null;
		for (final String table : tables) {
			createTable(client, table);
			var streamArn = client.describeTable(builder -> builder.tableName(table).build()).get().table().latestStreamArn();
			var tName = streamClient.describeStream(builder -> builder.streamArn(streamArn).build()).get().streamDescription().tableName();
			//System.out.println("find me: " + tName);
			if (withHistory) {
				historyTable = table + "_history";
				createHistoryTable(client, historyTable);
			}
		}

		final var globalEnabledAnnotation = parameter.getAnnotation(GlobalEnabled.class);

		var globalEnabled = true;
		if (globalEnabledAnnotation != null) {
			globalEnabled = globalEnabledAnnotation.value();
		}

		return getDatabaseManager(client, tables, historyTable, globalEnabled, hashed, classPath);
	}

	private Arguments gatherArguments(final List<Object> argumentsList) {
		final var argumentObjects = new Object[argumentsList.size()];
		for (int i = 0; i < argumentObjects.length; i++) {
			argumentObjects[i] = argumentsList.get(i);
		}

		return Arguments.of(argumentObjects);
	}
}
