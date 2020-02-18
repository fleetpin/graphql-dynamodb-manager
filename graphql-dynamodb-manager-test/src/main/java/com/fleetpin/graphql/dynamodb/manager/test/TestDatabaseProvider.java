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

package com.fleetpin.graphql.dynamodb.manager.test;

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.fleetpin.graphql.dynamodb.manager.DatabaseKey;
import com.fleetpin.graphql.dynamodb.manager.DynamoItem;
import com.fleetpin.graphql.dynamodb.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.fleetpin.graphql.dynamodb.manager.test.DynamoDbInitializer.*;

public final class TestDatabaseProvider implements ArgumentsProvider {
    private DynamoDBProxyServer server;
    private CompletableFuture<Object> finished;

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
        closePreviousRun();

        final String port = findFreePort();

        server = startDynamoServer(port);
        final var client = startDynamoClient(port);

        System.setProperty("sqlite4java.library.path", "native-libs");
        createTable(client, "prod");
        createTable(client, "stage");

        finished = new CompletableFuture<>();

        final var testMethod = extensionContext.getRequiredTestMethod();
        final var testDatabase = testMethod.getAnnotation(TestDatabase.class);
        final var organisationIds = testDatabase.organisationIds();

        if (testMethod.getParameterCount() == 1) {
            return Stream.of(
                    Arguments.of(getInMemoryDatabase(organisationIds[0], new ConcurrentHashMap<>(), finished)),
                    Arguments.of(getEmbeddedDatabase(organisationIds[0], client, finished))
            );
        } else {
            if (organisationIds.length < 2) {
                throw new IllegalArgumentException("Not enough organisationIds were provided via @TestDatabase.");
            }

            final var map = new ConcurrentHashMap<DatabaseKey, DynamoItem>();
            return buildParameters(client, map, testDatabase, organisationIds);
        }
    }

    private Stream<Arguments> buildParameters(
            final DynamoDbAsyncClient async,
            final ConcurrentHashMap<DatabaseKey, DynamoItem> map,
            final TestDatabase testDatabase,
            final String[] organisationIds
    ) {
        final Stream.Builder<Arguments> params = Stream.builder();

        if (testDatabase.useProd()) {
            params.add(
                    Arguments.of(
                            getEmbeddedDatabase(organisationIds[0], async, finished),
                            getProductionDatabase(organisationIds[1], async, finished)
                    )
            );
        } else {
            params.add(
                    Arguments.of(
                            getInMemoryDatabase(organisationIds[0], map, finished),
                            getInMemoryDatabase(organisationIds[1], map, finished)
                    )
            ).add(
                    Arguments.of(
                            getEmbeddedDatabase(organisationIds[0], async, finished),
                            getEmbeddedDatabase(organisationIds[1], async, finished)
                    )
            );
        }
        return params.build();
    }

    private void closePreviousRun() throws Exception {
        if (server != null) {
            finished.complete(null);
            server.stop();
        }
    }
}
