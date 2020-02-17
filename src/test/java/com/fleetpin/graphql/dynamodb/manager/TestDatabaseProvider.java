package com.fleetpin.graphql.dynamodb.manager;

import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

final class TestDatabaseProvider implements ArgumentsProvider {
    private DynamoDBProxyServer server;
    private CompletableFuture<Object> finished;

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
        closePreviousRun();

        final String port = DynamoDBInitializer.findFreePort();

        server = DynamoDBInitializer.startDynamoServer(port);
        final var client = DynamoDBInitializer.startDynamoClient(port);

        System.setProperty("sqlite4java.library.path", "native-libs");
        DynamoDBInitializer.createTable(client, "prod");
        DynamoDBInitializer.createTable(client, "stage");

        finished = new CompletableFuture<>();

        final var testMethod = extensionContext.getRequiredTestMethod();
        final var testDatabase = testMethod.getAnnotation(TestDatabase.class);
        final var organisationIds = testDatabase.organisationIds();

        if (testMethod.getParameterCount() == 1) {
            return Stream.of(
                    Arguments.of(DynamoDBInitializer.getInMemoryDatabase(organisationIds[0], new ConcurrentHashMap<>(), finished)),
                    Arguments.of(DynamoDBInitializer.getEmbeddedDatabase(organisationIds[0], client, finished))
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
                            DynamoDBInitializer.getEmbeddedDatabase(organisationIds[0], async, finished),
                            DynamoDBInitializer.getProductionDatabase(organisationIds[1], async, finished)
                    )
            );
        } else {
            params.add(
                    Arguments.of(
                            DynamoDBInitializer.getInMemoryDatabase(organisationIds[0], map, finished),
                            DynamoDBInitializer.getInMemoryDatabase(organisationIds[1], map, finished)
                    )
            ).add(
                    Arguments.of(
                            DynamoDBInitializer.getEmbeddedDatabase(organisationIds[0], async, finished),
                            DynamoDBInitializer.getEmbeddedDatabase(organisationIds[1], async, finished)
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
