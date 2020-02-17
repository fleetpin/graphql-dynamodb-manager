package com.fleetpin.graphql.dynamodb.manager;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public final class TestDatabaseProvider implements ArgumentsProvider {
    private DynamoDBProxyServer server;
    private CompletableFuture<Object> finished;

    @Override
    public Stream<Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
        closePreviousRun();

        final String port = findFreePort();

        final String[] localArgs = {"-inMemory", "-port", port};
        server = ServerRunner.createServerFromCommandLineArgs(localArgs);
        server.start();

        final var async = DynamoDbAsyncClient.builder()
                .endpointOverride(new URI("http://localhost:" + port))
                .build();

        System.setProperty("sqlite4java.library.path", "native-libs");
        DynamoDBInitializer.createTable(async, "prod");
        DynamoDBInitializer.createTable(async, "stage");

        finished = new CompletableFuture<>();

        final var map = new ConcurrentHashMap<DatabaseKey, DynamoItem>();
        final var testMethod = extensionContext.getRequiredTestMethod();
        final var testDatabase = testMethod.getAnnotation(TestDatabase.class);
        final var organisationIds = testDatabase.organisationIds();

        if (testMethod.getParameterCount() == 1) {
            return Stream.of(
                    Arguments.of(DynamoDBInitializer.getInMemoryDatabase(organisationIds[0], new ConcurrentHashMap<>(), finished)),
                    Arguments.of(DynamoDBInitializer.getEmbeddedDatabase(organisationIds[0], async, finished))
            );
        } else {
            if (organisationIds.length < 2) {
                throw new IllegalArgumentException("Not enough organisationIds were provided via @TestDatabase.");
            }

            return buildParameters(async, map, testDatabase, organisationIds);
        }
    }

    private Stream<Arguments> buildParameters(final DynamoDbAsyncClient async, final ConcurrentHashMap<DatabaseKey, DynamoItem> map, final TestDatabase testDatabase, final String[] organisationIds) {
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

    private String findFreePort() throws IOException {
        final var serverSocket = new ServerSocket(0);
        final var port = String.valueOf(serverSocket.getLocalPort());
        serverSocket.close();

        return port;
    }
}
