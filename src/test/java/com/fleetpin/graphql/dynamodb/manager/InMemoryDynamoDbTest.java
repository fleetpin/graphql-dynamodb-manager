package com.fleetpin.graphql.dynamodb.manager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class InMemoryDynamoDbTest {
    private InMemoryDynamoDb inMemoryDynamoDb;

    @BeforeEach
    void setUp() {
        inMemoryDynamoDb = new InMemoryDynamoDb();
    }

    @Test
    void shouldBeAbleToGetOnlyEntryWithMatchingDatabaseKey() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new SimpleTable("table-0"));
        final var databaseKey = new DatabaseKey("organisation-0", SimpleTable.class, "id-0");

        final var items = inMemoryDynamoDb.get(List.of(databaseKey)).get();

        assertEquals("id-0", items.get(0).getId());
    }
}
