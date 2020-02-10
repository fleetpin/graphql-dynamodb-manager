package com.fleetpin.graphql.dynamodb.manager;

import com.fleetpin.graphql.builder.annotations.Id;
import com.google.common.collect.HashMultimap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class InMemoryDynamoDbTest {
    private InMemoryDynamoDb inMemoryDynamoDb;

    @BeforeEach
    void setUp() {
        inMemoryDynamoDb = new InMemoryDynamoDb(() -> "generated-id");
    }

    @Test
    void shouldBeAbleToPutWithNullIdAndGenerateUsingGenerator() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new DynamoDBQueryTest.SimpleTable("name")).get();

        final var databaseKeys = new DatabaseKey("organisation-0", DynamoDBQueryTest.SimpleTable.class, "generated-id");
        final var items = inMemoryDynamoDb.get(List.of(databaseKeys)).get();

        assertEquals("generated-id", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToGetOnlyEntryWithMatchingDatabaseKey() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-0")).get();
        final var databaseKey = new DatabaseKey("organisation-0", IdExposingTable.class, "id-0");

        final var items = inMemoryDynamoDb.get(List.of(databaseKey)).get();

        assertEquals("id-0", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToFilterItemsWithNonMatchingDatabaseKeys() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-0")).get();
        inMemoryDynamoDb.put("organisation-1", new IdExposingTable("id-1")).get();
        inMemoryDynamoDb.put("organisation-2", new IdExposingTable("id-2")).get();
        final var databaseKey = new DatabaseKey("organisation-0", IdExposingTable.class, "id-0");

        final var items = inMemoryDynamoDb.get(List.of(databaseKey)).get();

        assertEquals(1, items.size());
        assertEquals("id-0", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToFetchOnlyItemWhenGlobalRegardlessOfGivenKey() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("global", new IdExposingTable("id-0")).get();
        final var databaseKey = new DatabaseKey("dontcare-0", IdExposingTable.class, "id-0");

        final var items = inMemoryDynamoDb.get(List.of(databaseKey)).get();

        assertEquals("id-0", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToDeleteItemWithMatchingOrganisationIdAndId() throws ExecutionException, InterruptedException {
        final var table = new IdExposingTable("id-0");
        table.setSource("table-0", HashMultimap.create(), "organisation-0");

        inMemoryDynamoDb.put("organisation-0", table).get();
        inMemoryDynamoDb.delete("organisation-0", table).get();
        final var databaseKey = new DatabaseKey("organisation-0", IdExposingTable.class, "id-0");

        final var items = inMemoryDynamoDb.get(List.of(databaseKey)).get();

        assertTrue(items.isEmpty());
    }

    static final class IdExposingTable extends Table {
        private final String id;

        public IdExposingTable(final String id) {
            this.id = id;
        }

        @Id
        @Override
        public String getId() {
            return id;
        }
    }
}
