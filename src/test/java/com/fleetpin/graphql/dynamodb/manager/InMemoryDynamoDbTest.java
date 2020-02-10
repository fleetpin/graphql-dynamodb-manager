package com.fleetpin.graphql.dynamodb.manager;

import com.fleetpin.graphql.builder.annotations.Id;
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
    void shouldShouldAbleToFetchOnlyItemWhenGlobalRegardlessOfGivenKey() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("global", new IdExposingTable("id-0")).get();
        final var databaseKey = new DatabaseKey("dontcare-0", IdExposingTable.class, "id-0");

        final var items = inMemoryDynamoDb.get(List.of(databaseKey)).get();

        assertEquals("id-0", items.get(0).getId());
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
