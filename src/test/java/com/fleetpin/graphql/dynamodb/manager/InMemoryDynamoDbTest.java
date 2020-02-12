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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fleetpin.graphql.builder.annotations.Id;
import com.google.common.collect.HashMultimap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

final class InMemoryDynamoDbTest {
    private InMemoryDynamoDb inMemoryDynamoDb;

    @BeforeEach
    void setUp() {
        final var mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).registerModule(new ParameterNamesModule())
                .registerModule(new Jdk8Module())
                .registerModule(new JavaTimeModule())
                .disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS).disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        inMemoryDynamoDb = new InMemoryDynamoDb(mapper, new ConcurrentHashMap<>(), () -> "generated-id");
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

        assertNull(items.get(0));
    }

    @Test
    void shouldBeAbleToFindItemUsingQueryMethod() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-0")).get();

        final var databaseQueryKey = new DatabaseQueryKey("organisation-0", IdExposingTable.class);
        final var items = inMemoryDynamoDb.query(databaseQueryKey).get();

        assertFalse(items.isEmpty());
        assertEquals("id-0", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToFindItemUsingQueryMethodAndGlobalOrganisationId() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("global", new IdExposingTable("id-0")).get();

        final var databaseQueryKey = new DatabaseQueryKey("fooey", IdExposingTable.class);
        final var items = inMemoryDynamoDb.query(databaseQueryKey).get();

        assertFalse(items.isEmpty());
        assertEquals("id-0", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToFindUsingGlobalQuery() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-id", new DynamoDBIndexesTest.SimpleTable("name-0", "lookup-0")).get();

        final var items = inMemoryDynamoDb.queryGlobal(DynamoDBIndexesTest.SimpleTable.class, "lookup-0").get();

        assertFalse(items.isEmpty());
        assertEquals("generated-id", items.get(0).getId());
    }

    @Test
    void shouldBeAbleToFindUsingSecondaryQuery() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new DynamoDBIndexesTest.SimpleTable("name-0", "lookup-0")).get();

        final var items = inMemoryDynamoDb.querySecondary(DynamoDBIndexesTest.SimpleTable.class, "organisation-0", "name-0").get();

        assertFalse(items.isEmpty());
        assertEquals("generated-id", items.get(0).getId());
    }

    @Test
    void shouldLinkItemOntoExistingItem() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-0")).get();
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-1")).get();

        final var targetItem = inMemoryDynamoDb.link("organisation-0", new IdExposingTable("id-0"), IdExposingTable.class, List.of("id-1")).get();

        final var item = inMemoryDynamoDb.get(List.of(new DatabaseKey("organisation-0", IdExposingTable.class, "id-0"))).get();
        assertEquals(item.get(0).getLinks(), targetItem.getLinks());

        assertFalse(targetItem.getLinks().get("idexposingtables").isEmpty());
        assertEquals("id-1", targetItem.getLinks().get("idexposingtables").toArray()[0]);

        final var linkedItem = inMemoryDynamoDb.get(List.of(new DatabaseKey("organisation-0", IdExposingTable.class, "id-1"))).get();
        assertFalse(linkedItem.get(0).getLinks().get("idexposingtables").isEmpty());
        assertEquals("id-0", linkedItem.get(0).getLinks().get("idexposingtables").toArray()[0]);
    }

    @Test
    void shouldClearLinksOfEntity() throws ExecutionException, InterruptedException {
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-0")).get();
        inMemoryDynamoDb.put("organisation-0", new IdExposingTable("id-1")).get();
        inMemoryDynamoDb.link("organisation-0", new IdExposingTable("id-0"), IdExposingTable.class, List.of("id-1")).get();

        final var clearedEntity = inMemoryDynamoDb.deleteLinks("organisation-0", new IdExposingTable("id-0")).get();
        final var item = inMemoryDynamoDb.get(List.of(new DatabaseKey("organisation-0", IdExposingTable.class, "id-0"))).get();

        assertTrue(clearedEntity.getLinks().isEmpty());
        assertTrue(item.get(0).getLinks().isEmpty());
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
