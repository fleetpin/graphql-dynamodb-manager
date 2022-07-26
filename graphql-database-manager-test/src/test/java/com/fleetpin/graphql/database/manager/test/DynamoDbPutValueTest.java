package com.fleetpin.graphql.database.manager.test;

import com.fleetpin.graphql.database.manager.PutValue;
import com.fleetpin.graphql.database.manager.RevisionMismatchException;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.CompletableFuture;

public class DynamoDbPutValueTest {

    @TestDatabase
    void testSuccess() {
        DynamoDbIndexesTest.SimpleTable entry1 = new DynamoDbIndexesTest.SimpleTable("garry", "john");

        var completableFuture = new CompletableFuture<DynamoDbIndexesTest.SimpleTable>();
        var putValue = new PutValue<DynamoDbIndexesTest.SimpleTable>("test", entry1, false, completableFuture);

        Assertions.assertEquals(false, putValue.getFuture().isDone());
        Assertions.assertEquals(0, putValue.getEntity().getRevision());

        putValue.resolve();

        Assertions.assertEquals(true, putValue.getFuture().isDone());
        Assertions.assertEquals(1, putValue.getEntity().getRevision());
    }


    @TestDatabase
    void testFailure() {
        DynamoDbIndexesTest.SimpleTable entry1 = new DynamoDbIndexesTest.SimpleTable("garry", "john");

        var completableFuture = new CompletableFuture<DynamoDbIndexesTest.SimpleTable>();
        var putValue = new PutValue<DynamoDbIndexesTest.SimpleTable>("test", entry1, false, completableFuture);

        Assertions.assertEquals(false, putValue.getFuture().isDone());
        Assertions.assertEquals(0, putValue.getEntity().getRevision());

        putValue.fail(new RevisionMismatchException(new Exception("hello")));

        Assertions.assertEquals(true, putValue.getFuture().isDone());
        Assertions.assertEquals(true, putValue.getFuture().isCompletedExceptionally());
        Assertions.assertEquals(0, putValue.getEntity().getRevision());
    }
}
