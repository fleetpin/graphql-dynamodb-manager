package com.fleetpin.graphql.database.manager.test.hashed;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fleetpin.graphql.database.manager.DataWriter;
import com.fleetpin.graphql.database.manager.DatabaseDriver;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

final class DynamoDbDataWriterTest {

	@TestDatabase(hashed = true)
	void testDispatchSize() {
		DatabaseDriver my = Mockito.mock(DatabaseDriver.class, Mockito.CALLS_REAL_METHODS);
		DynamoDbIndexesTest.SimpleTable entry1 = new DynamoDbIndexesTest.SimpleTable("garry", "john");
		var dataWriter = new DataWriter(my::bulkPut);
		dataWriter.put("test", entry1, true);
		Assertions.assertEquals(1, dataWriter.dispatchSize());
	}

	@TestDatabase(hashed = true)
	void testDispatch() {
		DatabaseDriver my = Mockito.mock(DatabaseDriver.class, Mockito.CALLS_REAL_METHODS);
		DynamoDbIndexesTest.SimpleTable entry1 = new DynamoDbIndexesTest.SimpleTable("garry", "john");
		var dataWriter = new DataWriter(my::bulkPut);
		dataWriter.put("test", entry1, true);
		dataWriter.dispatch();
		verify(my, times(1)).bulkPut(Mockito.anyList());
	}
}
