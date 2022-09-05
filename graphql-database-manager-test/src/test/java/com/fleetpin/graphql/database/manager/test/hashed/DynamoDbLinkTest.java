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

package com.fleetpin.graphql.database.manager.test.hashed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.Hash;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

final class DynamoDbLinkTest {

	@TestDatabase(hashed = true)
	void testSimpleQuery(@DatabaseNames({ "prod", "stage" }) final Database db, @DatabaseNames("prod") final Database dbProd)
		throws InterruptedException, ExecutionException {
		var garry = db.put(new SimpleTable("garry")).get();
		var bob = dbProd.put(new AnotherTable("bob")).get();

		var t = assertThrows(CompletionException.class, () -> db.link(garry, bob.getClass(), bob.getId()).join());
		assertEquals(UnsupportedOperationException.class, t.getCause().getClass());
	}

	@Hash(SimplerHasher.class)
	static class SimpleTable extends Table {

		private String name;

		public SimpleTable() {}

		public SimpleTable(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}

	@Hash(SimplerHasher.class)
	static class AnotherTable extends Table {

		private String name;

		public AnotherTable() {}

		public AnotherTable(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}
	}
}
