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

package com.fleetpin.graphql.database.manager.test;

import com.fleetpin.graphql.database.manager.Database;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.test.annotations.TestDatabase;
import org.junit.jupiter.api.Assertions;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class DynamoDbQueryBuilderTest {
	static class Ticket extends Table {
		private String value;

		public Ticket(String id, String value) {
			setId(id);
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		@Override
		public String toString() {
			return "Ticket{" +
					"id='" + getId() + '\'' +
					", value='" + value + '\'' +
					'}';
		}
	}

	@TestDatabase
	void testAfter(final Database db) throws InterruptedException, ExecutionException {
		db.put(new Ticket("budgetId1:sales;trinkets:2020/10", "6 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2020/11", "7 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2020/12", "8 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2021/01", "9 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets:2021/02", "10 trinkets")).get();
		db.put(new Ticket("budgetId1:sales;trinkets;whatchamacallits:2020/10", "11 whatchamacallits")).get();
		db.put(new Ticket("budgetId1:sales;trinkets;whatchamacallits:2020/11", "12 whatchamacallits")).get();
		db.put(new Ticket("budgetId1:sales;trinkets;whatchamacallits:2020/12", "13 whatchamacallits")).get();
		db.put(new Ticket("budgetId2:usa;expenses;flights;domestic:2020/10", "1 million dollars")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2020/10", "1 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2020/11", "2 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2020/12", "3 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2021/01", "4 widgets")).get();
		db.put(new Ticket("budgetId1:sales;widgets:2021/02", "5 widgets")).get();

		var result2 = db.query(Ticket.class, builder -> builder.startsWith("budgetId1:").after("budgetId1:sales;trinkets:2020/10").limit(10)).get();
		Assertions.assertEquals("budgetId1:sales;trinkets:2020/11", result2.get(0).getId());
		Assertions.assertEquals(10, result2.size());
	}

	static class BigData extends Table {
		private String name;
		private Double[][] matrix;

		public BigData(String id, String name, Double[][] matrix) {
			setId(id);
			this.name = name;
			this.matrix = matrix;
		}
	}

	private Double[][] createMatrix(Integer size) {
		Double[][] m = new Double[size][size];
		Random r = new Random();
		Double k = r.nextDouble();
		m[0][0] = r.nextDouble();
		for (int i = 0; i < m.length; i++) {
			for (int j = 0; j < m[i].length; j++) {
				if (i == 0 && j == 0) continue;
				else if (j == 0) {
					m[i][j] = m[i - 1][m[i - 1].length - 1] + k;
				}
				else m[i][j] = m[i][j - 1] + k;
			}
		}

		return m;
	}

	static String getId(int i) {
		return String.format("%04d", i);
	}
	
	private void swallow(CompletableFuture<?> f) {
		try {
			f.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException();
		}
	}

	// This test tests querying against large pieces of data which force Dynamoclient to return multiple pages.
	@TestDatabase
	void testBig(final Database db) throws InterruptedException, ExecutionException {
		var n = 1000;
		List<String> ids = Stream.iterate(1, i -> i + 1)
				.map(i -> getId(i))
				.limit(n)
				.collect(Collectors.toList());

		var l = Stream.iterate(1, i -> i + 1)
				.limit(n)
				// Must pick a sufficiently sized matrix in order to force multiple pages to test limit, 100 works well
				.map(i -> new BigData(ids.get(i - 1), "bigdata-" + i.toString(), createMatrix(100)))
				.collect(Collectors.toList());

		l.stream().map(db::put).forEach(this::swallow);

		var result = db.query(BigData.class, builder -> builder.after(getId(456)).limit(100)).get();

		Assertions.assertEquals(100, result.size());
		Assertions.assertEquals("bigdata-457", result.get(0).name);
		Assertions.assertEquals("bigdata-556", result.get(result.size() - 1).name);
	}

	@TestDatabase
	void testBigPlusGlobal(final Database db) throws InterruptedException, ExecutionException {
		var n = 400;
		List<String> ids = Stream.iterate(1, i -> i + 1)
				.map(i -> getId(i))
				.limit(n)
				.collect(Collectors.toList());

		var l = Stream.iterate(1, i -> i + 1)
				.limit(n)
				// Must pick a sufficiently sized matrix in order to force multiple pages to test limit, 100 works well
				.map(i -> new BigData(ids.get(i - 1), "bigdata-" + i.toString(), createMatrix(100)))
				.collect(Collectors.toList());

		l.stream().map(db::put).forEach(this::swallow);
		db.putGlobal(new BigData(getId(999), "big global", createMatrix(100)));

		var result = db.query(BigData.class, builder -> builder.after(getId(200)).limit(100)).get();

		Assertions.assertEquals(100, result.size());
		Assertions.assertEquals("bigdata-201", result.get(0).name);
		Assertions.assertEquals("bigdata-300", result.get(result.size() - 1).name);
		Assertions.assertFalse(result.stream().anyMatch(x -> x.name == "big global"));
	}
}
