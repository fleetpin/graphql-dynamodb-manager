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

package com.fleetpin.graphql.database.manager.dynamo;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.fleetpin.graphql.database.manager.DatabaseDriver;
import com.fleetpin.graphql.database.manager.DatabaseManager;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class DynamoDbManager extends DatabaseManager {

	private final ObjectMapper mapper;
	private final Supplier<String> idGenerator;
	private final DynamoDbAsyncClient client;

	private DynamoDbManager(ObjectMapper mapper, Supplier<String> idGenerator, DynamoDbAsyncClient client, DatabaseDriver dynamoDb) {
		super(dynamoDb);
		this.mapper = mapper;
		this.idGenerator = idGenerator;
		this.client = client;
	}

	public static DyanmoDbManagerBuilder builder() {
		return new DyanmoDbManagerBuilder();
	}

	public static class DyanmoDbManagerBuilder {

		private DynamoDbAsyncClient client;
		private ObjectMapper mapper;
		private List<String> tables;
		private Supplier<String> idGenerator;
		private DatabaseDriver database;
		private String historyTable;
		private int batchWriteSize = 25;
		private int maxRetry = 10;
		private boolean globalEnabled = true;
		private boolean hash = false;

		public DyanmoDbManagerBuilder dynamoDbAsyncClient(DynamoDbAsyncClient client) {
			this.client = client;
			return this;
		}

		public DyanmoDbManagerBuilder objectMapper(ObjectMapper mapper) {
			this.mapper = mapper;
			return this;
		}

		public DyanmoDbManagerBuilder tables(List<String> tables) {
			this.tables = tables;
			return this;
		}

		public DyanmoDbManagerBuilder tables(String... tables) {
			this.tables = Arrays.asList(tables);
			return this;
		}

		public DyanmoDbManagerBuilder historyTable(String historyTable) {
			this.historyTable = historyTable;
			return this;
		}

		public DyanmoDbManagerBuilder idGenerator(Supplier<String> idGenerator) {
			this.idGenerator = idGenerator;
			return this;
		}

		public DyanmoDbManagerBuilder dynamoDb(final DatabaseDriver database) {
			this.database = database;
			return this;
		}

		public DyanmoDbManagerBuilder batchWriteSize(int batchWriteSize) {
			if (batchWriteSize < 0 || batchWriteSize > 25) {
				throw new RuntimeException("Batch write size must be between 0-25");
			}
			this.batchWriteSize = batchWriteSize;
			return this;
		}

		public DyanmoDbManagerBuilder maxRetry(int maxRetry) {
			this.maxRetry = maxRetry;
			return this;
		}

		public DyanmoDbManagerBuilder global(boolean global) {
			this.globalEnabled = global;
			return this;
		}

		public DyanmoDbManagerBuilder hash(boolean hash) {
			this.hash = hash;
			return this;
		}

		public DynamoDbManager build() {
			Preconditions.checkNotNull(tables, "Tables must be set");
			Preconditions.checkArgument(!tables.isEmpty(), "Empty table array");

			if (mapper == null) {
				mapper =
					new ObjectMapper()
						.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
						.registerModule(new ParameterNamesModule())
						.registerModule(new Jdk8Module())
						.registerModule(new JavaTimeModule())
						.disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
						.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
						.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
						.disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
						.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
			}
			if (client == null) {
				client = DynamoDbAsyncClient.create();
			}
			if (idGenerator == null) {
				idGenerator = () -> UUID.randomUUID().toString();
			}

			database =
				Objects.requireNonNullElse(
					database,
					new DynamoDb(mapper, tables, historyTable, client, idGenerator, batchWriteSize, maxRetry, globalEnabled, hash)
				);

			return new DynamoDbManager(mapper, idGenerator, client, database);
		}
	}

	public ObjectMapper getMapper() {
		return mapper;
	}

	public String newId() {
		return idGenerator.get();
	}

	public <T> T convertTo(Map<String, AttributeValue> item, Class<T> type) {
		return TableUtil.convertTo(mapper, item, type);
	}

	public <T> T convertTo(AttributeValue item, Class<T> type) {
		return TableUtil.convertTo(mapper, item, type);
	}

	public AttributeValue toAttributes(Object entity) {
		var entries = TableUtil.toAttributes(mapper, entity);
		return AttributeValue.builder().m(entries).build();
	}

	public DynamoDbAsyncClient getDynamoDbAsyncClient() {
		return client;
	}
}
