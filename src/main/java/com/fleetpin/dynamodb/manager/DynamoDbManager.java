package com.fleetpin.dynamodb.manager;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.google.common.base.Preconditions;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class DynamoDbManager {

	
	private final ObjectMapper mapper;
	private final DynamoDb dynamoDb;
	
	
	private DynamoDbManager(ObjectMapper mapper, DynamoDb dynamoDb) {
		super();
		this.mapper = mapper;
		this.dynamoDb = dynamoDb;
	}
	
	public Database getDatabase(String organisationId) {
		return new Database(mapper, organisationId, dynamoDb);
	}
	
	
	public static DyanmoDbManagerBuilder builder() {
		return new DyanmoDbManagerBuilder();
	}
	
	public static class DyanmoDbManagerBuilder {
		private DynamoDbAsyncClient client;
		private ObjectMapper mapper;
		private List<String> tables;
		private Supplier<String> idGenerator;
		
		
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

		public DyanmoDbManagerBuilder idGenerator(Supplier<String> idGenerator) {
			this.idGenerator = idGenerator;
			return this;
		}
		
		public DynamoDbManager build() {
			Preconditions.checkNotNull(tables, "Tables must be set");
			Preconditions.checkArgument(!tables.isEmpty(), "Empty table array");

			
			if(mapper == null) {
				mapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).registerModule(new ParameterNamesModule())
						   .registerModule(new Jdk8Module())
						   .registerModule(new JavaTimeModule())
						   .disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS).disable(SerializationFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
						   .setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
			}
			if(client == null) {
				client = DynamoDbAsyncClient.create();
			}
			if(idGenerator == null) {
				idGenerator = () -> UUID.randomUUID().toString();
			}
			
			return new DynamoDbManager(mapper, new DynamoDbImpl(mapper, tables, client, idGenerator));
			
		}
		
	}

	public ObjectMapper getMapper() {
		return mapper;
	}
	

	
}
