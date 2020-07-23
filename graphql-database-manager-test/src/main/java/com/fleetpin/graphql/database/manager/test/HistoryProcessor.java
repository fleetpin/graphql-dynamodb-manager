package com.fleetpin.graphql.database.manager.test;

import static com.fleetpin.graphql.database.manager.test.DynamoDbInitializer.createHistoryTable;
import static com.fleetpin.graphql.database.manager.test.DynamoDbInitializer.createTable;
import static com.fleetpin.graphql.database.manager.test.DynamoDbInitializer.getDatabaseManager;

import java.lang.reflect.Parameter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.dynamo.DynamoDb;
import com.fleetpin.graphql.database.manager.dynamo.HistoryUtil;
import com.fleetpin.graphql.database.manager.test.annotations.DatabaseNames;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

public class HistoryProcessor {

	private String[] tables;
	private DynamoDbAsyncClient client;
	private DynamoDbStreamsAsyncClient streamClient;

	public HistoryProcessor(DynamoDbAsyncClient client, DynamoDbStreamsAsyncClient streamClient, Parameter parameter,
			String organisationId) {
        final var databaseNames = parameter.getAnnotation(DatabaseNames.class);
        this.tables = databaseNames != null ? databaseNames.value() : new String[]{"table"};
        this.client = client;
        this.streamClient = streamClient;
	}
	
	public void process() {
		try {
			for (final String table : tables) {
				
				
				var streamArn = client.describeTable(builder -> builder.tableName(table).build()).get().table()
						.latestStreamArn();

				var shards = streamClient.describeStream(builder -> builder.streamArn(streamArn).build()).get()
						.streamDescription().shards();
		        for (final var shard : shards) {
		        	var shardIterator = streamClient.getShardIterator(builder -> builder.shardIteratorType(ShardIteratorType.TRIM_HORIZON).streamArn(streamArn).shardId(shard.shardId())).get().shardIterator();
		        	var response = streamClient.getRecords(builder -> builder.shardIterator(shardIterator)).get();
		        	var stream = HistoryUtil.toHistoryValue(response.records());
		        	
		        	var items = new HashMap<String, List<WriteRequest>>();
		        	var writeRequests = stream.map(item -> WriteRequest.builder().putRequest(builder -> builder.item(item)).build()).collect(Collectors.toList());
		        	if (writeRequests == null || writeRequests.isEmpty()) {
		        	
		        	} else {
			        	items.put(table+"_history", writeRequests);
			        	System.out.println(items);
			        	client.batchWriteItem(builder -> builder.requestItems(items)).get();
		        	}
		        }				
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
