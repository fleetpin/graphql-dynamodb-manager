package com.fleetpin.graphql.database.manager.dynamo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.QueryBuilder;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.util.TableCoreUtil;
import org.dataloader.DataLoader;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


public class DynamoDbQueryBuilder<V extends Table> implements QueryBuilder<V> {
    private DynamoDbAsyncClient dynamoClient;
    private ObjectMapper mapper;
    private Class<V> type;
    private ArrayList<String> conditions = new ArrayList<String>();
    private Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    private String table;

    private ArrayList<Function<QueryRequest.Builder, QueryRequest.Builder>> transformations = new ArrayList<>();

    private AttributeValue attr(String s) {
        return AttributeValue.builder().s(s).build();
    }

    public DynamoDbQueryBuilder(String organisationId, DynamoDbAsyncClient client, ObjectMapper mapper) {
        this.dynamoClient = client;
        this.mapper = mapper;

        this.conditions.add("organisationId = :organisationId");
        this.expressionAttributeValues.put(":organisationId", attr(organisationId));
    }

    public QueryBuilder<V> on(Class<V> type) {
        if (this.type != null && this.type != type) {
            throw new RuntimeException("Resetting the query type is not supported. Already set to " + type.getSimpleName() + " (you already called on or the query type was provided another way.)");
        }
        this.type = type;
        this.table = TableCoreUtil.table(type);
        return this;
    }

    public QueryBuilder<V> startsWith(String prefix) {
        if (this.table == null) {
            throw new RuntimeException("table can not be null, did you forget to call .on(Table::class)?");
        }

        this.conditions.add("begins_with(id, :startsWith)");
        this.expressionAttributeValues.put(":startsWith", attr(this.table + prefix));

        return this;
    }

    public QueryBuilder<V> limit(Integer i) {
        transformations.add((builder) -> builder.limit(i));
        return this;
    }

    public QueryBuilder<V> from(String s) {
        transformations.add(builder -> builder.exclusiveStartKey(Map.of("id", attr(s))));
        return this;
    }

    public QueryBuilder<V> until(String s) {
        throw new RuntimeException("unimplemented");
    }

    public CompletableFuture<List<V>> exec() {
        var toReturn = new ArrayList<V>();
        return this.dynamoClient.queryPaginator(requestBuilder -> {
            var builder = requestBuilder.tableName(this.table);
            for (var transform : transformations) {
                builder = transform.apply(builder);
            }
            builder.expressionAttributeValues(expressionAttributeValues)
                    .keyConditionExpression(String.join(" AND ", conditions));
        })
                .flatMapIterable(response -> response.items())
                .map(item -> TableUtil.convertTo(this.mapper, item, this.type))
                .subscribe(item -> toReturn.add(item))
                .thenApply(__ -> toReturn);
    }
}


