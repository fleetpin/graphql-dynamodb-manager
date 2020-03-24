package com.fleetpin.graphql.database.manager.dynamo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.QueryBuilder;
import com.fleetpin.graphql.database.manager.QueryBuilderFactory;
import com.fleetpin.graphql.database.manager.Table;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

public class DynamoDbQueryBuilderFactory implements QueryBuilderFactory {
    String organisationId;
    DynamoDbAsyncClient client;
    ObjectMapper mapper;

    public DynamoDbQueryBuilderFactory(String organisationId, DynamoDbAsyncClient client, ObjectMapper mapper) {
        this.organisationId = organisationId;
        this.client = client;
        this.mapper = mapper;
    }

    @Override
    public <V extends Table> QueryBuilder<V> getBuilder(Class<V> type) {
        return new DynamoDbQueryBuilder<V>(organisationId, client, mapper).on(type);
    }
}
