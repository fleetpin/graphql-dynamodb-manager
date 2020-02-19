package com.fleetpin.graphql.database.manager;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public abstract class AbstractDynamoDbManager {
    protected <T extends Table> AttributeValue entityToAttributes(final ObjectMapper mapper, final T entity) {
        return TableUtil.toAttributes(mapper, entity);
    }

    protected <T> T attributeValueTo(final ObjectMapper mapper, final AttributeValue attributeValue, final Class<T> type) {
        return TableUtil.convertTo(mapper, attributeValue, type);
    }

    protected <T> T attributeValuesTo(final ObjectMapper mapper, final Map<String, AttributeValue> attributeValue, final Class<T> type) {
        return TableUtil.convertTo(mapper, attributeValue, type);
    }
}
