package com.fleetpin.graphql.database.dynamo.history.lambda;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Identity;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;

public class DynamoUtil {

	public static Record toV2(DynamodbStreamRecord record) {

		var builder = Record.builder();
		builder.awsRegion(record.getAwsRegion());
		builder.dynamodb(toV2(record.getDynamodb()));
		builder.eventID(record.getEventID());
		builder.eventName(record.getEventName());
		builder.eventSource(record.getEventSource());
		builder.eventVersion(record.getEventVersion());
		builder.userIdentity(toV2(record.getUserIdentity()));
		return builder.build();
	}

	public static Identity toV2(com.amazonaws.services.dynamodbv2.model.Identity userIdentity) {
		var builder = Identity.builder();
		builder.principalId(userIdentity.getPrincipalId());
		builder.type(userIdentity.getType());
		return builder.build();
	}

	public static StreamRecord toV2(com.amazonaws.services.dynamodbv2.model.StreamRecord dynamodb) {
		var builder = StreamRecord.builder();

		builder.approximateCreationDateTime(dynamodb.getApproximateCreationDateTime().toInstant());
		builder.keys(toV2(dynamodb.getKeys()));
		builder.newImage(toV2(dynamodb.getNewImage()));
		builder.oldImage(toV2(dynamodb.getOldImage()));
		builder.sequenceNumber(dynamodb.getSequenceNumber());
		builder.sizeBytes(dynamodb.getSizeBytes());
		builder.streamViewType(dynamodb.getStreamViewType());

		return builder.build();
	}

	public static Map<String, AttributeValue> toV2(
			Map<String, com.amazonaws.services.dynamodbv2.model.AttributeValue> attribute) {
		if (attribute == null) {
			return null;
		}
		Map<String, AttributeValue> toReturn = new HashMap<>();
		attribute.forEach((key, value) -> toReturn.put(key, toV2(value)));
		return toReturn;
	}

	public static AttributeValue toV2(com.amazonaws.services.dynamodbv2.model.AttributeValue value) {
		if (value.getB() != null) {
			return AttributeValue.builder().b(SdkBytes.fromByteBuffer(value.getB())).build();
		}
		if (value.getBOOL() != null) {
			return AttributeValue.builder().bool(value.getBOOL()).build();
		}
		if (value.getNULL() != null) {
			return AttributeValue.builder().nul(value.getNULL()).build();
		}
		if (value.getBS() != null) {
			return AttributeValue.builder()
					.bs(value.getBS().stream().map(SdkBytes::fromByteBuffer).collect(Collectors.toList())).build();
		}
		if (value.getL() != null) {
			return AttributeValue.builder().l(value.getL().stream().map(DynamoUtil::toV2).collect(Collectors.toList()))
					.build();
		}
		if (value.getM() != null) {
			return AttributeValue.builder().m(toV2(value.getM())).build();
		}
		if (value.getN() != null) {
			return AttributeValue.builder().n(value.getN()).build();
		}
		if (value.getNS() != null) {
			return AttributeValue.builder().ns(value.getNS()).build();
		}
		if (value.getS() != null) {
			return AttributeValue.builder().s(value.getS()).build();
		}
		if (value.getSS() != null) {
			return AttributeValue.builder().ss(value.getSS()).build();
		}
		throw new RuntimeException("Unknown type " + value);
	}

}