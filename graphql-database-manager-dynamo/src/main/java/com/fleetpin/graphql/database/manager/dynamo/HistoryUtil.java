package com.fleetpin.graphql.database.manager.dynamo;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;

public class HistoryUtil {

	public static Stream<HashMap<String, AttributeValue>> toHistoryValue(List<Record> records) {
		
		return records.stream().map(record -> {
			var newImage = record.dynamodb().newImage();
			if (newImage != null) {
				var item = new HashMap<>(newImage);
				var id = newImage.get("id").s().split(":",2);
				var idRevision = DynamoDb.toRevisionId(id[1], Long.parseLong(newImage.get("revision").n()));
				
				item.put("id", newImage.get("id"));
				item.put("idRevision", idRevision);
				item.put("organisationId", newImage.get("organisationId"));
				item.put("organisationIdType", AttributeValue.builder().s(newImage.get("organisationId").s()+":"+id[0]).build());
				
				var updatedAtTime = Instant.parse(newImage.get("item").m().get("updatedAt").s()).toEpochMilli();
				var idDate =  DynamoDb.toRevisionId(id[1], updatedAtTime);
				item.put("idDate", idDate);
				item.put("updatedAt", AttributeValue.builder().n(Long.toString(updatedAtTime)).build());
				
				var startsUpdatedAt = DynamoDb.toUpdatedAtId(id[1], Instant.parse(newImage.get("item").m().get("updatedAt").s()).toEpochMilli(), true);
				item.put("startsWithUpdatedAt", startsUpdatedAt);

				return item;

			} else {
				return null;
			}
		}).filter(Objects::nonNull);
		
	}

	
}
