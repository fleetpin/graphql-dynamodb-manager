package com.fleetpin.graphql.database.manager.dynamo;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.primitives.UnsignedBytes;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;

public class HistoryUtil {

	public static Stream<HashMap<String, AttributeValue>> toHistoryValue(List<Record> records) {
		
		return records.stream().map(record -> {
			var newImage = record.dynamodb().newImage();
			if (newImage != null) {
				var item = new HashMap<>(newImage);
				var id = newImage.get("id").s().split(":",2);
				var idRevision = toRevisionId(id[1], Long.parseLong(newImage.get("revision").n()));
				
				item.put("id", newImage.get("id"));
				item.put("idRevision", idRevision);
				item.put("organisationId", newImage.get("organisationId"));
				item.put("organisationIdType", AttributeValue.builder().s(newImage.get("organisationId").s()+":"+id[0]).build());
				
				var updatedAtTime = Instant.parse(newImage.get("item").m().get("updatedAt").s()).toEpochMilli();
				var idDate =  toRevisionId(id[1], updatedAtTime);
				item.put("idDate", idDate);
				item.put("updatedAt", AttributeValue.builder().n(Long.toString(updatedAtTime)).build());
				
				var startsUpdatedAt = toUpdatedAtId(id[1], Instant.parse(newImage.get("item").m().get("updatedAt").s()).toEpochMilli(), true);
				item.put("startsWithUpdatedAt", startsUpdatedAt);

				return item;

			} else {
				return null;
			}
		}).filter(Objects::nonNull);
		
	}

	public static AttributeValue toId(String id) {
		var idBytes = (id+":").getBytes(StandardCharsets.UTF_8);
		var idBuffer = ByteBuffer.allocate(idBytes.length);
		idBuffer.put(idBytes);
		idBuffer.flip();
		
		var idAttribute = AttributeValue.builder().b(SdkBytes.fromByteBuffer(idBuffer)).build();
		return idAttribute;
	}
    
	public static AttributeValue toRevisionId(String id, Long revision) {
		var idBytes = (id+":").getBytes(StandardCharsets.UTF_8);
		var revisionId = ByteBuffer.allocate(idBytes.length+Long.BYTES);
		revisionId.put(idBytes);
		revisionId.putLong(revision);
		revisionId.flip();
		
		var revisionIdAttribute = AttributeValue.builder().b(SdkBytes.fromByteBuffer(revisionId)).build();
		return revisionIdAttribute;
	}
	
	public static AttributeValue toUpdatedAtId(String starts, Long updatedAt, Boolean from) {
		var idBytes = ByteBuffer.wrap(starts.getBytes(StandardCharsets.UTF_8));
		var date = ByteBuffer.allocate(Long.BYTES);	
		date.putLong(updatedAt);
		date.flip();
		
		var updatedAtId = ByteBuffer.allocate(Math.max(idBytes.capacity(), date.capacity()) + date.capacity()); 
		
		if (idBytes.capacity() < date.capacity()) {
			for (int i = 0; i < date.capacity(); i++) {
				if (i < idBytes.capacity()) {
					updatedAtId.put(idBytes.get());
					updatedAtId.put(date.get());
				} else {
					if (from) {
						updatedAtId.put((byte) 0);
					} else {
						updatedAtId.put(UnsignedBytes.MAX_VALUE);
					}
					updatedAtId.put(date.get());
				}
			}
		} else {
			for (int i = 0; i < date.capacity(); i++) {
				updatedAtId.put(idBytes.get());
				updatedAtId.put(date.get());
			}
			for (int i = date.capacity(); i < idBytes.capacity(); i++) {
				updatedAtId.put(idBytes.get());
			}
		}
		
		updatedAtId.flip();
		var updatedAtIdAttribute = AttributeValue.builder().b(SdkBytes.fromByteBuffer(updatedAtId)).build();
		return updatedAtIdAttribute;
	}

}
