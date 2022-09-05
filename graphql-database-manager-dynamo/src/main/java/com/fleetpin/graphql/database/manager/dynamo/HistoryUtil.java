package com.fleetpin.graphql.database.manager.dynamo;

import com.google.common.primitives.UnsignedBytes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Objects;
import java.util.stream.Stream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;

public class HistoryUtil {

	public static Stream<HashMap<String, AttributeValue>> toHistoryValue(Stream<Record> records) {
		return records
			.map(record -> record.dynamodb().newImage())
			.filter(Objects::nonNull)
			.filter(newImage -> {
				var hasHistory = newImage.get("history");
				return (hasHistory != null && hasHistory.bool() == Boolean.TRUE);
			})
			.map(newImage -> {
				var item = new HashMap<>(newImage);
				var organisationId = newImage.get("organisationId").s();
				var idWithType = newImage.get("id").s();
				var hashed = item.get("hashed");
				if (hashed != null && hashed.bool()) {
					var split = organisationId.indexOf(':');
					var typeIndex = organisationId.indexOf(':', split + 1);
					idWithType = organisationId.substring(split + 1, typeIndex) + ":" + newImage.get("item").m().get("id").s();
					organisationId = organisationId.substring(0, split);
				}

				var id = idWithType.split(":", 2);
				var idRevision = toRevisionId(id[1], Long.parseLong(newImage.get("revision").n()));

				item.put("id", AttributeValue.builder().s(idWithType).build());
				item.remove("hashed");
				item.put("idRevision", idRevision);
				item.put("organisationId", AttributeValue.builder().s(organisationId).build());
				item.put("organisationIdType", AttributeValue.builder().s(organisationId + ":" + id[0]).build());

				var updatedAtTime = Instant.parse(newImage.get("item").m().get("updatedAt").s()).toEpochMilli();
				var idDate = toRevisionId(id[1], updatedAtTime);
				item.put("idDate", idDate);
				item.put("updatedAt", AttributeValue.builder().n(Long.toString(updatedAtTime)).build());

				var startsUpdatedAt = toUpdatedAtId(id[1], Instant.parse(newImage.get("item").m().get("updatedAt").s()).toEpochMilli(), true);
				item.put("startsWithUpdatedAt", startsUpdatedAt);

				return item;
			});
	}

	public static AttributeValue toId(String id) {
		var idBytes = (id + ":").getBytes(StandardCharsets.UTF_8);
		var idBuffer = ByteBuffer.allocate(idBytes.length);
		idBuffer.put(idBytes);
		idBuffer.flip();

		var idAttribute = AttributeValue.builder().b(SdkBytes.fromByteBuffer(idBuffer)).build();
		return idAttribute;
	}

	public static AttributeValue toRevisionId(String id, Long revision) {
		var idBytes = (id + ":").getBytes(StandardCharsets.UTF_8);
		var revisionId = ByteBuffer.allocate(idBytes.length + Long.BYTES);
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
