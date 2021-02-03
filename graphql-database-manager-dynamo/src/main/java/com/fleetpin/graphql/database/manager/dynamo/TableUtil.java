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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.GlobalIndex;
import com.fleetpin.graphql.database.manager.annotations.SecondaryIndex;
import com.fleetpin.graphql.database.manager.util.BaseTableUtil;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class TableUtil extends BaseTableUtil {

	static String getSecondaryGlobal(Table entity) {
		for(var method: entity.getClass().getMethods()) {
			if(method.isAnnotationPresent(GlobalIndex.class)) {
				try {
					var secondary = method.invoke(entity);
					if(secondary instanceof Optional) {
						secondary = ((Optional) secondary).orElse(null);
					}
					return (String) secondary;
				} catch (ReflectiveOperationException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return null;
	}

	static String getSecondaryOrganisation(Table entity) {
		for(var method: entity.getClass().getMethods()) {
			if(method.isAnnotationPresent(SecondaryIndex.class)) {
				try {
					var secondary = method.invoke(entity);
					if(secondary instanceof Optional) {
						secondary = ((Optional) secondary).orElse(null);
					}
					return (String) secondary;
				} catch (ReflectiveOperationException e) {
					throw new RuntimeException(e);
				}
			}
		}
		return null;
	}

	static Map<String, AttributeValue> toAttributes(ObjectMapper mapper, Object entity) {
		Map<String, AttributeValue> entries = new HashMap<>();
		ObjectNode tree = mapper.valueToTree(entity);

		Iterator<Entry<String, JsonNode>> fields = tree.fields();
		fields.forEachRemaining(entry -> {
			Entry<String, JsonNode> field = entry;
			AttributeValue attribute;
			if(field.getKey().equals("links")){
				attribute = toSpecialAttribute(field.getValue());
			}else {
				attribute = toAttribute(field.getValue());
			}
			if(attribute != null) {
				entries.put(field.getKey(), attribute);
			}
		});
		return entries;
		
	}

	private static AttributeValue toSpecialAttribute(JsonNode value) {

		ObjectNode tree = (ObjectNode) value;
		Map<String, AttributeValue> entries = new HashMap<>();
		Iterator<Entry<String, JsonNode>> fields = tree.fields();
		fields.forEachRemaining(entry -> {
			//As links, assuming structure a bit here
			Entry<String, JsonNode> field = entry;
			entries.put(field.getKey(), createSS(field.getValue()));
		});
		return AttributeValue.builder().m(entries).build();
	}

	private static AttributeValue createSS(JsonNode value) {

		List<String> array = stream(value).map(x -> x.asText()).collect(Collectors.toList());
		return AttributeValue.builder().ss(array).build();
	}


	static AttributeValue toAttribute(JsonNode value) {
		switch(value.getNodeType()) {
		case NUMBER:
			return AttributeValue.builder().n(value.asText()).build();
		case BINARY:
			try {
				return AttributeValue.builder().b(SdkBytes.fromByteArray(value.binaryValue())).build();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		case BOOLEAN:
			return AttributeValue.builder().bool(value.asBoolean()).build();
		case STRING: 
			String v = value.asText();
			if(v.isEmpty()) {
				return null;
			}
			return AttributeValue.builder().s(v).build();
		case ARRAY: 
			return processArray(value);
		case OBJECT:
			ObjectNode tree = (ObjectNode) value;
			Map<String, AttributeValue> entries = new HashMap<>();
			Iterator<Entry<String, JsonNode>> fields = tree.fields();
			fields.forEachRemaining(entry -> {
				Entry<String, JsonNode> field = entry;
				entries.put(field.getKey(), toAttribute(field.getValue()));
			});
			return AttributeValue.builder().m(entries).build();
		case NULL:
			return AttributeValue.builder().nul(true).build();
		default:
			throw new RuntimeException("unknown type " + value.getNodeType());
		}
	}

	private static AttributeValue processArray(JsonNode value) {
		return basicArray(value);
	}



	private static AttributeValue basicArray(JsonNode value) {
		List<AttributeValue> array = stream(value).map(TableUtil::toAttribute).collect(Collectors.toList());
		return AttributeValue.builder().l(array).build();
	}

	private static Stream<JsonNode> stream(JsonNode array) {
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(array.iterator(), 0), false);
	}



	static <T> CompletableFuture<List<T>> all(List<CompletableFuture<T>> collect) {
		return CompletableFuture.allOf(collect.toArray(CompletableFuture[]::new))
				.thenApply(__ -> collect.stream().map(m -> {
					try {
						return m.get();
					} catch (InterruptedException | ExecutionException e) {
						throw new RuntimeException(e);
					}
				}).collect(Collectors.toList()));
	}
}
