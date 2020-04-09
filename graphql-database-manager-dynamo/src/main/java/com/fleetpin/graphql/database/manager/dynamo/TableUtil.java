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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.GlobalIndex;
import com.fleetpin.graphql.database.manager.annotations.SecondaryIndex;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class TableUtil {

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
			AttributeValue attribute = toAttribute(field.getValue());
			if(attribute != null) {
				entries.put(field.getKey(), attribute);
			}
		});
		return entries;
		
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

	static <T> T convertTo(ObjectMapper mapper, AttributeValue attributeValue, Class<T> type) {
		if(attributeValue == null) {
			return null;
		}
		try {
			return mapper.treeToValue(toJson(mapper, attributeValue), type);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}

	static <T> T convertTo(ObjectMapper mapper, Map<String, AttributeValue> item, Class<T> type) {
		try {
			ObjectNode objNode = mapper.createObjectNode();
			item.forEach((key, v) -> {
				objNode.set(key, toJson(mapper, v));
			});
			return mapper.treeToValue(objNode, type);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static JsonNode toJson(ObjectMapper mapper, AttributeValue value) {
		if(value.bool() != null) {
			return BooleanNode.valueOf(value.bool());
		}
		if(value.nul() != null && value.nul()) {
			return NullNode.instance;
		}
		if(value.b() != null) {
			return BinaryNode.valueOf(value.b().asByteArray());
		}
		if(value.n() != null) {
			double v = Double.parseDouble(value.n());
			if(Math.floor(v) == v) {
				return LongNode.valueOf(Long.parseLong(value.n()));
			}
			return DoubleNode.valueOf(v);
		}
		if(value.s() != null) {
			return TextNode.valueOf(value.s());
		}

		Object defArray =  DefaultSdkAutoConstructList.getInstance();
		Object defMap =  DefaultSdkAutoConstructMap.getInstance();
		if(value.bs() != defArray) {
			ArrayNode arrayNode = mapper.createArrayNode();
			for(SdkBytes b: value.bs()) {
				arrayNode.add(BinaryNode.valueOf(b.asByteArray()));
			}
			return arrayNode;
		}
		if(value.l() != defArray) {
			ArrayNode arrayNode = mapper.createArrayNode();
			for(AttributeValue l: value.l()) {
				arrayNode.add(toJson(mapper, l));	 
			}
			return arrayNode;
		}

		if(value.ns() != defArray) {
			ArrayNode arrayNode = mapper.createArrayNode();
			for(String s: value.ns()) {
				arrayNode.add(TextNode.valueOf(s));
			}
			return arrayNode;
		}
		if(value.ss() != defArray) {
			ArrayNode arrayNode = mapper.createArrayNode();
			for(String s: value.ss()) {
				arrayNode.add(TextNode.valueOf(s));
			}
			return arrayNode;
		}
		if(value.m() != defMap) {
			ObjectNode objNode = mapper.createObjectNode();
			if(value.m().isEmpty()) {
				return NullNode.instance;
			}
			value.m().forEach((key, v) -> {
				objNode.set(key, toJson(mapper, v));
			});
			return objNode;
		}
		throw new RuntimeException("Unsupported type " + value);
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
