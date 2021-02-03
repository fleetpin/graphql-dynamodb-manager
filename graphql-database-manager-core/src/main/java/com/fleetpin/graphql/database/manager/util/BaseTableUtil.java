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

package com.fleetpin.graphql.database.manager.util;

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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructList;
import software.amazon.awssdk.core.util.DefaultSdkAutoConstructMap;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.UncheckedIOException;
import java.util.Map;

public class BaseTableUtil {


	public static <T> T convertTo(ObjectMapper mapper, AttributeValue attributeValue, Class<T> type) {
		if(attributeValue == null) {
			return null;
		}
		try {
			return mapper.treeToValue(toJson(mapper, attributeValue), type);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
	}

	public static <T> T convertTo(ObjectMapper mapper, Map<String, AttributeValue> item, Class<T> type) {
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
			if(Math.floor(v) == v && value.n().indexOf('.') == -1 && Long.MAX_VALUE < v && Long.MIN_VALUE > v) {
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

}
