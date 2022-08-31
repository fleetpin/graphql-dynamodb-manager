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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.util.TableCoreUtil;
import java.util.*;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class Flattener {

	private final Map<String, DynamoItem> lookup;
	private final boolean includeOrganisationId;

	Flattener(boolean includeOrganisationId) {
		lookup = new HashMap<>();
		this.includeOrganisationId = includeOrganisationId;
	}

	public void add(String table, List<Map<String, AttributeValue>> list) {
		list.forEach(item -> {
			var i = new DynamoItem(table, item);
			if (i.isDeleted()) {
				lookup.remove(getId(i));
			} else {
				lookup.merge(getId(i), i, this::merge);
			}
		});
	}

	private String getId(DynamoItem item) {
		if (includeOrganisationId) {
			return item.getOrganisationId() + ":" + item.getId();
		} else {
			return item.getId();
		}
	}

	public DynamoItem get(Class<? extends Table> type, String id) {
		return lookup.get(TableCoreUtil.table(type) + ":" + id);
	}

	public void addItems(List<DynamoItem> list) {
		list.forEach(item -> {
			if (item.isDeleted()) {
				lookup.remove(getId(item));
			} else {
				lookup.merge(getId(item), item, this::merge);
			}
		});
	}

	public DynamoItem merge(DynamoItem existing, DynamoItem replace) {
		var item = new HashMap<>(replace.getItem());
		//only links in parent
		if (item.get("item") == null) {
			item.put("item", existing.getItem().get("item"));
		}
		var toReturn = new DynamoItem(replace.getTable(), item);
		toReturn.getLinks().putAll(existing.getLinks());

		return toReturn;
	}

	public <T extends Table> List<T> results(ObjectMapper mapper, Class<T> type) {
		return results(mapper, type, Optional.empty());
	}

	public <T extends Table> List<T> results(ObjectMapper mapper, Class<T> type, Optional<Integer> limit) {
		var items = new ArrayList<DynamoItem>(lookup.values());
		Collections.sort(items);
		return items.stream().limit(limit.orElse(Integer.MAX_VALUE)).map(t -> t.convertTo(mapper, type)).collect(Collectors.toList());
	}
}
