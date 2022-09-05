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
import com.fleetpin.graphql.database.manager.TableAccess;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoItem implements Comparable<DynamoItem> {

	private final String table;
	private final Map<String, AttributeValue> item;
	private final String id;

	private final HashMultimap<String, String> links;
	private String organisationId;

	DynamoItem(String table, Map<String, AttributeValue> item) {
		this.table = table;
		this.item = item;

		this.links = HashMultimap.create();

		var links = item.get("links");
		if (links != null) {
			links
				.m()
				.forEach((t, value) -> {
					this.links.putAll(t, value.ss());
				});
		}
		var id = item.get("id").s();

		var organisationId = item.get("organisationId").s();

		var hashed = item.get("hashed");
		if (hashed != null && hashed.bool()) {
			var split = organisationId.indexOf(":");
			this.id = organisationId.substring(split + 1) + "\t" + id; // not a real id used anywhere but is consistent for flattener
			this.organisationId = organisationId.substring(0, split);
		} else {
			this.id = id;
			this.organisationId = organisationId;
		}
	}

	public boolean isDeleted() {
		var deleted = item.get("deleted");
		if (deleted != null && deleted.bool()) {
			return true;
		}
		return false;
	}

	//TODO: AWS has made this more difficult with version 2 of the api keep an eye out might get easy again in the future
	public <T> T convertTo(ObjectMapper mapper, Class<T> type) {
		if (isDeleted()) {
			return null;
		}
		var table = TableUtil.convertTo(mapper, item.get("item"), type);
		if (table instanceof Table) {
			Table t = (Table) table;
			var revision = item.get("revision");
			if (revision != null) {
				t.setRevision(Long.parseLong(revision.n()));
			}
			TableAccess.setTableSource(t, this.table, links, organisationId);
		}
		return table;
	}

	public String getTable() {
		return table;
	}

	Map<String, AttributeValue> getItem() {
		return item;
	}

	public Multimap<String, String> getLinks() {
		return links;
	}

	public String getId() {
		return id;
	}

	@Override
	public int compareTo(DynamoItem o) {
		return getId().compareTo(o.getId());
	}

	public String getField(String tableTarget) {
		var attribute = item.get(tableTarget);
		if (attribute == null) {
			return null;
		}
		return attribute.s();
	}

	public String getOrganisationId() {
		return organisationId;
	}
}
