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

import static com.fleetpin.graphql.database.manager.util.TableCoreUtil.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fleetpin.graphql.database.manager.util.BackupItem;
import com.google.common.collect.HashMultimap;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoBackupItem implements Comparable<DynamoBackupItem>, BackupItem {

	private String table;
	private Map<String, JsonNode> item;
	private String id;

	private HashMultimap<String, String> links;
	private String organisationId;
	private boolean hashed;
	private String parallelHash;

	public DynamoBackupItem() {}

	public DynamoBackupItem(String table, Map<String, AttributeValue> item, ObjectMapper objectMapper) {
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

		this.table = table;
		this.item = (Map<String, JsonNode>) TableUtil.convertTo(objectMapper, item, Map.class);

		this.links = HashMultimap.create();

		var links = item.get("links");
		if (links != null) {
			links
				.m()
				.forEach((t, value) -> {
					this.links.putAll(t, value.ss());
				});
		}
		this.id = item.get("id").s();

		this.organisationId = item.get("organisationId").s();

		var hashed = item.get("hashed");
		var parallelHash = item.get("parallelHash");

		if (hashed != null) {
			this.hashed = hashed.bool().booleanValue();
		}
		if (parallelHash != null) {
			this.parallelHash = parallelHash.s();
		}
	}

	public String getTable() {
		return table;
	}

	public Map<String, JsonNode> getItem() {
		return item;
	}

	@JsonIgnore
	public HashMultimap<String, String> getLinks() {
		return links;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public int compareTo(DynamoBackupItem o) {
		return getId().compareTo(o.getId());
	}

	@Override
	public String getOrganisationId() {
		return organisationId;
	}

	@Override
	public boolean isHashed() {
		return hashed;
	}

	@Override
	public String getParallelHash() {
		return parallelHash;
	}
}
