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

package com.fleetpin.graphql.database.manager;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fleetpin.graphql.builder.annotations.GraphQLIgnore;
import com.fleetpin.graphql.builder.annotations.Id;
import com.google.common.collect.HashMultimap;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

public abstract class Table {

	private String id;
	private long revision;
	private Instant createdAt;
	private Instant updatedAt;

	@JsonIgnore
	private String sourceTable;

	@JsonIgnore
	private String sourceOrganistaionId;

	@JsonIgnore
	private HashMultimap<String, String> links = HashMultimap.create();

	@Id
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setRevision(long revision) {
		this.revision = revision;
	}

	public long getRevision() {
		return revision;
	}

	void setCreatedAt(Instant createdAt) {
		this.createdAt = createdAt;
	}

	void setUpdatedAt(Instant updatedAt) {
		this.updatedAt = updatedAt;
	}

	public final Instant getCreatedAt() {
		return createdAt;
	}

	public final Instant getUpdatedAt() {
		return updatedAt;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (obj instanceof Table) {
			Table other = (Table) obj;
			return Objects.equals(getId(), other.getId());
		}
		return false;
	}

	@JsonIgnore
	@GraphQLIgnore
	String getSourceTable() {
		return sourceTable;
	}

	void setSource(String sourceTable, HashMultimap<String, String> links, String sourceOrganisationId) {
		//so bad data does not cause error
		if (createdAt == null) {
			createdAt = Instant.MIN;
		}
		if (updatedAt == null) {
			updatedAt = Instant.MIN;
		}
		this.sourceTable = sourceTable;
		this.links = links;
		this.sourceOrganistaionId = sourceOrganisationId;
	}

	@JsonIgnore
	@GraphQLIgnore
	String getSourceOrganisationId() {
		return sourceOrganistaionId;
	}

	void setLinks(String type, Collection<String> groupIds) {
		this.links.removeAll(type);
		this.links.putAll(type, groupIds);
	}

	@JsonIgnore
	@GraphQLIgnore
	HashMultimap<String, String> getLinks() {
		return links;
	}
}
