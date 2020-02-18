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

import com.fleetpin.graphql.database.manager.table.Table;

import java.util.Objects;

public class DatabaseKey {

	private final String organisationId;
	private final Class<? extends Table> type;
	private final String id;

	public DatabaseKey(String organisationId, Class<? extends Table> type, String id) {
		this.organisationId = organisationId;
		this.type = type;
		this.id = id;
	}

	public String getOrganisationId() {
		return organisationId;
	}

	public Class<? extends Table> getType() {
		return type;
	}

	public String getId() {
		return id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, organisationId, type);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseKey other = (DatabaseKey) obj;
		return Objects.equals(id, other.id) && Objects.equals(organisationId, other.organisationId)
				&& Objects.equals(type, other.type);
	}

	@Override
	public String toString() {
		return "DatabaseKey [organisationId=" + organisationId + ", type=" + type + ", id=" + id + "]";
	}
	
	
	
	

}
