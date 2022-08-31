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

import java.util.Objects;

public class DatabaseQueryHistoryKey<T extends Table> {

	private final String organisationId;
	private final QueryHistory<T> queryHistory;

	DatabaseQueryHistoryKey(String organisationId, QueryHistory<T> queryHistory) {
		super();
		this.organisationId = organisationId;
		this.queryHistory = queryHistory;
	}

	public String getOrganisationId() {
		return organisationId;
	}

	public QueryHistory<T> getQueryHistory() {
		return queryHistory;
	}

	@Override
	public int hashCode() {
		return Objects.hash(organisationId, queryHistory);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		DatabaseQueryHistoryKey other = (DatabaseQueryHistoryKey) obj;
		return Objects.equals(organisationId, other.organisationId) && Objects.equals(queryHistory, other.queryHistory);
	}
}
