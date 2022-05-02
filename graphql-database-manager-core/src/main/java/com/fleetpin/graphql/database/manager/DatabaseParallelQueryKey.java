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

public class DatabaseParallelQueryKey<T extends Table> {

	private final String organisationId;
	private final ParallelQuery<T> query;

	DatabaseParallelQueryKey(String organisationId, ParallelQuery<T> query) {
		super();
		this.organisationId = organisationId;
		this.query = query;
	}

	public String getOrganisationId() {
		return organisationId;
	}

	public ParallelQuery<T> getQuery() {
		return query;
	}

	@Override
	public int hashCode() {
		return Objects.hash(organisationId, query);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseParallelQueryKey other = (DatabaseParallelQueryKey) obj;
		return Objects.equals(organisationId, other.organisationId) && Objects.equals(query, other.query);
	}
	
	
	

}
