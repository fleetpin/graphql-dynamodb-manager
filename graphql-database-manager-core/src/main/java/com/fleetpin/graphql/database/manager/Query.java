package com.fleetpin.graphql.database.manager;

import java.util.Map;
import java.util.Objects;

public abstract class Query<T extends Table, R> {

	private final Class<T> type;
	private final String startsWith;
	private final Integer limit;

	protected Query(Class<T> type, String startsWith, Integer limit) {
		this.type = type;
		this.startsWith = startsWith;
		this.limit = limit;
	}

	public Class<T> getType() {
		return type;
	}
	
	public String getStartsWith() {
		return startsWith;
	}

	public Integer getLimit() {
		return limit;
	}

	public boolean hasLimit() { return getLimit() != null; }

	@Override
	public int hashCode() {
		return Objects.hash(limit, startsWith, type);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Query other = (Query) obj;
		return Objects.equals(limit, other.limit)
				&& Objects.equals(startsWith, other.startsWith)
				&& Objects.equals(type, other.type);
	}
}