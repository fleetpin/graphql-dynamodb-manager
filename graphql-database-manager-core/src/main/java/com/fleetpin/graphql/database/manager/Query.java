package com.fleetpin.graphql.database.manager;

import java.util.Objects;

public class Query<T extends Table> {

	private final Class<T> type;
	private final String startsWith;
	private final String after;
	private final Integer limit;

	Query(Class<T> type, String startsWith, String after, Integer limit) {
		if (type == null) {
			throw new RuntimeException("type can not be null, did you forget to call .on(Table::class)?");
		}
		this.type = type;
		this.startsWith = startsWith;
		this.after = after;
		this.limit = limit;
	}

	public Class<T> getType() {
		return type;
	}
	
	public String getStartsWith() {
		return startsWith;
	}

	public String getAfter() {
		return after;
	}


	public Integer getLimit() {
		return limit;
	}

	public boolean hasLimit() { return getLimit() != null; }

	@Override
	public int hashCode() {
		return Objects.hash(after, limit, startsWith, type);
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
		return Objects.equals(after, other.after) && Objects.equals(limit, other.limit)
				&& Objects.equals(startsWith, other.startsWith)
				&& Objects.equals(type, other.type);
	}

    
}