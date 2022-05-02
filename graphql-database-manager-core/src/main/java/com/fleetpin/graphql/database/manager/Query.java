package com.fleetpin.graphql.database.manager;

import java.util.Map;
import java.util.Objects;

public class Query<T extends Table> {

	private final Class<T> type;
	private final String startsWith;
	private final String after;
	private final Integer limit;

	private final Map<String, ParallelStartsWith> afterParallel;
	private final Integer parallelRequests;
	private final String parallelGrouping;

	public Query(Class<T> type, String startsWith, String after, Integer limit) {
		this(type, startsWith, after, null, limit, null, null);
	}

	public Query(Class<T> type, String startsWith, Map<String, ParallelStartsWith> afterParallel, Integer limit, Integer parallelRequests, String parallelGrouping) {
		this(type, startsWith, null, afterParallel, limit, parallelRequests, parallelGrouping);
	}


	private Query(Class<T> type, String startsWith, String after, Map<String, ParallelStartsWith> afterParallel, Integer limit, Integer parallelRequests, String parallelGrouping) {
		if (type == null) {
			throw new RuntimeException("type can not be null, did you forget to call .on(Table::class)?");
		}
		this.type = type;
		this.startsWith = startsWith;
		this.after = after;
		this.limit = limit;
		this.afterParallel = afterParallel;
		this.parallelRequests = parallelRequests;
		this.parallelGrouping = parallelGrouping;
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

	public Map<String, ParallelStartsWith> getAfterParallel() {
		return afterParallel;
	}

	public Integer getParallelRequestCount() { return parallelRequests; }

	public String getParallelGrouping() { return parallelGrouping; }

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