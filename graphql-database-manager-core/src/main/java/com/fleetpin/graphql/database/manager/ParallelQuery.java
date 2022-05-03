package com.fleetpin.graphql.database.manager;

import java.util.Map;
import java.util.Objects;
import java.util.List;

public class ParallelQuery<T extends Table> extends Query<T, List<T>> {
	private final Map<String, ParallelStartsWith> afterParallel;
	private final Integer parallelRequests;
	private final String parallelGrouping;

	public ParallelQuery(Class<T> type, String startsWith, Integer limit, Map<String, ParallelStartsWith> afterParallel, Integer parallelRequests, String parallelGrouping) {
		super(type, startsWith, limit);
		this.afterParallel = afterParallel;
		this.parallelRequests = parallelRequests;
		this.parallelGrouping = parallelGrouping;
	}

	public Map<String, ParallelStartsWith> getAfterParallel() {
		return afterParallel;
	}

	public Integer getParallelRequestCount() { return parallelRequests; }

	public String getParallelGrouping() { return parallelGrouping; }

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		ParallelQuery<?> that = (ParallelQuery<?>) o;
		return Objects.equals(afterParallel, that.afterParallel) && Objects.equals(parallelRequests, that.parallelRequests) && Objects.equals(parallelGrouping, that.parallelGrouping);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), afterParallel, parallelRequests, parallelGrouping);
	}
}