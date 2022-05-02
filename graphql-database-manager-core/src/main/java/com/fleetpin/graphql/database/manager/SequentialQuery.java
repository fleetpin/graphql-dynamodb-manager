package com.fleetpin.graphql.database.manager;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SequentialQuery<T extends Table> extends Query<T, List<T>> {
	private final String after;

	protected SequentialQuery(Class<T> type, String startsWith, Integer limit, String after) {
		super(type, startsWith, limit);
		this.after = after;
	}

	public String getAfter() {
		return after;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		SequentialQuery<?> that = (SequentialQuery<?>) o;
		return Objects.equals(after, that.after);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), after);
	}
}