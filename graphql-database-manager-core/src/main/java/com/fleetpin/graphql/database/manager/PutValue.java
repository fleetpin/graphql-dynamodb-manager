package com.fleetpin.graphql.database.manager;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class PutValue<T extends Table> {

	private final String organisationId;
	private final T entity;
	private final boolean check;
	private final CompletableFuture<T> future;

	public PutValue(String organisationId, T entity, boolean check, CompletableFuture<T> future) {
		this.organisationId = organisationId;
		this.entity = entity;
		this.check = check;
		this.future = future;
	}

	public T getEntity() {
		return entity;
	}

	public String getOrganisationId() {
		return this.organisationId;
	}

	public boolean getCheck() {
		return check;
	}

	public CompletableFuture<T> getFuture() {
		return future;
	}

	public void resolve() {
		entity.setRevision(entity.getRevision() + 1);
		future.complete(entity);
	}

	public void fail(Throwable error) {
		if (!future.isDone()) {
			future.completeExceptionally(error);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		PutValue<?> putValue = (PutValue<?>) o;
		return (
			check == putValue.check &&
			Objects.equals(organisationId, putValue.organisationId) &&
			Objects.equals(entity, putValue.entity) &&
			Objects.equals(future, putValue.future)
		);
	}

	@Override
	public int hashCode() {
		return Objects.hash(organisationId, entity, check, future);
	}
}
