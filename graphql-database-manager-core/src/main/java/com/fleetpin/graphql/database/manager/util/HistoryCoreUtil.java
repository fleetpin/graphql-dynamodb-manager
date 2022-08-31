package com.fleetpin.graphql.database.manager.util;

import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.annotations.History;

public final class HistoryCoreUtil {

	public static boolean hasHistory(Class<? extends Table> type) {
		Class<?> tmp = type;
		return tmp.getDeclaredAnnotation(History.class) != null;
	}

	public static boolean hasHistory(Table type) {
		return hasHistory(type.getClass());
	}
}
