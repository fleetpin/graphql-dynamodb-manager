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

package com.fleetpin.graphql.database.manager.dynamo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fleetpin.graphql.database.manager.Table;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public final class Flatterner {

	private final Map<String, Table> lookup;
	
	
	Flatterner() {
		lookup = new HashMap<>();
	}
	
	
	public <T extends Table> void add(String table, List<Map<String, AttributeValue>> list) {
		list.forEach(item -> {
			var i = new T(table, item);
			if(i.isDeleted()) {
				lookup.remove(i.getId());
			}else {
				lookup.merge(i.getId(), i, this::merge);
			}
		});
		
	}


	public <T extends Table> T get(String id) {
		return (T) lookup.get(id);
	}


	public <T extends Table> void addItems(List<T> list) {
		list.forEach(item -> {
			if(item.isDeleted()) {
				lookup.remove(item.getId());
			}else {
				lookup.merge(item.getId(), item, this::merge);
			}
		});
		
	}
	
	public <T extends Table> T merge(T existing, T replace) {
		var item = new HashMap<>(replace.getItem());
		//only links in parent
		if(item.get("item") == null) {
			item.put("item", existing.getItem().get("item"));
		}
		var toReturn = new T(replace.getTable(), item);
		toReturn.getLinks().putAll(existing.getLinks());

		return toReturn;
	}


	public <T extends Table> List<T> results() {
		var items = new ArrayList<T>(lookup.values());
		Collections.sort(items);
		return items;
	}

}
