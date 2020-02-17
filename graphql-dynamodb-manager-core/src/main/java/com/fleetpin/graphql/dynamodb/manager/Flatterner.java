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

package com.fleetpin.graphql.dynamodb.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Flatterner {

	private final Map<String, DynamoItem> lookup;
	
	
	Flatterner() {
		lookup = new HashMap<>();
	}
	
	
	public void add(String table, List<Map<String, AttributeValue>> list) {
		list.forEach(item -> {
			var i = new DynamoItem(table, item);
			if(i.isDeleted()) {
				lookup.remove(i.getId());
			}else {
				lookup.merge(i.getId(), i, this::merge);
			}
		});
		
	}


	public DynamoItem get(String id) {
		return lookup.get(id);
	}


	public void addItems(List<DynamoItem> list) {
		list.forEach(item -> {
			if(item.isDeleted()) {
				lookup.remove(item.getId());
			}else {
				lookup.merge(item.getId(), item, this::merge);
			}
		});
		
	}
	
	public DynamoItem merge(DynamoItem existing, DynamoItem replace) {
		var item = new HashMap<>(replace.getItem());
		//only links in parent
		if(item.get("item") == null) {
			item.put("item", existing.getItem().get("item"));
		}
		var toReturn = new DynamoItem(replace.getTable(), item);
		toReturn.getLinks().putAll(existing.getLinks());

		return toReturn;
	}


	public List<DynamoItem> results() {
		var items = new ArrayList<DynamoItem>(lookup.values());
		Collections.sort(items);
		return items;
	}

}
