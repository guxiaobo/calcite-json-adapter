/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.Pair;
import com.alibaba.fastjson.JSONObject;

/***
 * DefaultMetadataProvider reflect table and column information from the 
 * data itself, each key of the map will be a table, each sub object of the 
 * JSONObject will be columns, the sub objects' key will be the column names.
 * 
 * @author xiaobo gu
 *
 */
public class DefaultMetadataProvider 
	implements MetadataProvider {
	Map<String, List<JSONObject>> data;
	
	public DefaultMetadataProvider(Map<String, List<JSONObject>> data) {
		this.data = data;
	}

	@Override
	public Set<String> getTableNames() {
		return data.keySet();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public RelDataType getTableRowType(
			String schemaName, String tableName, 
			RelDataTypeFactory typeFactory) {
		List<JSONObject> table = data.get(tableName);
		if (null == table || table.isEmpty())
			return null;
		
		final Map<String, RelDataType> typeMap = new HashMap<String, RelDataType>();
		for (int i = 0; i < table.size(); i++) {
			JSONObject obj = table.get(i);
			for (String key : obj.keySet()) {
				Class clazz = obj.get(key).getClass();
				JsonFieldType type = JsonFieldType.of(clazz);
				if (type == null)
					continue;
				RelDataType fieldType = type.toType((JavaTypeFactory) typeFactory);
				// Already add this column, we must check they are the same type.
				if (typeMap.containsKey(key)) {
					if (!typeMap.get(key).equals(fieldType)) {
						throw new IllegalArgumentException("table " + tableName + " column " + key + " type conflic");
					}
				}
				typeMap.put(key, fieldType);
			}
		}
		if(typeMap.size() < 1)
			throw new IllegalArgumentException("table " + tableName + " does not contain support types");
		
		final List<RelDataType> types = new ArrayList<>();
		final List<String> names = new ArrayList<>();
		for (Entry<String, RelDataType> e : typeMap.entrySet()) {
			names.add(e.getKey());
			types.add(e.getValue());
		}
		return typeFactory.createStructType(Pair.zip(names, types));
	}
}
