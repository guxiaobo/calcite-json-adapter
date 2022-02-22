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

import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/***
 * A JsonSchema turns a map of Lists of JSONObjects as  SQL queryable tables.
 * 
 * @author xiaobo gu
 *
 */
public class JsonSchema 
	extends AbstractSchema {
	
	protected Map<String, Table> tableMap;
	protected Map<String, List<JSONObject>> data;
	protected MetadataProvider metaProvider;
	
	/***
	 * Create a JsonSchema using a user provided metadata provider.
	 * @param data
	 * @param metaProvider
	 */
	public JsonSchema(Map<String, List<JSONObject>> data, MetadataProvider metaProvider) {
		assert data != null;

		this.data = data;
		this.metaProvider = metaProvider != null ?  metaProvider : new DefaultMetadataProvider(data);
	}
	/***
	 * Create a JsonSchema using the DefaultMetadataProvider.
	 * @param data
	 */
	public JsonSchema(Map<String, List<JSONObject>> data) {
		this(data,null);
	}
	
	@Override 
	protected Map<String, Table> getTableMap() {
		// Build a map from table name to table; each list becomes a table.
	    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
	    
	    for (Entry<String,List<JSONObject>> e : data.entrySet()) {
	        final Table table = new JsonScannableTable(e.getKey(),e.getValue(), metaProvider);
	        builder.put(e.getKey(), table);
	    }
	    return builder.build();
	  }
}
