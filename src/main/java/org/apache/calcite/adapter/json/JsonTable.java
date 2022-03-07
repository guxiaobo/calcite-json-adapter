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

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

/***
 * A JsonTable turns a List of JSONObjects as a SQL queryable table.
 * 
 * @author xiaobo gu
 *
 */
public class JsonTable <T extends Map<String, Object>>
	extends AbstractTable {
	protected String schemaName;
	protected String tableName;
	protected List<T> data ;
	protected MetadataProvider metaProvider;
	protected RelDataType rowDataType;
	
	public JsonTable(String schemaName, String tableName, 
			List<T> data,  MetadataProvider metaProvider) {
		assert tableName != null && !tableName.isBlank();
		assert data != null;
		assert metaProvider != null;
		
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.data = data;
		this.metaProvider = metaProvider;
	}
	
	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		if(null != rowDataType)
			return rowDataType;
		
		rowDataType = metaProvider.getTableRowType(schemaName, tableName, typeFactory);
		return rowDataType;
	}
	
	public Statistic getStatistic() {
		return Statistics.UNKNOWN;
	}
}

// End JsonTable.java
