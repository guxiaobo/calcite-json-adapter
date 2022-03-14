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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/***
 * A JsonTable turns a List of JSONObjects as a SQL queryable table.
 * 
 * @author xiaobo gu
 *
 */
public class JsonTable <T extends Map<String, ?>>
	extends AbstractTable 
	implements QueryableTable, TranslatableTable{
	
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
	
	public Enumerable<Object> project(DataContext root, Integer[] fields) {
	    return new AbstractEnumerable<Object>() {
	      public Enumerator<Object> enumerator() {
	        return new JsonEnumerator<T>(
	        		tableName, 
	        		data, 
	        		metaProvider.getTableRowType(schemaName, tableName, root.getTypeFactory()), 
	        		metaProvider,
	        		fields);
	      }
	    };
	  }

	@Override
	  public <K> Queryable<K> asQueryable(
			  QueryProvider queryProvider, SchemaPlus schemaPlus, String s) {
	    throw new UnsupportedOperationException();
	  }
	
	  @Override
	  public Type getElementType() {
	    return Object[].class;
	  }
   
	  @SuppressWarnings("rawtypes")
	@Override
	  public Expression getExpression(
			  SchemaPlus schemaPlus, String tableName, Class clazz) {
	    return Schemas.tableExpression(
	    		schemaPlus, getElementType(), tableName, clazz);
	  }

	  @Override
	  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
	    relOptTable.getRowType();
	    int fieldCount = relOptTable.getRowType().getFieldCount();
	    Integer[] fields = JsonEnumerator.identityList(fieldCount);
	    return new JsonTableScan(context.getCluster(), relOptTable, fields);
	  }
	  
}

// End JsonTable.java
