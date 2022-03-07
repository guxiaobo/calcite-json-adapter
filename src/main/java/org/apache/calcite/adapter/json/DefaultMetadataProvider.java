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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/***
 * DefaultMetadataProvider reflect table and column information from the 
 * data itself, each key of the map will be a table, each sub object of the 
 * JSONObject will be columns, the sub objects' key will be the column names.
 * 
 * @author xiaobo gu
 *
 */
public class DefaultMetadataProvider <T extends Map<String, Object> >
	implements MetadataProvider {
	Map<String, List<T> > data;
	
	public DefaultMetadataProvider(Map<String, List<T>> data) {
		this.data = data;
	}

	@Override
	public Set<String> getTableNames() {
		return data.keySet();
	}

	@Override
	public RelDataType getTableRowType(
			String schemaName, String tableName, 
			RelDataTypeFactory typeFactory) {
		List<T> table = data.get(tableName);
		if (null == table || table.isEmpty())
			return null;
		
		final Map<String, RelDataType> typeMap = new HashMap<String, RelDataType>();
		for (int i = 0; i < table.size(); i++) {
			T obj = table.get(i);
			for (String key : obj.keySet()) {
				SqlTypeName sqlName = toSqlTypeName(obj.get(key));
				if (sqlName == null)
					continue;
				RelDataType fieldType = toNullableRelDataType((JavaTypeFactory) typeFactory, sqlName);
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
	
	protected  RelDataType toNullableRelDataType(
			JavaTypeFactory typeFactory,
		    SqlTypeName sqlTypeName) {	
		return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
	}
	
	@Override
	public SqlTypeName toSqlTypeName(Object obj) {
		Class<?> clazz = obj.getClass();
		if(String.class.isAssignableFrom(clazz)) {
			return SqlTypeName.VARCHAR;
		}else if(Boolean.class.isAssignableFrom(clazz)){
			return SqlTypeName.BOOLEAN;
		}else if (Long.class.isAssignableFrom(clazz)) {
			return SqlTypeName.BIGINT;
		}else if (Integer.class.isAssignableFrom(clazz)) {
			return SqlTypeName.INTEGER;
		}else if (BigDecimal.class.isAssignableFrom(clazz)) {
			return SqlTypeName.DECIMAL;
		}else if (Float.class.isAssignableFrom(clazz)) {
			return SqlTypeName.REAL;
		}else if (Double.class.isAssignableFrom(clazz)) {
			return SqlTypeName.FLOAT;
		}else if (LocalDate.class.isAssignableFrom(clazz)) {
			return SqlTypeName.DATE;
		}else if (LocalDateTime.class.isAssignableFrom(clazz)) {
			return SqlTypeName.TIMESTAMP;
		}else if (LocalTime.class.isAssignableFrom(clazz)) {
			return SqlTypeName.TIME;	
		}
		return null;
	}

	@Override
	public Object convertValue(SqlTypeName sName, Object fieldData) {
		switch(sName) {
		  case BIGINT:
			  if (fieldData instanceof Long)
				  return fieldData;
			  else if(fieldData instanceof Integer)
				  return Long.valueOf(((Integer)fieldData).longValue());
			  else if(fieldData instanceof String)
				  return  Long.valueOf((String)fieldData);
		  case INTEGER:
			  if (fieldData instanceof Integer)
				  return fieldData;
			  else if(fieldData instanceof String)
				  return Integer.valueOf((String)fieldData);
		  case BOOLEAN:
			  if (fieldData instanceof Boolean)
				  return fieldData;
			  else if(fieldData instanceof String)
				  return((String)fieldData).toLowerCase().equals("true");
		  case DECIMAL:
			  if (fieldData instanceof BigDecimal)
				  return fieldData;
			  else if(fieldData instanceof String)
				  return new BigDecimal((String)fieldData);
		  case REAL:
			  if(fieldData instanceof BigDecimal)
				  return ((BigDecimal)fieldData).floatValue();
			  else if (fieldData instanceof Float)
				  return fieldData;
			  else if(fieldData instanceof String)
				  return Float.valueOf((String)fieldData);
		  case FLOAT:
			  if(fieldData instanceof BigDecimal)
				  return ((BigDecimal)fieldData).doubleValue();
			  else if (fieldData instanceof Double)
				  return fieldData;
			  else if(fieldData instanceof String)
				  return  Double.valueOf((String)fieldData);
		  case DATE:
			  LocalDate localDate = null;
			  if(fieldData instanceof LocalDate)
				  localDate = (LocalDate)fieldData;
			  else if (fieldData instanceof String)
				  localDate = LocalDate.parse((String)fieldData);
			  if(null != localDate) {
				  Date date = Date.from(
					  (localDate.atStartOfDay(
							  ZoneId.systemDefault())).toInstant());
			  	return (int)(date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
			  }
			  break;
		  case TIMESTAMP:
			  LocalDateTime localDateTime = null;
			  if(fieldData instanceof LocalDateTime)
				  localDateTime = (LocalDateTime)fieldData;
			  else if (fieldData instanceof String)
				  localDateTime = LocalDateTime.parse((String)fieldData);
			  if(null != localDateTime) {
				  Date date = Date.from(
					  (localDateTime.atZone(
							  ZoneId.systemDefault())).toInstant());
				  return date.getTime();
			  }
			  break;
		  case TIME:
			  LocalTime localTime = null;
			  if(fieldData instanceof LocalTime)
				  localTime = (LocalTime)fieldData;
			  else if (fieldData instanceof String)
				  localTime = LocalTime.parse((String)fieldData);
			  if(null != localTime) {
				  LocalTime t = localTime;
				  int seconds = (t.getHour()*3600 + t.getMinute()*60 + t.getSecond())*1000;
				  return seconds;		 
			  }
			  break;
		  case VARCHAR:
			  return fieldData.toString();	
		  default:
				  return null;
		  }
		return null;
	}

}
