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
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/***
 * At the moment, we only support 6 common used types.
 * @author xiaobo gu
 *
 */
enum JsonFieldType {
	  STRING(String.class, "string"),
	  BOOLEAN(Boolean.class, "boolean"),
	  LONG(Long.class, "long"),
	  DECIMAL(BigDecimal.class, "decimal"),
	  DATE(LocalDate.class, "date"),
	  TIMESTAMP(LocalDateTime.class, "timestamp"),
	  INTEGER(Integer.class, "integer"),
	  FLOAT(Float.class, "float"),
	  DOUBLE(Double.class, "double");

	  @SuppressWarnings("rawtypes")
	  private final Class clazz;
	  private final String simpleName;

	  private static final Map<String, JsonFieldType> MAP = new HashMap<>();

	  static {
	    for (JsonFieldType value : values()) {
	      MAP.put(value.simpleName, value);
	    }
	  }

	  @SuppressWarnings("rawtypes")
	  JsonFieldType(Class clazz, String simpleName) {
	    this.clazz = clazz;
	    this.simpleName = simpleName;
	  }
	  
	  @SuppressWarnings("rawtypes")
	  public Class getClazz() {
		  return clazz;
	  }
	  
	  public String getSimpleName() {
		return simpleName;
	  }

	  private static RelDataType toNullableRelDataType(
			JavaTypeFactory typeFactory,
		    SqlTypeName sqlTypeName) {	
		return typeFactory.createTypeWithNullability(typeFactory.createSqlType(sqlTypeName), true);
	  }
	  
		public RelDataType toType(JavaTypeFactory typeFactory) {
			switch(this) {
			case STRING:
				return toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
			case BOOLEAN:
				return toNullableRelDataType(typeFactory, SqlTypeName.BOOLEAN);
			case LONG:
				return toNullableRelDataType(typeFactory, SqlTypeName.BIGINT);
			case DECIMAL:
				return toNullableRelDataType(typeFactory, SqlTypeName.DECIMAL);
			case INTEGER:
				return toNullableRelDataType(typeFactory, SqlTypeName.INTEGER);
			case FLOAT:
				return toNullableRelDataType(typeFactory, SqlTypeName.FLOAT);
			case DOUBLE:
				return toNullableRelDataType(typeFactory, SqlTypeName.DOUBLE);
			case DATE:
				return toNullableRelDataType(typeFactory, SqlTypeName.DATE);
			case TIMESTAMP:
				return toNullableRelDataType(typeFactory, SqlTypeName.TIMESTAMP);				
			default:
				return toNullableRelDataType(typeFactory, SqlTypeName.VARCHAR);
			}
		    
		}

	  public static JsonFieldType of(String typeString) {
	    return MAP.get(typeString);
	  }
	  
	  @SuppressWarnings("rawtypes")
	public static JsonFieldType of(Class clazz) {
		  for(JsonFieldType v : values()) {
			  if(v.getClazz().equals(clazz))
				  return v;
		  }
		  return null;
	  }
	}