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
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;

/***
 * At the moment, we only support 7 common used types.
 * @author xiaobo gu
 *
 */
enum JsonFieldType {
	  STRING(String.class, "string"),
	  BOOLEAN(Boolean.class, "boolean"),
	  LONG(Long.class, "long"),
	  DECIMAL(BigDecimal.class, "decimal"),
	  DATE(java.sql.Date.class, "date"),
	  TIME(java.sql.Time.class, "time"),
	  TIMESTAMP(java.sql.Timestamp.class, "timestamp");

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

	public RelDataType toType(JavaTypeFactory typeFactory) {
	    RelDataType javaType = typeFactory.createJavaType(clazz);
	    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
	    return typeFactory.createTypeWithNullability(sqlType, true);
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