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
import java.util.Date;
import java.util.List;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import com.alibaba.fastjson.JSONObject;
import java.math.BigDecimal;
import java.lang.Long;
import java.lang.Float;
import java.lang.Double;


/***
 * 
 * @author xiaobo gu
 *
 */
public class JsonEnumerator implements Enumerator<Object[]> {

	protected Enumerator<Object[]> enumerator;
	protected List<JSONObject> data;
	protected String tableName;  
	protected RelDataType rowDataType;
	

	  public JsonEnumerator(String tableName, 
			  List<JSONObject> data, 
			  RelDataType rowDataType) {
		  this.tableName = tableName;
		  this.data = data;
		  this.rowDataType = rowDataType;
		  
		  List<Object[]> objs = new ArrayList<Object[]>();
		  int fieldCount = rowDataType.getFieldCount();
		  List<RelDataTypeField> fields = rowDataType.getFieldList();
		  for(int i = 0; i < data.size(); i++) {
			  Object[] obj = new Object[fieldCount];
			  for(int j = 0; j < fields.size(); j++) {
				  RelDataTypeField field = fields.get(j);
				  Object fieldData = data.get(i).get(field.getName());
				  if(null == fieldData) {
					  obj[j] = null;
					  continue;
				  }
				  SqlTypeName sName = field.getType().getSqlTypeName();
				  switch(sName) {
				  case BIGINT:
					  if (fieldData instanceof Long)
						  obj[j] = fieldData;
					  else if(fieldData instanceof Integer)
						  obj[j] = Long.valueOf(((Integer)fieldData).longValue());
					  else if(fieldData instanceof String)
						  obj[j] =  Long.valueOf((String)fieldData);
					  else throw new IllegalArgumentException(fieldData.toString() + " is not long format");
					  break;
				  
				  case INTEGER:
					  if (fieldData instanceof Integer)
						  obj[j] = fieldData;
					  else if(fieldData instanceof String)
						  obj[j] = Integer.valueOf((String)fieldData);
					  else throw new IllegalArgumentException(fieldData.toString() + " is not integer format");
					  break;
					  
				  case BOOLEAN:
					  if (fieldData instanceof Boolean)
						  obj[j] = fieldData;
					  else if(fieldData instanceof String)
						  obj[j] = ((String)fieldData).toLowerCase().equals("true");
					  else throw new IllegalArgumentException(fieldData.toString() + " is not boolean format");
					  break;
				  
				  case DECIMAL:
					  if (fieldData instanceof BigDecimal)
						  obj[j] = fieldData;
					  else if(fieldData instanceof String)
						  obj[j] =  new BigDecimal((String)fieldData);
					  else throw new IllegalArgumentException(fieldData.toString() + " is not bigdecimal format");
					  break;
					  	
				  case REAL:
					  if(fieldData instanceof BigDecimal)
						  obj[j] = ((BigDecimal)fieldData).floatValue();
					  else if (fieldData instanceof Float)
						  obj[j] = fieldData;
					  else if(fieldData instanceof String)
						  obj[j] =  Float.valueOf((String)fieldData);
					  else throw new IllegalArgumentException(fieldData.toString() + " is not float format");
					  break;
					  
				  case FLOAT:
					  if(fieldData instanceof BigDecimal)
						  obj[j] = ((BigDecimal)fieldData).doubleValue();
					  else if (fieldData instanceof Double)
						  obj[j] = fieldData;
					  else if(fieldData instanceof String)
						  obj[j] =  Double.valueOf((String)fieldData);
					  else throw new IllegalArgumentException(fieldData.toString() + " is not double format");
					  break;
					  
				  case DATE:
					  LocalDate localDate = null;
					  if(fieldData instanceof LocalDate)
						  localDate = (LocalDate)fieldData;
					  else if (fieldData instanceof String)
						  localDate = LocalDate.parse((String)fieldData);
					  else
						  throw new IllegalArgumentException(fieldData.toString() + " is not date format");
					  Date date = Date.from(
							  (localDate.atStartOfDay(
									  ZoneId.systemDefault())).toInstant());
					  obj[j] = (int)(date.getTime() / DateTimeUtils.MILLIS_PER_DAY);
					  break;
					  
				  case TIMESTAMP:
					  LocalDateTime localDateTime = null;
					  if(fieldData instanceof LocalDateTime)
						  localDateTime = (LocalDateTime)fieldData;
					  else if (fieldData instanceof String)
						  localDateTime = LocalDateTime.parse((String)fieldData);
					  else
						  throw new IllegalArgumentException(fieldData.toString() + " is not datetime format");
					  
					  date = Date.from(
							  (localDateTime.atZone(
									  ZoneId.systemDefault())).toInstant());
					  obj[j] = date.getTime();
					  break;
					  
				  case TIME:
					  LocalTime localTime = null;
					  if(fieldData instanceof LocalTime)
						  localTime = (LocalTime)fieldData;
					  else if (fieldData instanceof String)
						  localTime = LocalTime.parse((String)fieldData);
					  else
						  throw new IllegalArgumentException(fieldData.toString() + " is not time format");
					  
					  LocalTime t = localTime;
					  int seconds = (t.getHour()*3600 + t.getMinute()*60 + t.getSecond())*1000;
					  obj[j] = seconds;
					  break;
				  
				  
				  case VARCHAR:
					  default:
					  obj[j] = fieldData.toString();			  
				  }
			  }
			  objs.add(obj);
		  }		  
		  enumerator = Linq4j.enumerator(objs);
	  }
	  
	  public Object[] current() {
	    return enumerator.current();
	  }

	  public boolean moveNext() {
	    return enumerator.moveNext();
	  }

	  public void reset() {
	    enumerator.reset();
	  }

	  public void close() {
	    enumerator.close();
	  }
	  
}
// End JsonEnumerator.java