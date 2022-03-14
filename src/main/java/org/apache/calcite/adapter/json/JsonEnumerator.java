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
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;


/***
 * 
 * @author xiaobo gu
 *
 */
public class JsonEnumerator <T extends Map<String, ?> >
	implements Enumerator<Object> {

	protected List<T> data;
	protected String tableName;  
	protected RelDataType rowDataType;
	protected MetadataProvider provider;
	protected Integer[] fields;
	private int pos;
	

	  public JsonEnumerator(String tableName, 
			  List<T> data, 
			  RelDataType rowDataType,
			  MetadataProvider provider,
			  Integer[] fields) {
		  this.tableName = tableName;
		  this.data = data;
		  this.rowDataType = rowDataType;
		  this.provider = provider;
		  this.fields = fields;
		  pos = -1;		  
	  }
	  
	  public Object current() {
			int fieldCount = fields.length;
			Object[] obj = new Object[fieldCount];
			for (int j = 0; j < fieldCount; j++) {
				//RelDataTypeField field = rowDataType.getField(fieldIndex[j], false, false);
				RelDataTypeField field = rowDataType.getFieldList().get(fields[j]);
				
				Object fieldData = data.get(pos).get(field.getName());
				if (null == fieldData) {
					obj[j] = null;
					continue;
				}
				SqlTypeName sName = field.getType().getSqlTypeName();
				obj[j] = provider.convertValue(sName, fieldData);
			}
			if (fields.length > 1)
				return obj;
			else
				return obj[0];
	  }
	  
	  @Override
	  public boolean moveNext() {
	    return (this.data.size() > (++this.pos));
	  }

	  @Override
	  public void reset() {
	    this.pos = 0;
	  }

	  @Override
	  public void close() {}
	  
	  
	 
	  public static Integer[] identityList(int n) {
		    Integer[] ret = new Integer[n];
		    for (int i = 0; i < n; i ++) {
		      ret[i] = i;
		    }
		    return ret;
	  }
	  
}
// End JsonEnumerator.java