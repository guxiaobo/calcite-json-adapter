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
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import com.alibaba.fastjson.JSONObject;


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
				  obj[j] = data.get(i).get(fields.get(j).getName());
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