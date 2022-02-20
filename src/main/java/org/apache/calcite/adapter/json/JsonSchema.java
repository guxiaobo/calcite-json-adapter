package org.apache.calcite.adapter.json;

import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

public class JsonSchema extends AbstractSchema {
	
	private Map<String, Table> tableMap;
	private Map<String, List<JSONObject>> data;
	
	public JsonSchema(Map<String, List<JSONObject>> data) {
		this.data = data;
	}
	
	@Override 
	protected Map<String, Table> getTableMap() {
	    if (tableMap == null) {
	      tableMap = createTableMap();
	    }
	    return tableMap;
	}
	
	private Map<String, Table> createTableMap() {
	    // Build a map from table name to table; each list becomes a table.
	    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
	    
	    for (Entry<String,List<JSONObject>> e : data.entrySet()) {
	        final Table table = new JsonScannableTable(e.getValue());
	        builder.put(e.getKey(), table);
	    }
	    return builder.build();
	  }
}
