package org.apache.calcite.adapter.json;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;

import com.alibaba.fastjson.JSONObject;

public class JsonTable extends AbstractTable {
	  protected List<JSONObject> data ;

	  public JsonTable(List<JSONObject> data) {
	   this.data = data;
	  }
	  /***
	   * TODO: 
	   */
	  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
	    
		  return null;
	  }

	  public Statistic getStatistic() {
	    return Statistics.UNKNOWN;
	  }
}

// End JsonTable.java
