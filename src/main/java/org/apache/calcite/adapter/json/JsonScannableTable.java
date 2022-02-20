package org.apache.calcite.adapter.json;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;
import com.alibaba.fastjson.JSONObject;

public class JsonScannableTable extends JsonTable
	implements ScannableTable {
	/**
	* Creates a JsonScannableTable.
	*/
	public JsonScannableTable(List<JSONObject> data) {
		super(data);
	}

	public String toString() {
		return "JsonScannableTable";
	}

	public Enumerable<Object[]> scan(DataContext root) {
		return new AbstractEnumerable<Object[]>() {
		  public Enumerator<Object[]> enumerator() {
		    return new JsonEnumerator(data);
		  }
		};
	}
}