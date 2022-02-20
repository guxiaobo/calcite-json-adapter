package org.apache.calcite.adapter.json;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import com.alibaba.fastjson.JSONObject;

public class JsonEnumerator implements Enumerator<Object[]> {

	  private Enumerator<Object[]> enumerator;
	  List<JSONObject> data;

	  public JsonEnumerator(List<JSONObject> data) {
		  this.data = data;
		  List<Object[]> objs = new ArrayList<Object[]>();
		  //TODO: convert JSONObject to Object[] for each row
		  
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