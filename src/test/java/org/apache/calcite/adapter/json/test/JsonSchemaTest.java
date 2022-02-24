package org.apache.calcite.adapter.json.test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.json.JsonSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.junit.Test;

import com.alibaba.fastjson.JSONObject;

public class JsonSchemaTest extends BaseTest{
	
	private Map<String, List<JSONObject>> makeJsonMap(){
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		List<JSONObject> t1 = new ArrayList<JSONObject>();
		JSONObject r1 = new JSONObject();
		t1.add(r1);
		map.put("t1", t1);
		
		r1.put("c1", Long.valueOf(100));
		r1.put("c2", "column2");
		r1.put("c3", Boolean.FALSE);
		r1.put("c4", new BigDecimal("2.1"));
		r1.put("c5", LocalDate.now());
		r1.put("c6", LocalDateTime.now());
		
		return map;
	}
	
	//@Test
	public void test10() throws SQLException {
		Map<String, List<JSONObject>> map = makeJsonMap();
		
		String sql1 = "select count(*) from t1";
		String sql2 = "select count(*) from js.t1";

		Schema schema = new JsonSchema(map);
		
		CalciteConnection conn = this.openConn1(schema, "js");		
		Long l1 = exeGetLong(conn, sql2);
		System.out.println("sql result1 " + l1);
		conn.close();
	}


}
