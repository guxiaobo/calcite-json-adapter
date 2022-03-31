package org.apache.calcite.adapter.json.test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.json.JsonSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.junit.Test;

public class MapSchemaTest extends BaseTest{
	
	private Map<String, List<Map<String, Object>>> makeJsonMap(){
		Map<String, List<Map<String, Object>>> map = new HashMap<String, List<Map<String, Object>>>();
		List<Map<String, Object>> t1 = new ArrayList<Map<String, Object>>();
		Map<String, Object> r1 = new HashMap<String, Object>();
		t1.add(r1);
		
		map.put("t1", t1);
		
		r1.put("c1", Long.valueOf(100));
		r1.put("c2", "column2");
		r1.put("c3", Boolean.FALSE);
		r1.put("c4", new BigDecimal("2.1"));
		r1.put("c5", LocalDate.now());
		r1.put("c6", LocalDateTime.now());
		r1.put("c7", Integer.valueOf(200));
		r1.put("c8", Double.valueOf(3.43d));
		r1.put("c9", Float.valueOf(2.33f));
		r1.put("c10", LocalTime.now());
		
		
		List<Map<String, Object>> t2 = new ArrayList<Map<String, Object>>();
		Map<String, Object> r2 = new HashMap<String, Object>();
		t2.add(r2);
		
		map.put("t2", t2);
		
		r2.put("c21", Long.valueOf(200));
		r2.put("c22", "column22");
		r2.put("c23", Boolean.FALSE);
		r2.put("c24", new BigDecimal("2.4"));
		r2.put("c25", LocalDate.now());
		r2.put("c26", LocalDateTime.now());
		r2.put("c27", Integer.valueOf(270));
		r2.put("c28", Double.valueOf(2.83d));
		r2.put("c29", Float.valueOf(2.29f));
		r2.put("c210", LocalTime.now());
		
		return map;
	}
	
	@Test
	public void test01() throws SQLException {
			
			Map<String, List<Map<String, Object>>> map = makeJsonMap();
			Schema schema = new JsonSchema<>("js", map);
			
			CalciteConnection conn = this.openConn1(schema, "js");		
			
			String sql = "select a.c1 + b.c21 from js.t1  as a join js.t2 as b on true";
			System.out.println("join result " + exeGetLong(conn, sql));
			
		}
	
	//@Test
	public void test10() throws SQLException {
		Map<String, List<Map<String, Object>>> map = makeJsonMap();
		
		//String sql1 = "select count(*) from t1";
		String sql2 = "select count(*) from js.t1";

		Schema schema = new JsonSchema<>("js", map);
		
		CalciteConnection conn = this.openConn1(schema, "js");		
		//System.out.println("sql1 result " + exeGetLong(conn, sql1));
		System.out.println("sql2 result " + exeGetLong(conn, sql2));
		System.out.println("c1 = " + exeGetLong(conn, "select max(c1) from js.t1"));
		System.out.println("c2 = " + exeGetString(conn, "select c2 from js.t1 limit 1"));
		System.out.println("c3 = " + exeGetBoolean(conn, "select c3 from js.t1 limit 1"));
		System.out.println("c4 = " + exeGetDecimal(conn, "select c4 from js.t1 limit 1"));
		System.out.println("c5 = " + exeGetDate(conn, "select c5 from js.t1 limit 1"));
		System.out.println("c6 = " + exeGetTimestamp(conn, "select c6 from js.t1 limit 1"));
		System.out.println("c7 = " + exeGetInteger(conn, "select c7 from js.t1 limit 1"));
		System.out.println("c8 as float = " + exeGetFloat(conn, "select c8 from js.t1 limit 1"));
		System.out.println("c9 as float = " + exeGetFloat(conn, "select c9 from js.t1 limit 1"));
		System.out.println("c8 as double = " + exeGetDouble(conn, "select c8 from js.t1 limit 1"));
		System.out.println("c9 as double = " + exeGetDouble(conn, "select c9 from js.t1 limit 1"));
		System.out.println("c10 = " + exeGetTime(conn, "select c10 from js.t1 limit 1"));
		conn.close();
	}

}
