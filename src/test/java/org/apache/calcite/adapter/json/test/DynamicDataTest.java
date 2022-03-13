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

import com.alibaba.fastjson.JSONObject;

public class DynamicDataTest extends BaseTest{
	
	private Map<String, List<JSONObject>> makeJsonMap(){
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		List<JSONObject> t1 = new ArrayList<JSONObject>();
		JSONObject r1 = new JSONObject();
		t1.add(r1);
		t1.add(r1);
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
		
		return map;
	}
	
	private JSONObject makeRow() {
		JSONObject r1 = new JSONObject();
		
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
		return r1;
	}
	
	/***
	 * 在语句执行前后动态增加数据的行数
	 * @throws SQLException
	 */
	//@Test
	public void test1() throws SQLException {
		List<JSONObject> t = new ArrayList<JSONObject>();
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		JSONObject row = makeRow();
		map.put("t1", t);
		t.add(row);
		
		String sql1 = "select count(*) from js.t1";
		//String sql2 = "select count(*) from js.t1";
		try {
			Schema schema = new JsonSchema<>("js", map);
			
			CalciteConnection conn = this.openConn1(schema, "js");		
			System.out.println("sql1 result " + exeGetLong(conn, sql1));
			
			t.add(row);
			System.out.println("sql1 result " + exeGetLong(conn, sql1));
			
			conn.close();
		}catch(Throwable th) {
			th.printStackTrace();
		}
	}
	
	/***
	 * 动态某个列的改变数据类型
	 * @throws SQLException
	 */
	@Test
	public void test2() throws SQLException {
		List<JSONObject> t = new ArrayList<JSONObject>();
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		JSONObject row = makeRow();
		map.put("t1", t);
		t.add(row);
		
		String sql1 = "select c1 from js.t1";

		Schema schema = new JsonSchema<>("js", map);
		
		CalciteConnection conn = this.openConn1(schema, "js");		
		System.out.println("sql2 result " + exeGetLong(conn, sql1));
		
		row.put("c1", Long.valueOf(200L));
		System.out.println("sql2 result " + exeGetLong(conn, sql1));
		
		
		conn.close();
	}
	
	//@Test
	public void test3() throws SQLException {
		List<JSONObject> t = new ArrayList<JSONObject>();
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		JSONObject row = makeRow();
		map.put("t1", t);
		t.add(row);
		
		
		String sql3 = "select c11 from js.t1";

		Schema schema = new JsonSchema<>("js", map);
		
		CalciteConnection conn = this.openConn1(schema, "js");		
		try {
			System.out.println("sql3 result " + exeGetLong(conn, sql3));
		}catch(Throwable et) {
			et.printStackTrace();
		}
		
		row.put("c11", Long.valueOf(300L));
		System.out.println("sql3 result " + exeGetLong(conn, sql3));
		
		conn.close();
	}
	
	//@Test
	public void test4() throws SQLException {
		List<JSONObject> t = new ArrayList<JSONObject>();
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		JSONObject row = makeRow();
		map.put("t1", t);
		t.add(row);
		
		
		String sql4 = "select count(*) from js.t2";

		Schema schema = new JsonSchema<>("js", map);
		
		CalciteConnection conn = this.openConn1(schema, "js");		
		try {
			System.out.println("sql4 result " + exeGetLong(conn, sql4));
		}catch(Throwable et) {
			et.printStackTrace();
		}
		
		map.put("t2", t);
		System.out.println("sql4 result " + exeGetLong(conn, sql4));
		
		conn.close();
	}
	
	//@Test
	public void test5() throws SQLException {
		List<JSONObject> t = new ArrayList<JSONObject>();
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		JSONObject row = makeRow();
		map.put("t1", t);
		t.add(row);
		
		
		String sql5 = "select count(*) from s2.t1";

		Schema schema = new JsonSchema<>("s1", map);
		
		CalciteConnection conn = this.openConn1(schema, "s1");
		
		try {
			System.out.println("sql5 result " + exeGetLong(conn, sql5));
		}catch(Throwable et) {
			et.printStackTrace();
		}
		Schema s2 = new JsonSchema<>("s2", map);
		conn.getRootSchema().add("s2", s2);
		
		System.out.println("sql5 result " + exeGetLong(conn, sql5));
		
		conn.close();
	}

}
