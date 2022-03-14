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

public class JsonSchemaTest extends BaseTest{
	
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
		r1.put("c11", Long.valueOf(110));
		
		return map;
	}
	
	@Test
	public void test1_1() throws SQLException {
		Map<String, List<JSONObject>> map = makeJsonMap();
		Schema schema = new JsonSchema<>("js", map);
		
		CalciteConnection conn = this.openConn1(schema, "js");		
		System.out.println("num of t1 : "      + exeGetLong(conn, "select count(*) from js.t1"));
		System.out.println("max(c1+c11) = "    + exeGetLong(conn, "select max(c1+c11) from js.t1"));
		System.out.println("min(c1+c11) = "    + exeGetLong(conn, "select min(c1+c11) from js.t1"));
		System.out.println("sum(c1+c11) = "    + exeGetLong(conn, "select sum(c1+c11) from js.t1"));
		System.out.println("avg(c1+c11) = "    + exeGetLong(conn, "select avg(c1+c11) from js.t1"));
		System.out.println("sqrt(c1) = "       + exeGetDecimal(conn, "select sqrt(c1) from js.t1"));
		System.out.println("ln(c4) = "         + exeGetDecimal(conn, "select ln(c4) from js.t1"));
		System.out.println("exp(c8) = "        + exeGetDecimal(conn, "select exp(c8) from js.t1"));
		System.out.println("log10(c9) = "        + exeGetDecimal(conn, "select log10(c9) from js.t1"));
		System.out.println("rand(c11) = "        + exeGetDouble(conn, "select rand() from js.t1"));
		


		System.out.println("upper(c2) = " + exeGetString(conn, "select upper(c2) from js.t1 limit 1"));
		System.out.println("LOWER(c2) = " + exeGetString(conn, "select LOWER(c2) from js.t1 limit 1"));
		System.out.println("TRIM(C2) = " + exeGetString(conn, "select trim(c2) from js.t1 limit 1"));
		System.out.println("SUBSTRING(c2 from 0 for 1) = " + exeGetString(conn, "select SUBSTRING(c2 from 0 for 1) from js.t1 limit 1"));
		
		System.out.println("CHAR_LENGTH(c2) = " + exeGetLong(conn, "select CHAR_LENGTH(c2) from js.t1 limit 1"));
		System.out.println("POSITION('A' in c2) = " + exeGetLong(conn, "select POSITION('A' in c2) from js.t1 limit 1"));
		
		
		
		System.out.println("c3 = " + exeGetBoolean(conn, "select c3 from js.t1 limit 1"));
		System.out.println("Year(c5) = " + exeGetInteger(conn, "select Year( c5 )from js.t1 limit 1"));
		System.out.println("Month(c6) = " + exeGetInteger(conn, "select Month(c6) from js.t1 limit 1"));
		System.out.println("second(c10) = " + exeGetInteger(conn, "select second(c10) from js.t1 limit 1"));
		
		System.out.println("c7 = " + exeGetInteger(conn, "select c7 from js.t1 limit 1"));
		System.out.println("c8 as float = " + exeGetFloat(conn, "select c8 from js.t1 limit 1"));
		System.out.println("c9 as float = " + exeGetFloat(conn, "select c9 from js.t1 limit 1"));
		System.out.println("c8 as double = " + exeGetDouble(conn, "select c8 from js.t1 limit 1"));
		System.out.println("c9 as double = " + exeGetDouble(conn, "select c9 from js.t1 limit 1"));
		
		conn.close();
	}
	
	//@Test
	public void test1_2()throws SQLException{
		Map<String, List<JSONObject>> map = makeJsonMap();
		Schema schema = new JsonSchema<>("js", map);
		CalciteConnection conn = this.openConn1(schema, "js");		
		System.out.println("c1 + c11 = " + exeGetLong(conn, "select c1 + c11 from js.t1"));
		
		conn.close();
		
	}

}
