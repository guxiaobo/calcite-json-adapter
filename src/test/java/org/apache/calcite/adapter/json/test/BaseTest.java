package org.apache.calcite.adapter.json.test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;

public class BaseTest {
	
	public CalciteConnection openConn1(Schema schema, String schemaName)
			throws SQLException {
		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		info.setProperty("caseSensitive", "false");
		info.setProperty("schema", "hr");

		Connection connection =
		    DriverManager.getConnection("jdbc:calcite:", info);
		CalciteConnection conn =
		    connection.unwrap(CalciteConnection.class);
		conn.getRootSchema().add(schemaName, schema);
		return conn;
	}
	
	
	public static Long exeGetLong(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Long ret = null;
		if(rs.next()) {
			ret = rs.getLong(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static BigDecimal exeGetDecimal(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		BigDecimal ret = null;
		if(rs.next()) {
			ret = rs.getBigDecimal(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Boolean exeGetBoolean(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Boolean ret = null;
		if(rs.next()) {
			ret = rs.getBoolean(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Date exeGetDate(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Date ret = null;
		if(rs.next()) {
			ret = rs.getDate(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Timestamp exeGetTimestamp(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Timestamp ret = null;
		if(rs.next()) {
			ret = rs.getTimestamp(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Time exeGetTime(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Time ret = null;
		if(rs.next()) {
			ret = rs.getTime(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static String exeGetString(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		String ret = null;
		if(rs.next()) {
			ret = rs.getString(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Integer exeGetInteger(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Integer ret = null;
		if(rs.next()) {
			ret = rs.getInt(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Float exeGetFloat(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Float ret = null;
		if(rs.next()) {
			ret = rs.getFloat(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	public static Double exeGetDouble(Connection conn, String sql) throws SQLException {
		Statement statement = conn.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		Double ret = null;
		if(rs.next()) {
			ret = rs.getDouble(1);
		}
		rs.close();
		statement.close();
		return ret;
	}
	
	



}
