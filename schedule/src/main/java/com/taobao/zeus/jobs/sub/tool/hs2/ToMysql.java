package com.taobao.zeus.jobs.sub.tool.hs2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;


public class ToMysql {
	private DBHelper db = null;
	
	public ToMysql(String url, String driver, String user, String password) {
		this.db = new DBHelper(url, driver, user, password);
	}
		
	public boolean executorTransaction(ArrayList<ArrayList<Object>> results, String sqlList) throws Exception,SQLException {
		Connection conn = null;
		try {
			conn = db.getConnection();
			conn.setAutoCommit(false);
			String[] sqls = sqlList.split(";");
			for (String sql : sqls) {
				String tmpSql = sql.trim();
				if (tmpSql.startsWith("delete")) {
					delete(conn, sql);
				} else if (tmpSql.startsWith("insert")) {
					if (results != null && results.size() > 0)
						insert(results,conn, sql);
				} else if (tmpSql.startsWith("update")) {
					update(conn, sql);
				}
			}
			conn.commit();
			return true;
		} catch (Exception ex) {
			if (conn == null) {
				throw new Exception(db.toString());
			}
			conn.rollback();
			throw ex;
		} finally {
			db.close(conn, null);
		}
	}
	
	private void update(Connection conn, String sql) throws Exception,SQLException {
		PreparedStatement pst = null;
		try {
			pst = conn.prepareStatement(sql);
			pst.executeUpdate();
		} catch (Exception ex) {
			throw ex;
		} finally {
			db.close(null, pst);
		}
	}
	
	private void delete(Connection conn, String sql) throws Exception,SQLException {
		PreparedStatement pst = null;
		try {
			pst = conn.prepareStatement(sql);
			pst.executeUpdate();
		} catch (Exception ex) {
			throw ex;
		} finally {
			db.close(null, pst);
		}
	}
	
	private void insert(ArrayList<ArrayList<Object>> results, Connection conn, String sql) throws Exception,SQLException {
		PreparedStatement pst = null;
		try {
			if (results == null)
				throw new Exception("插入结果列表为空");
			
			pst = conn.prepareStatement(sql);
			for (ArrayList<Object> row : results) {
				for (int i = 0, len = row.size(); i < len; i++) {
					pst.setObject(i+1, row.get(i));
				}
				pst.executeUpdate();
			}
		} catch (Exception ex) {
			throw ex;
		} finally {
			db.close(null, pst);
		}
	}
	
	public HashMap<String, String> getFeildJDBCTypes(DBHelper db, String tableName) {
		Connection conn = null;
		ResultSet rs = null;
		HashMap<String,String> feildMap = new HashMap<String, String>();
		try {
			conn = db.getConnection();
			DatabaseMetaData meta = conn.getMetaData();
			rs = meta.getColumns(null, null, tableName, null);
			while (rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");    
				String dataTypeName = rs.getString("TYPE_NAME");
				feildMap.put(columnName, dataTypeName);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			db.close(conn, null);
		}
		return feildMap;
	}
}