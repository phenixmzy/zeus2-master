package com.taobao.zeus.jobs.sub.tool.hs2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HS2Client implements HiveClientInterface {
	private String url;
	private String driver;
	private String user;
	private String password;
	
	private Connection conn;
	private Statement stmt;
	private ResultSet rs;

	public HS2Client(String url, String driver, String user, String password)  {
		this.url = url;
		this.driver = driver;
		this.user = user;
		this.password = password;
	}
	
	public void init() throws SQLException, ClassNotFoundException {
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
		stmt = conn.createStatement();
	}
	
	private ArrayList<Object> getRowOne(ResultSet rs) throws SQLException {
		ArrayList<Object> row = new ArrayList<Object>();

		ResultSetMetaData rsmd = rs.getMetaData();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			row.add(rs.getObject(i));
		}
		return row;
	}
	

	private ArrayList<ArrayList<Object>> getRows(ResultSet rs) throws SQLException {
		ArrayList<ArrayList<Object>> ret = new ArrayList<ArrayList<Object>>();
		while (rs.next()) {
			ret.add(getRowOne(rs));
		}
		return ret;
	}
	
	
	@Override
	public void execute(String hql) throws Exception {
		// TODO Auto-generated method stub
		try {
			stmt.execute(hql);
		} catch (Exception ex) {
			throw ex;
		} finally {
			clean();
		}		
	}
	
	@Override
	public ResultSet executeResultSet(ArrayList<String> hqls) throws Exception  {
		int len = hqls.size();
		String lastHql = len < 2 ? hqls.get(0) : hqls.get(len - 1);
		
		for (int i = 0; i < len - 1 ; i++) {
			String hql = hqls.get(i);
			stmt.execute(hql);
		}
		
		try {
			return stmt.executeQuery(lastHql);
		} catch (SQLException e) {
			throw e;
		}
	}
	
	/* (non-Javadoc)
	 * 当返回的查询数据量很大时,可能会造成内存溢出
	 * @see com.taobao.zeus.jobs.sub.tool.hs2.HiveClientInterface#executeAndGetRows(java.util.ArrayList)
	 */
	@Override
	public ArrayList<ArrayList<Object>> executeAndGetRows(ArrayList<String> hqls) throws Exception {
		// TODO Auto-generated method stub
		rs = executeResultSet(hqls);
		if (rs != null)
			return getRows(rs);
		else 
			return new ArrayList<ArrayList<Object>>();
	}

	@Override
	public void clean() throws Exception {
		// TODO Auto-generated method stub
		if (rs != null)
			rs.close();
		
		if (stmt != null)
			stmt.close();
		
		if (conn != null)
			conn.close();
	}
}