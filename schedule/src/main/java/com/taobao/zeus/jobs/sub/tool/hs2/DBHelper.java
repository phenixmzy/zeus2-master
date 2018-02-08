package com.taobao.zeus.jobs.sub.tool.hs2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DBHelper {
	private String url;
	private String driver;
	private String user;
	private String pwd;

    public DBHelper(String url, String driver, String user, String pwd) { 
        this.url = url;
        this.driver = driver;
        this.user = user;
        this.pwd = pwd;
    }

    public Connection getConnection() throws SQLException, ClassNotFoundException {
    	Class.forName(driver);
    	return DriverManager.getConnection(url, user, pwd);
    }
    
    public boolean execute(PreparedStatement ps) throws SQLException {
    	return ps.execute();
    }
    
    public void closePreparedStatement(PreparedStatement pst) {
        try {
        	if (pst != null)
        		pst.close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }
    
    public void close(Connection conn, PreparedStatement pst) {  
        try {
        	if (conn != null)
        		conn.close();
        	if (pst != null)
        		pst.close();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }
}
