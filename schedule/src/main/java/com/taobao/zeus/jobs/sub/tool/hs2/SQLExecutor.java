package com.taobao.zeus.jobs.sub.tool.hs2;

import java.util.ArrayList;
import java.util.HashMap;

public class SQLExecutor {
	private String mUrl; //"jdbc:mysql://192.168.1.225:3306/zoneconsume"
	private String mDriver; //"com.mysql.jdbc.Driver"
	private String mUser; //"root"
	private String mPassword; //"redhat"
	
	private String rdbSqls = null;
	
	public void builder(HashMap<String, String> params, String rdbSqls) {
		setmUrl(params.get("sql.url"));
		setmDriver(params.get("sql.driver"));
		setmUser(params.get("sql.user"));
		setmPassword(params.get("sql.password"));
		
		setRdbSqls(rdbSqls);
	}

	public String getmUrl() {
		return mUrl;
	}

	public void setmUrl(String mUrl) {
		this.mUrl = mUrl;
	}

	public String getmDriver() {
		return mDriver;
	}

	public void setmDriver(String mDriver) {
		this.mDriver = mDriver;
	}

	public String getmUser() {
		return mUser;
	}

	public void setmUser(String mUser) {
		this.mUser = mUser;
	}

	public String getmPassword() {
		return mPassword;
	}

	public void setmPassword(String mPassword) {
		this.mPassword = mPassword;
	}

	public String getRdbSqls() {
		return rdbSqls;
	}

	public void setRdbSqls(String rdbSqls) {
		this.rdbSqls = rdbSqls;
	}
	
	public void run() throws Exception {
		try {
			ToMysql toMysql = new ToMysql(mUrl, mDriver, mUser, mPassword);
			boolean ok = toMysql.executorTransaction(null, rdbSqls);
		} catch (Exception ex) {
			throw ex;
		}
	}
	
}
