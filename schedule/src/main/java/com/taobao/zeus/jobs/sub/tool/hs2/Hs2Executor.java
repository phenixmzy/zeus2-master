package com.taobao.zeus.jobs.sub.tool.hs2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Hs2Executor {
	private String mUrl; //"jdbc:mysql://192.168.1.225:3306/zoneconsume"
	private String mDriver; //"com.mysql.jdbc.Driver"
	private String mUser; //"root"
	private String mPassword; //"redhat"
	
	private String hUrl; //"jdbc:hive2://192.168.1.225:10000/appstore"
	private String hDriver; //"org.apache.hive.jdbc.HiveDriver"
	private String hUser; //"root"
	private String hPassword; //"redhat"
	
	private String hs2Command;

	private ArrayList<String> hqlList = new ArrayList<String>();
	private String rdbSqls = null;
	
	
	public void builder(HashMap<String, String> params, String hs2Command, String sqlCommand) {
		setmUrl(params.get("sql.url"));
		setmDriver(params.get("sql.driver"));
		setmUser(params.get("sql.user"));
		setmPassword(params.get("sql.password"));
		
		sethUrl(params.get("hs2.url"));
		sethDriver(params.get("hs2.driver"));
		sethUser(params.get("hs2.user"));
		sethPassword(params.get("hs2.password"));
		setHs2Command(hs2Command);
		setRdbSqls(sqlCommand);
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

	public String gethUrl() {
		return hUrl;
	}

	public void sethUrl(String hUrl) {
		this.hUrl = hUrl;
	}

	public String gethDriver() {
		return hDriver;
	}

	public void sethDriver(String hDriver) {
		this.hDriver = hDriver;
	}

	public String gethUser() {
		return hUser;
	}

	public void sethUser(String hUser) {
		this.hUser = hUser;
	}

	public String gethPassword() {
		return hPassword;
	}

	public void sethPassword(String hPassword) {
		this.hPassword = hPassword;
	}

	public String getHs2Command() {
		return hs2Command;
	}

	public void setHs2Command(String hs2Command) {
		this.hs2Command = hs2Command;
	}

	public String getRdbSqls() {
		return rdbSqls;
	}

	public void setRdbSqls(String rdbSqls) {
		this.rdbSqls = rdbSqls;
	}
	
	private void parseHs2Command() {
		String[] hqls = hs2Command.split(";");
		for (String hql : hqls)
			hqlList.add(hql);
	}
	
	public void run() throws Exception {
		parseHs2Command();
		
		HS2Client client = null;
		ToMysql toMysql = null;
		try {
			toMysql = new ToMysql(mUrl, mDriver, mUser, mPassword);
			client = new HS2Client(hUrl, hDriver, hUser, hPassword);
			client.init();
			ArrayList<ArrayList<Object>> results = client.executeAndGetRows(hqlList);
			boolean ok = toMysql.executorTransaction(results, rdbSqls);

		} catch (Exception ex) {
			throw ex;
		} finally {
			if (client != null)
				client.clean();
		}
	}	
}