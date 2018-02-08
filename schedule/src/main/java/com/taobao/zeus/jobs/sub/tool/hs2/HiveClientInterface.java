package com.taobao.zeus.jobs.sub.tool.hs2;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface HiveClientInterface {
	 public void execute(String sql) throws Exception;
	 public ResultSet executeResultSet(ArrayList<String> hqls) throws Exception;
	 public ArrayList<ArrayList<Object>> executeAndGetRows(ArrayList<String> hqls) throws Exception;
	 public void clean() throws Exception;
}
