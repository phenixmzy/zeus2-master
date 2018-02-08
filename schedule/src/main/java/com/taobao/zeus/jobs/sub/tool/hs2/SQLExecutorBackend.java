package com.taobao.zeus.jobs.sub.tool.hs2;

import java.util.HashMap;

public class SQLExecutorBackend {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("Executor must has mysql and confInfo");
		}
		
		String param = args[0];
		String sql = args[1];
		
		HashMap<String, String> conf = getConf(param);
		if (sql.trim().length() == 0) {
			throw new Exception("invaild sql");
		}
		
		SQLExecutor executor = new SQLExecutor();
		executor.builder(conf, sql);
		executor.run();
	}
	
	
	public static HashMap<String, String> getConf(String params) {
		HashMap<String, String> conf = new HashMap<String, String>();
		String[] paramsArray = params.split(",");
		for(String param : paramsArray) {
			String[] c = param.split("=");
			if (c.length == 2) {
				String key = c[0];
				String value = c[1];
				conf.put(key, value);
			} else if (c.length == 1) {
				String key = c[0];
				conf.put(key, "");
			}
		}	
		return conf;
	}
}
