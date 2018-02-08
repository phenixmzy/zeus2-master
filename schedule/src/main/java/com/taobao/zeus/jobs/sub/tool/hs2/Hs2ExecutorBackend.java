package com.taobao.zeus.jobs.sub.tool.hs2;

import java.util.ArrayList;
import java.util.HashMap;

public class Hs2ExecutorBackend {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		if (args.length != 3) {
			throw new Exception("Executor must container mysql,hive confInfo and commandlist");
		}
		
		String param = args[0];
		String hs2Command = args[1];
		String sql = args[2];
		
		if (hs2Command.trim().length() == 0) {
			throw new Exception("invaild hql");
		}
		
		HashMap<String, String> conf = getConf(param);
		Hs2Executor executor = new Hs2Executor();
		executor.builder(conf, hs2Command, sql);		
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