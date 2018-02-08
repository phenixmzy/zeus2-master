package com.taobao.zeus.jobs.sub.tool.hs2;

public class GroovyExecutorBackend {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 1)
			throw new Exception("lack groovy file");
		
		String groovyFile = args[0];
		GroovyExecutor executor = new GroovyExecutor(groovyFile);
		executor.run();
	}

}
