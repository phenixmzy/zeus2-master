package com.taobao.zeus.jobs.sub.tool.hs2;

import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;

import java.io.File;

public class GroovyExecutor {
	private String groovyFile;
	
	public GroovyExecutor(String groovyFile) {
		this.groovyFile = groovyFile;
	}
	
	public void run() throws Exception {
		ClassLoader parent = ClassLoader.getSystemClassLoader();
		GroovyClassLoader loader = new GroovyClassLoader(parent);
		try {
			Class groovyClass = loader.parseClass(new File(groovyFile));
			GroovyObject groovyObject = (GroovyObject) groovyClass.newInstance(); 
			groovyObject.invokeMethod("main", null);
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}
}
