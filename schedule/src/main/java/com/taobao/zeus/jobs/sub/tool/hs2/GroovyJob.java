package com.taobao.zeus.jobs.sub.tool.hs2;

import static com.taobao.zeus.util.RunningJobKeys.RUN_JAVA_MAIN_ARGS;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.springframework.context.ApplicationContext;

import com.taobao.zeus.jobs.JobContext;
import com.taobao.zeus.jobs.sub.JavaJob;
import com.taobao.zeus.util.PropertyKeys;
import com.taobao.zeus.util.RunningJobKeys;


public class GroovyJob extends JavaJob {

	public static String JAVA_COMMAND = "java";
	private static final String GROOVY_FILE_KEY = "Groovy_FILE_KEY";
	private ApplicationContext applicationContext;
	
	@SuppressWarnings("unused")
	public GroovyJob(JobContext jobContext, ApplicationContext applicationContext) {
		super(jobContext);
		// TODO Auto-generated constructor stub
		this.applicationContext = applicationContext;
		
		
		String main=getJavaClass();
		String args=getMainArguments();
		String classpath=getClassPaths();
		
		StringBuilder libClassPath = new StringBuilder();
		libClassPath.append(getMRClassPath(classpath));
		libClassPath.append(getGroovyClassPath());
		classpath=libClassPath.toString();
		
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_CLASS, "com.taobao.zeus.jobs.sub.tool.hs2.GroovyExecutorBackend");
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_CLASSPATH, classpath + File.pathSeparator+getSourcePathFromClass(GroovyExecutorBackend.class));
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_ARGS, main+" "+args);
		jobContext.getProperties().setProperty(RunningJobKeys.JOB_RUN_TYPE,	"GroovyJob");
	}
	
	public String getGroovyClassPath() {
		String groovyPath = getSourcePathFromClass(groovy.lang.GroovyClassLoader.class);
		return File.pathSeparator+ groovyPath;
	}
	
	//hadoop2依赖的JAR包，Apache需要的jar在${HADOOP_HOME}/libs/目录下，其他版本可能在${HADOOP_HOME}/lib
		public String getMRClassPath(String classpath){
			StringBuilder sb=new StringBuilder(classpath);
			String hadoophome=System.getenv("HADOOP_HOME");
			if(hadoophome!=null && !"".equals(hadoophome)){
				File f1=new File(hadoophome+"/libs");
				if(f1.exists()){
					sb.append(File.pathSeparator);
					sb.append(hadoophome);
					sb.append("/libs/*");	
				}
				File f2=new File(hadoophome+"/lib");
				if(f2.exists()){
					sb.append(File.pathSeparator);
					sb.append(hadoophome);
					sb.append("/lib/*");	
				}
			}
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/common"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/hdfs"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/mapreduce"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/tools"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/yarn"));
			return sb.toString();
		}
		
		private String getMRJarClassPath(String hadoopHome, String jarPath) {
			StringBuilder jarsPath=new StringBuilder();
			String hadoophome=System.getenv("HADOOP_HOME");
			if(hadoophome!=null && !"".equals(hadoophome)){
				File f1=new File(hadoophome + jarPath);
				if(f1.exists()){
					jarsPath.append(File.pathSeparator);
					jarsPath.append(hadoophome);
					jarsPath.append(jarPath);
					jarsPath.append("/*");	
				}
				File f2=new File(hadoophome + jarPath +"/lib");
				if(f2.exists()){
					jarsPath.append(File.pathSeparator);
					jarsPath.append(hadoophome);
					jarsPath.append(jarPath);
					jarsPath.append("/lib/*");	
				}
			}
			return jarsPath.toString();
		}
		
		/**
		 * @param jobScript PropertyKeys.JOB_SQL_SRCIPT or PropertyKeys.JOB_SRCIPT
		 * @param scriptType .hs2 or .persistence.sql
		 * @return 把要执行持久化SQL or HQL语句 写到磁盘文件以作保存
		 * @throws Exception
		 */
		private void writerCommand(String jobScript, String scriptType, String runPath) throws Exception {
			String script = getProperties().getLocalProperty(jobScript);
			String groovyFile = jobContext.getWorkDir() + File.separator + (new Date().getTime()) + scriptType;
			File f = new File(groovyFile);
			if (!f.exists()) {
				f.createNewFile();
			}
			OutputStreamWriter writer = null;
			try {
				writer = new OutputStreamWriter(new FileOutputStream(f),
						Charset.forName(jobContext.getProperties().getProperty("zeus.fs.encode", "utf-8")));
				writer.write(script.replaceAll("^--.*", ""));
				
			} catch (Exception e) {
				jobContext.getJobHistory().getLog().appendZeusException(e);
			} finally {
				IOUtils.closeQuietly(writer);
			}
			getProperties().setProperty(runPath, f.getAbsolutePath());
			getProperties().setProperty(GROOVY_FILE_KEY, groovyFile);
			
		}
		
	
		
		public void setMainArgs() {
			StringBuilder mainArgs = new StringBuilder();
			try {
				mainArgs.append("\"").append(getProperty(GROOVY_FILE_KEY, "")).append("\"");
				jobContext.getProperties().setProperty(RUN_JAVA_MAIN_ARGS, mainArgs.toString());
				
			} catch (Exception ex) {
				log("生成执行命令失败");
				log(ex);
			}
		}
		
		private void runInner() throws Exception {
			writerCommand(PropertyKeys.JOB_SCRIPT,".groovy",PropertyKeys.RUN_GROOVY_PATH);
		}

		@Override
		public Integer run() throws Exception {
			// TODO Auto-generated method stub
			runInner();
			setMainArgs();
			return super.run();
		}
}
