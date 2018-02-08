package com.taobao.zeus.jobs.sub.tool.hs2;

import static com.taobao.zeus.util.RunningJobKeys.RUN_CLASSPATH;
import static com.taobao.zeus.util.RunningJobKeys.RUN_INITIAL_MEMORY_SIZE;
import static com.taobao.zeus.util.RunningJobKeys.RUN_JAVA_MAIN_ARGS;
import static com.taobao.zeus.util.RunningJobKeys.RUN_JAVA_MAIN_CLASS;
import static com.taobao.zeus.util.RunningJobKeys.RUN_JVM_PARAMS;
import static com.taobao.zeus.util.RunningJobKeys.RUN_MAX_MEMORY_SIZE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.taobao.zeus.model.FileDescriptor;
import com.taobao.zeus.jobs.AbstractJob;
import com.taobao.zeus.jobs.Job;
import com.taobao.zeus.jobs.JobContext;
import com.taobao.zeus.jobs.ProcessJob;
import com.taobao.zeus.jobs.sub.HiveJob;
import com.taobao.zeus.jobs.sub.JavaJob;
import com.taobao.zeus.jobs.sub.main.MapReduceMain;
import com.taobao.zeus.store.FileManager;
import com.taobao.zeus.util.PropertyKeys;
import com.taobao.zeus.util.RunningJobKeys;

/**
 * @author mark 
 * HiveServiceJob 是一个JavaJob. 通过java进程 运行. Hive语句向HS2 发送 hql语句,并且把计算的结果返回插入到mysql中.
 *
 */
public class HiveServiceJob extends JavaJob {
	public static final String UDF_HQL_NAME = "zeus_udf.hql";
	public static String JAVA_COMMAND = "java";

	private FileManager fileManager;
	private ApplicationContext applicationContext;

	@SuppressWarnings("unused")
	public HiveServiceJob(JobContext jobContext,
			ApplicationContext applicationContext) {
		super(jobContext);
		// TODO Auto-generated constructor stub
		this.applicationContext = applicationContext;
		fileManager = (FileManager) this.applicationContext.getBean("fileManager");
		
		String main=getJavaClass();
		String args=getMainArguments();
		String classpath=getClassPaths();
		
		StringBuilder libClassPath = new StringBuilder();
		libClassPath.append(classpath).append(getMRClassPath()).append(getHiveClassPath());
		classpath=libClassPath.toString();
		
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_CLASS, "com.taobao.zeus.jobs.sub.tool.hs2.Hs2ExecutorBackend");
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_CLASSPATH, classpath + File.pathSeparator+getSourcePathFromClass(Hs2ExecutorBackend.class));
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_ARGS, main+" "+args);
		jobContext.getProperties().setProperty(RunningJobKeys.JOB_RUN_TYPE,	"HiveServiceJob");
		
	}
	
	//hadoop2依赖的JAR包，Apache需要的jar在${HADOOP_HOME}/libs/目录下，其他版本可能在${HADOOP_HOME}/lib
	public String getMRClassPath(){
		StringBuilder sb=new StringBuilder();
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
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/common"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/hdfs"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/mapreduce"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/tools"));
			sb.append(getMRJarClassPath(hadoophome,"/share/hadoop/yarn"));
		}
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
	public String getHiveClassPath(){
		StringBuilder sb=new StringBuilder();
		String hivehome=System.getenv("HIVE_HOME");
		if(hivehome!=null && !"".equals(hivehome)){
			File f2=new File(hivehome+"/lib");
			if(f2.exists()){
				sb.append(File.pathSeparator);
				sb.append(hivehome);
				sb.append("/lib/*");	
			}
		}
		return sb.toString();
	}
	

	
	/**
	 * @param jobScript PropertyKeys.JOB_SQL_SRCIPT or PropertyKeys.JOB_SRCIPT
	 * @param scriptType .hs2 or .persistence.sql
	 * @return 把要执行持久化SQL or HQL语句 写到磁盘文件以作保存
	 * @throws Exception
	 */
	private void writerCommand(String jobScript, String scriptType, String runPath) throws Exception {
		String script = getProperties().getLocalProperty(jobScript);
		File f = new File(jobContext.getWorkDir() + File.separator + (new Date().getTime()) + scriptType);
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
	}
	
	/**
	 * @param jobScript PropertyKeys.JOB_SQL_SRCIPT or PropertyKeys.JOB_SRCIPT
	 * @param scriptType .persistence.sql or .hs2
	 * @param runPath PropertyKeys.RUN_HS2_PATH
	 * @return 把要执行持久化SQL or HQL语句 从磁盘文件读取并返回
	 * @throws Exception
	 */
	private String readerCommand(String jobScript, String scriptType, String runPath) throws Exception {
		StringBuilder builder = new StringBuilder();
		
		String commandFilePath = getProperty(runPath, "");
		File commandFile = new File(commandFilePath);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(commandFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				builder.append(line);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log("获取同步表脚本失败");
			log(e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return builder.toString();
	}

	
	private void writerUdfSql() {
		// TODO 请在此处填写udf文件对应的文档id
		String fileID = PropertyKeys.JOB_UDF_FILE_ID;
		try {
			FileDescriptor file = fileManager.getFile(fileID);
			File f = new File(jobContext.getWorkDir() + File.separator + UDF_HQL_NAME);
			if (f.exists()) {
				f.delete();
			}
			String udf = file.getContent();
			OutputStreamWriter writer = null;
			try {
				writer = new OutputStreamWriter(new FileOutputStream(f),
						Charset.forName(jobContext.getProperties().getProperty("zeus.fs.encode", "utf-8")));
				writer.write(udf.replaceAll("^--.*", ""));
				
			} catch (Exception e) {
				jobContext.getJobHistory().getLog().appendZeusException(e);
			} finally {
				IOUtils.closeQuietly(writer);
			}
		} catch (Exception e) {
			log("写入同步表脚本失败");
			log(e);
		}
	}
	
	private String readerUdfSql() {
		BufferedReader reader = null;
		StringBuilder builder = new StringBuilder();
		String udfFile = jobContext.getWorkDir() + File.separator + UDF_HQL_NAME;
		try {
			reader = new BufferedReader(new FileReader(udfFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				builder.append(line);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log("获取同步表脚本失败");
			log(e);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return builder.toString();
	}

	private String getHQLList() throws Exception {
		String udf = readerUdfSql();
		String hql = readerCommand(PropertyKeys.JOB_SCRIPT, ".hs2" , PropertyKeys.RUN_HS2_PATH);	
		return  new StringBuilder(udf).append(hql).toString();
	}
		
	
	public void setMainArgs() {
		StringBuilder mainArgs = new StringBuilder();
		try {
			
			String hqls = getHQLList();
			String sql =  readerCommand(PropertyKeys.JOB_SQL_SRCIPT, ".sql" , PropertyKeys.RUN_SQL_PATH);
			
			StringBuilder commandParam = new StringBuilder();
			String mUrl = jobContext.getProperties().getProperty("sql.url");
			String mDriver = jobContext.getProperties().getProperty("sql.driver");
			String mUser = jobContext.getProperties().getProperty("sql.user");
			String mPassword = jobContext.getProperties().getProperty("sql.password");
			
			commandParam.append("sql.url=" + mUrl)
					.append(",sql.driver=" + mDriver)
					.append(",sql.user=" + mUser + ",sql.password=" + mPassword).append(",");

			
			String hs2Url = jobContext.getProperties().getProperty("hs2.url");
			String hs2iver = jobContext.getProperties().getProperty("hs2.driver");
			String hs2User = jobContext.getProperties().getProperty("hs2.user");
			String hs2Password = jobContext.getProperties().getProperty("hs2.password");
			commandParam.append("hs2.url=" + hs2Url)
					.append(",hs2.driver=" + hs2iver)
					.append(",hs2.user=" + hs2User + ",hs2.password=" + hs2Password);
			
			
			mainArgs.append("\"").append(commandParam.toString()).append("\" \"").append(hqls).append("\" \"").append(sql).append("\"");
			
			jobContext.getProperties().setProperty(RUN_JAVA_MAIN_ARGS, mainArgs.toString());
			
		} catch (Exception ex) {
			log("生成执行命令失败");
			log(ex);
		}
	}
	
	private void runInner() throws Exception {
		writerUdfSql();
		writerCommand(PropertyKeys.JOB_SCRIPT,".hs2",PropertyKeys.RUN_HS2_PATH);
		writerCommand(PropertyKeys.JOB_SQL_SRCIPT,".sql",PropertyKeys.RUN_SQL_PATH);
		
		setMainArgs();
	}

	@Override
	public Integer run() throws Exception {
		// TODO Auto-generated method stub
		runInner();
		return super.run();
	}
		
}
