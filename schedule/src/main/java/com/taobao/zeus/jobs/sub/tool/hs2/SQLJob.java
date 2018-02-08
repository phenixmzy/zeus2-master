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

import com.taobao.zeus.jobs.JobContext;
import com.taobao.zeus.jobs.sub.JavaJob;
import com.taobao.zeus.util.PropertyKeys;
import com.taobao.zeus.util.RunningJobKeys;

public class SQLJob extends JavaJob {
	public static String JAVA_COMMAND = "java";
	
	public SQLJob(JobContext jobContext) {
		super(jobContext);
		// TODO Auto-generated constructor stub
		String main=getJavaClass();
		String args=getMainArguments();
		String classpath=getClassPaths();
		
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_CLASS, "com.taobao.zeus.jobs.sub.tool.hs2.SQLExecutorBackend");
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_CLASSPATH, classpath + File.pathSeparator+getSourcePathFromClass(SQLExecutorBackend.class));
		jobContext.getProperties().setProperty(RunningJobKeys.RUN_JAVA_MAIN_ARGS, main+" "+args);
		jobContext.getProperties().setProperty(RunningJobKeys.JOB_RUN_TYPE,	"SQLJob");
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
	
	public void setMainArgs() {
		StringBuilder mainArgs = new StringBuilder();
		try {
			String sql =  readerCommand(PropertyKeys.JOB_SQL_SRCIPT, ".sql" , PropertyKeys.RUN_SQL_PATH);
			
			StringBuilder commandParam = new StringBuilder();
			String mUrl = jobContext.getProperties().getProperty("sql.url");
			String mDriver = jobContext.getProperties().getProperty("sql.driver");
			String mUser = jobContext.getProperties().getProperty("sql.user");
			String mPassword = jobContext.getProperties().getProperty("sql.password");
			
			commandParam.append("sql.url=" + mUrl)
					.append(",sql.driver=" + mDriver)
					.append(",sql.user=" + mUser + ",sql.password=" + mPassword);

			mainArgs.append("\"").append(commandParam.toString()).append("\" \"").append(sql).append("\"");
			
			jobContext.getProperties().setProperty(RUN_JAVA_MAIN_ARGS, mainArgs.toString());
		} catch (Exception ex) {
			log("生成执行命令失败");
			log(ex);
		}
	}
	
	private void runInner() throws Exception {
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
