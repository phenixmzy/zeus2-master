package com.taobao.zeus.broadcast.alarm;

import java.io.IOException;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PostMethod;

import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.store.mysql.persistence.ZeusUser;

public class SMSAlarm extends AbstractZeusAlarm{
	private String postUrl;
	private String loginName;
	private String enterpriseID;
	private String password;
	
	@Override
	public void alarm(List<String> uids, String title, String content)
			throws Exception {
		//TODO to be implements
		String smsContent = content.substring(0, 60);
		List<ZeusUser> zeusUsers = userManager.findListByUid(uids);
		for (ZeusUser user : zeusUsers) {
			ScheduleInfoLog.info("Send SMS to " + user.getName() + " phone[" + user.getPhone() +  "]");
			sendMessage(user.getPhone(), smsContent);
		}
	}
	
	private void sendMessage(String phoneNum, String smsContent) {
		HttpClient client = new HttpClient();
		PostMethod method = new PostMethod(postUrl);
		client.getParams().setContentCharset("UTF-8");
		method.setRequestHeader("ContentType","application/x-www-form-urlencoded;charset=UTF-8");
		NameValuePair[] data = {// 提交短信
				new NameValuePair("loginName", loginName),
				new NameValuePair("enterpriseID",enterpriseID),
						new NameValuePair("password", password),
						new NameValuePair("mobiles", phoneNum),
						new NameValuePair("content", smsContent), };
		method.setRequestBody(data);
		try {
			client.executeMethod(method);
			ScheduleInfoLog.info(method.getResponseBodyAsString());
		} catch (HttpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void setPostUrl(String postUrl) {
		this.postUrl = postUrl;
	}

	public void setLoginName(String loginName) {
		this.loginName = loginName;
	}

	public void setEnterpriseID(String enterpriseID) {
		this.enterpriseID = enterpriseID;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPostUrl() {
		return postUrl;
	}

	public String getLoginName() {
		return loginName;
	}

	public String getEnterpriseID() {
		return enterpriseID;
	}

	public String getPassword() {
		return password;
	}
}
